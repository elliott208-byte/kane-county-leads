#!/usr/bin/env python3
"""
Kane County, Illinois — Motivated Seller Lead Scraper
======================================================
Recorder Portal : https://lrs.kanecountyrecorder.net/
Assessor        : https://assessments.kanecountyil.gov/
GIS / Parcel    : https://www.kanecountyil.gov/pages/gis.aspx

Outputs:
  dashboard/records.json
  data/records.json
  data/ghl_export.csv
"""

import asyncio
import csv
import io
import json
import logging
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("kane_scraper")

# ── Constants ─────────────────────────────────────────────────────────────────
# Kane County Recorder Land Records Search
RECORDER_BASE    = "https://lrs.kanecountyrecorder.net"
RECORDER_SEARCH  = "https://lrs.kanecountyrecorder.net/search"
RECORDER_ADV     = "https://lrs.kanecountyrecorder.net/search/advanced"

# Kane County Assessor / GIS parcel data candidates
PARCEL_CANDIDATES = [
    # Illinois state open data — Kane County parcels
    "https://datacatalog.cookcountyil.gov/api/views/c49d-89sn/rows.csv?accessType=DOWNLOAD",
    # Kane County GIS open data portal
    "https://opendata.kanecountyil.gov/datasets/kane-county-parcels.csv",
    "https://www.kanecountyil.gov/GIS/Downloads/Parcels.zip",
    "https://gis.kanecountyil.gov/arcgis/rest/services/Parcels/MapServer/0/query"
    "?where=1%3D1&outFields=*&f=json&resultRecordCount=1000",
    # Illinois state GIS
    "https://data.illinois.gov/api/views/ixkm-943y/rows.csv?accessType=DOWNLOAD",
]

# Kane County Assessor property search (for individual lookups)
ASSESSOR_SEARCH  = "https://assessments.kanecountyil.gov/Search"
ASSESSOR_BASE    = "https://assessments.kanecountyil.gov"

LOOKBACK_DAYS  = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 4

# ── Document type catalogue ───────────────────────────────────────────────────
# Kane County Recorder uses Illinois standard document type codes
DOC_TYPES = {
    # Lis Pendens
    "LP":        ("Lis Pendens",              "lis_pendens"),
    "RELLP":     ("Release Lis Pendens",      "release"),
    # Foreclosure
    "NOFC":      ("Notice of Foreclosure",    "foreclosure"),
    "FORE":      ("Foreclosure",              "foreclosure"),
    "TRUSTD":    ("Trustee Deed",             "foreclosure"),
    "DIL":       ("Deed in Lieu",             "foreclosure"),
    "SHFD":      ("Sheriff Deed",             "foreclosure"),
    # Tax
    "TAXDEED":   ("Tax Deed",                 "tax_deed"),
    "TAXLN":     ("Tax Lien",                 "tax_lien"),
    "CERT":      ("Certificate of Purchase",  "tax_deed"),
    # Judgments
    "JUD":       ("Judgment",                 "judgment"),
    "CCJ":       ("Certified Judgment",       "judgment"),
    "DRJUD":     ("Domestic Judgment",        "judgment"),
    "AJ":        ("Abstract of Judgment",     "judgment"),
    "CJUD":      ("Confession of Judgment",   "judgment"),
    # Liens
    "LN":        ("Lien",                     "lien"),
    "LNMECH":    ("Mechanic Lien",            "lien"),
    "LNHOA":     ("HOA Lien",                 "lien"),
    "LNCORPTX":  ("Corp Tax Lien",            "tax_lien"),
    "LNIRS":     ("IRS Lien",                 "tax_lien"),
    "LNFED":     ("Federal Lien",             "tax_lien"),
    "MEDLN":     ("Medicaid Lien",            "lien"),
    "MECLIEN":   ("Mechanic Lien",            "lien"),
    "STLN":      ("State Tax Lien",           "tax_lien"),
    # Probate
    "PRO":       ("Probate Document",         "probate"),
    "WILL":      ("Will",                     "probate"),
    "LTADM":     ("Letters of Administration","probate"),
    # Notice
    "NOC":       ("Notice of Commencement",   "noc"),
    # Bankruptcy
    "BNKRCY":    ("Bankruptcy",               "bankruptcy"),
}

CAT_LABELS = {
    "lis_pendens": "Lis Pendens",
    "foreclosure": "Pre-Foreclosure",
    "tax_deed":    "Tax Deed",
    "judgment":    "Judgment / Lien",
    "tax_lien":    "Tax Lien",
    "lien":        "Lien",
    "probate":     "Probate / Estate",
    "noc":         "Notice of Commencement",
    "bankruptcy":  "Bankruptcy",
    "release":     "Release",
}

ROOT           = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = ROOT / "dashboard" / "records.json"
DATA_JSON      = ROOT / "data"      / "records.json"
GHL_CSV        = ROOT / "data"      / "ghl_export.csv"

BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
SESSION_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# =============================================================================
# SECTION 1 – Kane County Parcel Lookup
# =============================================================================

def _retry_get(url: str, stream: bool = False, **kwargs) -> Optional[requests.Response]:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.get(
                url, stream=stream, timeout=90,
                headers=SESSION_HEADERS, **kwargs
            )
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning(f"GET attempt {attempt}/{RETRY_ATTEMPTS} [{url}]: {exc}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    return None


def _retry_post(url: str, data: dict, **kwargs) -> Optional[requests.Response]:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = requests.post(
                url, data=data, timeout=60,
                headers=SESSION_HEADERS, **kwargs
            )
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning(f"POST attempt {attempt}/{RETRY_ATTEMPTS} [{url}]: {exc}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    return None


def _parse_amount(text: str) -> float:
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").upper().strip())


def _name_variants(name: str) -> list:
    name = _normalize_name(name)
    variants = {name}
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            variants.add(f"{parts[1]} {parts[0]}")
            variants.add(f"{parts[0]} {parts[1]}")
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            variants.add(f"{tokens[-1]}, {' '.join(tokens[:-1])}")
            variants.add(f"{tokens[-1]} {' '.join(tokens[:-1])}")
    return [v for v in variants if v]


class KaneParcelLookup:
    """
    Builds an owner-name → address index from Kane County parcel data.
    Tries multiple data sources in order:
      1. Kane County GIS open data (CSV/ZIP)
      2. Illinois state open data
      3. Kane County Assessor individual lookups (fallback)
    """

    def __init__(self):
        self._index: dict = {}
        self._loaded = False

    def load(self):
        if self._loaded:
            return
        log.info("Loading Kane County parcel data ...")

        # Try ZIP sources first
        zip_sources = [
            "https://www.kanecountyil.gov/GIS/Downloads/Parcels.zip",
            "https://opendata.kanecountyil.gov/datasets/kane-county-parcels.zip",
        ]
        for url in zip_sources:
            if self._try_zip(url):
                self._loaded = True
                return

        # Try CSV sources
        csv_sources = [
            "https://opendata.kanecountyil.gov/datasets/kane-county-parcels.csv",
            "https://data.illinois.gov/api/views/ixkm-943y/rows.csv?accessType=DOWNLOAD",
            # ArcGIS REST API — paginated JSON
        ]
        for url in csv_sources:
            if self._try_csv(url):
                self._loaded = True
                return

        # Try ArcGIS REST API (Kane County GIS)
        if self._try_arcgis():
            self._loaded = True
            return

        log.warning("Kane County parcel data unavailable — address enrichment disabled.")
        self._loaded = True

    def _try_zip(self, url: str) -> bool:
        log.info(f"  Trying ZIP: {url}")
        r = _retry_get(url, stream=True)
        if r is None:
            return False
        try:
            raw = b""
            for chunk in r.iter_content(chunk_size=1 << 20):
                raw += chunk
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                names = zf.namelist()
                log.info(f"  ZIP contents: {names}")
                target = next(
                    (n for n in names if n.lower().endswith((".csv", ".txt", ".dbf"))),
                    None,
                )
                if target is None:
                    return False
                data = zf.read(target)
                if target.lower().endswith(".dbf"):
                    return self._load_dbf_bytes(data)
                else:
                    return self._load_csv_bytes(data)
        except Exception as exc:
            log.warning(f"  ZIP parse failed: {exc}")
            return False

    def _try_csv(self, url: str) -> bool:
        log.info(f"  Trying CSV: {url}")
        r = _retry_get(url)
        if r is None:
            return False
        try:
            return self._load_csv_bytes(r.content)
        except Exception as exc:
            log.warning(f"  CSV parse failed: {exc}")
            return False

    def _try_arcgis(self) -> bool:
        """Query Kane County ArcGIS REST API for parcel data."""
        base = (
            "https://gis.kanecountyil.gov/arcgis/rest/services"
            "/Parcels/MapServer/0/query"
        )
        log.info(f"  Trying ArcGIS REST: {base}")
        offset = 0
        batch  = 1000
        loaded = 0
        while True:
            params = {
                "where":           "1=1",
                "outFields":       "OWNER,SITEADDR,SITECITY,SITEZIP,MAILADR1,MAILCITY,MAILSTATE,MAILZIP",
                "f":               "json",
                "resultOffset":    offset,
                "resultRecordCount": batch,
            }
            try:
                r = requests.get(base, params=params, timeout=30,
                                 headers=SESSION_HEADERS)
                r.raise_for_status()
                data = r.json()
                features = data.get("features", [])
                if not features:
                    break
                for feat in features:
                    attrs = feat.get("attributes", {})
                    self._index_arcgis_row(attrs)
                    loaded += 1
                offset += batch
                if len(features) < batch:
                    break
            except Exception as exc:
                log.warning(f"  ArcGIS query failed at offset {offset}: {exc}")
                break

        if loaded:
            log.info(f"  ArcGIS: loaded {loaded:,} parcel records.")
            return True
        return False

    def _index_arcgis_row(self, attrs: dict):
        def g(*keys):
            for k in keys:
                v = attrs.get(k) or attrs.get(k.upper()) or attrs.get(k.lower())
                if v:
                    return str(v).strip()
            return ""

        owner = g("OWNER", "OWN1", "OWNERNAME")
        if not owner:
            return
        parcel = {
            "prop_address": g("SITEADDR", "SITE_ADDR", "SITEADDRESS"),
            "prop_city":    g("SITECITY", "SITE_CITY") or "Kane County",
            "prop_state":   "IL",
            "prop_zip":     g("SITEZIP", "SITE_ZIP"),
            "mail_address": g("MAILADR1", "MAILADDR", "MAIL_ADDR"),
            "mail_city":    g("MAILCITY", "MAIL_CITY"),
            "mail_state":   g("MAILSTATE", "MAIL_STATE") or "IL",
            "mail_zip":     g("MAILZIP", "MAIL_ZIP"),
        }
        for variant in _name_variants(owner):
            if variant and variant not in self._index:
                self._index[variant] = parcel

    def _load_csv_bytes(self, data: bytes) -> bool:
        text = data.decode("latin-1", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        col_map = self._detect_columns(reader.fieldnames or [])
        count = 0
        for row in reader:
            try:
                self._index_row(row, col_map)
                count += 1
            except Exception:
                pass
        if count:
            log.info(f"  CSV: loaded {count:,} parcel records.")
            return True
        return False

    def _load_dbf_bytes(self, data: bytes) -> bool:
        if not HAS_DBF:
            log.warning("dbfread not installed; skipping DBF.")
            return False
        tmp = Path("/tmp/kane_parcel.dbf")
        tmp.write_bytes(data)
        count = 0
        try:
            table = DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True)
            col_map = self._detect_columns(table.field_names)
            for row in table:
                try:
                    self._index_row(dict(row), col_map)
                    count += 1
                except Exception:
                    pass
        finally:
            tmp.unlink(missing_ok=True)
        if count:
            log.info(f"  DBF: loaded {count:,} parcel records.")
            return True
        return False

    @staticmethod
    def _detect_columns(fields: list) -> dict:
        upper = {f.upper(): f for f in fields}
        def pick(*candidates):
            for c in candidates:
                if c.upper() in upper:
                    return upper[c.upper()]
            return None
        return {
            "owner":      pick("OWN1","OWNER","OWNER_NAME","OWNERNAME","NAME"),
            "site_addr":  pick("SITEADDR","SITE_ADDR","SITE_ADDRESS","PROP_ADDR","ADDRESS"),
            "site_city":  pick("SITE_CITY","SITECITY","PROP_CITY","CITY"),
            "site_zip":   pick("SITE_ZIP","SITEZIP","PROP_ZIP","ZIP"),
            "mail_addr":  pick("MAILADR1","ADDR_1","MAIL_ADDR","MAIL_ADDRESS","MAILING_ADDR"),
            "mail_city":  pick("MAILCITY","MAIL_CITY","MAILING_CITY"),
            "mail_state": pick("MAILSTATE","MAIL_STATE","MAILING_STATE","STATE"),
            "mail_zip":   pick("MAILZIP","MAIL_ZIP","MAILING_ZIP"),
        }

    def _index_row(self, row: dict, col_map: dict):
        def g(key):
            col = col_map.get(key)
            return str(row.get(col, "") or "").strip() if col else ""
        owner = g("owner")
        if not owner:
            return
        parcel = {
            "prop_address": g("site_addr"),
            "prop_city":    g("site_city") or "Kane County",
            "prop_state":   "IL",
            "prop_zip":     g("site_zip"),
            "mail_address": g("mail_addr"),
            "mail_city":    g("mail_city"),
            "mail_state":   g("mail_state") or "IL",
            "mail_zip":     g("mail_zip"),
        }
        for variant in _name_variants(owner):
            if variant and variant not in self._index:
                self._index[variant] = parcel

    def lookup(self, name: str) -> Optional[dict]:
        for variant in _name_variants(name):
            result = self._index.get(variant)
            if result:
                return result
        return None

    def lookup_from_assessor(self, name: str) -> Optional[dict]:
        """
        Fallback: query Kane County Assessor website for a single owner name.
        Used when bulk data is unavailable.
        """
        try:
            params = {"SearchInput": name, "SearchType": "owner"}
            r = _retry_get(ASSESSOR_SEARCH, params=params)
            if r is None:
                return None
            soup = BeautifulSoup(r.text, "lxml")
            # Find first result row
            row = soup.find("tr", class_=re.compile(r"result|data", re.I))
            if not row:
                return None
            cells = row.find_all("td")
            if len(cells) < 3:
                return None
            return {
                "prop_address": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                "prop_city":    cells[2].get_text(strip=True) if len(cells) > 2 else "",
                "prop_state":   "IL",
                "prop_zip":     cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "mail_address": "",
                "mail_city":    "",
                "mail_state":   "IL",
                "mail_zip":     "",
            }
        except Exception as exc:
            log.debug(f"Assessor lookup failed for '{name}': {exc}")
            return None


# =============================================================================
# SECTION 2 – Kane County Recorder Scraper (Playwright)
# Portal: https://lrs.kanecountyrecorder.net/
# =============================================================================

async def _search_recorder(page, doc_type_label: str,
                            date_from: str, date_to: str) -> list:
    """
    Search lrs.kanecountyrecorder.net for one document type.
    The portal supports:
      - Quick Name search
      - Advanced search with document type + date range
    """
    records = []
    try:
        # Navigate to advanced search
        await page.goto(RECORDER_ADV, wait_until="networkidle", timeout=45_000)

        # ── Document Type ─────────────────────────────────────────────────────
        # Try select dropdown first
        doc_filled = False
        for sel in [
            "select[name*='doctype']",
            "select[id*='doctype']",
            "select[name*='DocumentType']",
            "select[id*='DocumentType']",
            "select[name*='type']",
            "#docType",
            "#documentType",
        ]:
            try:
                # Try by label text
                await page.select_option(sel, label=doc_type_label, timeout=2_000)
                doc_filled = True
                break
            except Exception:
                try:
                    await page.select_option(sel, value=doc_type_label, timeout=2_000)
                    doc_filled = True
                    break
                except Exception:
                    pass

        if not doc_filled:
            # Try text input
            for sel in [
                "input[name*='doctype']",
                "input[id*='doctype']",
                "input[name*='DocumentType']",
                "input[placeholder*='document']",
                "input[placeholder*='type']",
            ]:
                try:
                    await page.fill(sel, doc_type_label, timeout=2_000)
                    doc_filled = True
                    break
                except Exception:
                    pass

        # ── Date From ─────────────────────────────────────────────────────────
        for sel in [
            "input[name*='startdate']",
            "input[name*='StartDate']",
            "input[name*='fromdate']",
            "input[name*='FromDate']",
            "input[name*='dateFrom']",
            "input[id*='startdate']",
            "input[id*='StartDate']",
            "input[id*='fromDate']",
            "input[placeholder*='start']",
            "input[placeholder*='from']",
            "input[type='date']:first-of-type",
        ]:
            try:
                await page.fill(sel, date_from, timeout=2_000)
                break
            except Exception:
                continue

        # ── Date To ───────────────────────────────────────────────────────────
        for sel in [
            "input[name*='enddate']",
            "input[name*='EndDate']",
            "input[name*='todate']",
            "input[name*='ToDate']",
            "input[name*='dateTo']",
            "input[id*='enddate']",
            "input[id*='EndDate']",
            "input[id*='toDate']",
            "input[placeholder*='end']",
            "input[placeholder*='to']",
        ]:
            try:
                await page.fill(sel, date_to, timeout=2_000)
                break
            except Exception:
                continue

        # ── Submit ────────────────────────────────────────────────────────────
        for sel in [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Search')",
            "a:has-text('Search')",
            "#searchBtn",
            "#btnSearch",
        ]:
            try:
                await page.click(sel, timeout=5_000)
                break
            except Exception:
                continue

        await page.wait_for_load_state("networkidle", timeout=45_000)

        # ── Paginate ──────────────────────────────────────────────────────────
        page_num = 0
        while True:
            page_num += 1
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            rows = _parse_recorder_results(soup, doc_type_label)
            records.extend(rows)
            log.info(f"    [{doc_type_label}] page {page_num}: {len(rows)} rows")

            # Next page
            next_btn = (
                soup.find("a", string=re.compile(r"^\s*Next\s*$", re.I)) or
                soup.find("a", string=re.compile(r"^\s*>\s*$", re.I)) or
                soup.find("li", class_=re.compile(r"next", re.I))
            )
            if not next_btn or page_num >= 50:
                break
            try:
                await page.click(
                    "a:text('Next'), a:text('>'), li.next > a",
                    timeout=8_000,
                )
                await page.wait_for_load_state("networkidle", timeout=30_000)
            except PWTimeout:
                break

    except Exception as exc:
        log.warning(f"  [{doc_type_label}] recorder search error: {exc}")

    return records


def _parse_recorder_results(soup: BeautifulSoup, doc_code: str) -> list:
    """
    Parse Kane County LRS search results.
    The portal renders results as a table or card list.
    """
    records = []

    # Try table first
    table = None
    for t in soup.find_all("table"):
        ths = t.find_all("th")
        tds = t.find_all("td")
        if len(ths) >= 3 or len(tds) >= 6:
            table = t
            break

    if table:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]

        def col(cells, *keywords):
            for kw in keywords:
                for i, h in enumerate(headers):
                    if kw in h and i < len(cells):
                        return cells[i].get_text(strip=True)
            return ""

        def col_pos(cells, idx):
            return cells[idx].get_text(strip=True) if idx < len(cells) else ""

        for tr in table.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            try:
                link_tag  = tr.find("a", href=True)
                clerk_url = ""
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = (
                        href if href.startswith("http")
                        else urljoin(RECORDER_BASE, href)
                    )

                doc_num = col(cells, "document", "doc", "number", "instrument")
                if not doc_num and link_tag:
                    doc_num = link_tag.get_text(strip=True)
                if not doc_num:
                    doc_num = col_pos(cells, 0)

                filed   = col(cells, "date", "filed", "recorded") or col_pos(cells, 1)
                grantor = col(cells, "grantor", "owner", "from", "seller")
                grantee = col(cells, "grantee", "to", "buyer", "lender")
                legal   = col(cells, "legal", "description", "subdivision", "property")
                amount  = col(cells, "amount", "consideration", "debt", "value")

                if not doc_num:
                    continue

                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  doc_code,
                    "filed":     filed,
                    "owner":     grantor,
                    "grantee":   grantee,
                    "legal":     legal,
                    "amount":    amount,
                    "clerk_url": clerk_url,
                })
            except Exception as exc:
                log.debug(f"Row parse error: {exc}")

    else:
        # Card/div layout fallback
        cards = soup.find_all(
            "div",
            class_=re.compile(r"result|record|document|card|item", re.I)
        )
        for card in cards:
            try:
                link_tag  = card.find("a", href=True)
                clerk_url = ""
                if link_tag:
                    href = link_tag["href"]
                    clerk_url = (
                        href if href.startswith("http")
                        else urljoin(RECORDER_BASE, href)
                    )

                text = card.get_text(" ", strip=True)

                doc_num = ""
                m = re.search(r"(?:Doc(?:ument)?|Instrument|#)\s*[:#]?\s*(\S+)", text, re.I)
                if m:
                    doc_num = m.group(1)

                filed = ""
                m = re.search(r"(?:Date|Filed|Recorded)[:\s]+(\d{1,2}/\d{1,2}/\d{4})", text, re.I)
                if m:
                    filed = m.group(1)

                grantor = ""
                m = re.search(r"Grantor[:\s]+([^\n|]+)", text, re.I)
                if m:
                    grantor = m.group(1).strip()

                grantee = ""
                m = re.search(r"Grantee[:\s]+([^\n|]+)", text, re.I)
                if m:
                    grantee = m.group(1).strip()

                amount = ""
                m = re.search(r"\$[\d,]+(?:\.\d{2})?", text)
                if m:
                    amount = m.group(0)

                if not doc_num:
                    continue

                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  doc_code,
                    "filed":     filed,
                    "owner":     grantor,
                    "grantee":   grantee,
                    "legal":     "",
                    "amount":    amount,
                    "clerk_url": clerk_url,
                })
            except Exception as exc:
                log.debug(f"Card parse error: {exc}")

    return records


async def _search_by_date_range(page, date_from: str, date_to: str) -> list:
    """
    Alternative: search ALL documents in date range, then filter by type.
    Used when document-type filtering is not available.
    """
    records = []
    try:
        await page.goto(RECORDER_ADV, wait_until="networkidle", timeout=45_000)

        # Fill date range only
        for sel in [
            "input[name*='startdate']", "input[name*='StartDate']",
            "input[name*='fromdate']",  "input[id*='startdate']",
            "input[placeholder*='start']",
        ]:
            try:
                await page.fill(sel, date_from, timeout=2_000)
                break
            except Exception:
                continue

        for sel in [
            "input[name*='enddate']", "input[name*='EndDate']",
            "input[name*='todate']",  "input[id*='enddate']",
            "input[placeholder*='end']",
        ]:
            try:
                await page.fill(sel, date_to, timeout=2_000)
                break
            except Exception:
                continue

        for sel in [
            "button[type='submit']", "input[type='submit']",
            "button:has-text('Search')", "#searchBtn",
        ]:
            try:
                await page.click(sel, timeout=5_000)
                break
            except Exception:
                continue

        await page.wait_for_load_state("networkidle", timeout=45_000)

        page_num = 0
        while True:
            page_num += 1
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Parse all results and filter by doc type
            all_rows = _parse_recorder_results(soup, "ALL")
            for row in all_rows:
                doc_type_text = (row.get("doc_type") or "").upper()
                for code, (label, cat) in DOC_TYPES.items():
                    if code in doc_type_text or label.upper() in doc_type_text:
                        row["doc_type"] = code
                        records.append(row)
                        break

            log.info(f"    [date-range] page {page_num}: {len(all_rows)} total rows")

            next_btn = soup.find("a", string=re.compile(r"^\s*(Next|>)\s*$", re.I))
            if not next_btn or page_num >= 100:
                break
            try:
                await page.click("a:text('Next'), a:text('>')", timeout=8_000)
                await page.wait_for_load_state("networkidle", timeout=30_000)
            except PWTimeout:
                break

    except Exception as exc:
        log.warning(f"  Date-range search error: {exc}")

    return records


async def scrape_recorder(date_from: str, date_to: str) -> list:
    """Launch Playwright and scrape Kane County Recorder portal."""
    all_records = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            user_agent=BROWSER_UA,
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        # Initial load — check portal structure
        try:
            await page.goto(RECORDER_BASE, wait_until="networkidle", timeout=30_000)
            # Dismiss any dialogs
            for btn_text in ["Accept", "OK", "Close", "Agree", "Continue"]:
                try:
                    await page.click(
                        f"button:has-text('{btn_text}')", timeout=2_000
                    )
                except Exception:
                    pass
        except Exception:
            pass

        # Check if advanced search supports doc type filtering
        has_doctype_filter = False
        try:
            await page.goto(RECORDER_ADV, wait_until="networkidle", timeout=20_000)
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            has_doctype_filter = bool(
                soup.find("select", attrs={"name": re.compile(r"doctype|DocumentType|type", re.I)}) or
                soup.find("input",  attrs={"name": re.compile(r"doctype|DocumentType", re.I)})
            )
            log.info(f"  Doc-type filter available: {has_doctype_filter}")
        except Exception:
            pass

        if has_doctype_filter:
            # Search per document type
            for doc_code, (doc_label, _) in DOC_TYPES.items():
                log.info(f"  Scraping: {doc_code} — {doc_label}")
                for attempt in range(1, RETRY_ATTEMPTS + 1):
                    try:
                        rows = await _search_recorder(
                            page, doc_code, date_from, date_to
                        )
                        all_records.extend(rows)
                        log.info(f"  [{doc_code}] total: {len(rows)} records")
                        break
                    except Exception as exc:
                        log.warning(f"  [{doc_code}] attempt {attempt} failed: {exc}")
                        if attempt < RETRY_ATTEMPTS:
                            await asyncio.sleep(RETRY_DELAY * attempt)
        else:
            # Fallback: search all docs in date range and filter
            log.info("  Using date-range search (no doc-type filter detected)")
            for attempt in range(1, RETRY_ATTEMPTS + 1):
                try:
                    rows = await _search_by_date_range(page, date_from, date_to)
                    all_records.extend(rows)
                    log.info(f"  Date-range total: {len(rows)} records")
                    break
                except Exception as exc:
                    log.warning(f"  Date-range attempt {attempt} failed: {exc}")
                    if attempt < RETRY_ATTEMPTS:
                        await asyncio.sleep(RETRY_DELAY * attempt)

        await browser.close()

    log.info(f"Recorder scrape complete: {len(all_records)} raw records")
    return all_records


# =============================================================================
# SECTION 3 – Scoring Engine
# =============================================================================

def compute_flags(record: dict, today: datetime) -> list:
    flags    = []
    cat      = record.get("cat", "")
    doc_type = record.get("doc_type", "")
    owner    = (record.get("owner") or "").upper()

    if cat == "lis_pendens":              flags.append("Lis pendens")
    if cat == "foreclosure":              flags.append("Pre-foreclosure")
    if cat == "judgment":                 flags.append("Judgment lien")
    if cat in ("tax_lien", "tax_deed"):   flags.append("Tax lien")
    if doc_type in ("LNMECH", "MECLIEN"): flags.append("Mechanic lien")
    if cat == "probate":                  flags.append("Probate / estate")
    if cat == "bankruptcy":               flags.append("Bankruptcy")
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bLTD\b|L\.L\.C", owner):
        flags.append("LLC / corp owner")

    try:
        # Try multiple date formats
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
            try:
                filed_dt = datetime.strptime(record.get("filed", ""), fmt)
                if (today - filed_dt).days <= 7:
                    flags.append("New this week")
                break
            except ValueError:
                continue
    except Exception:
        pass

    return flags


def compute_score(record: dict, flags: list) -> int:
    score = 30
    score += 10 * len(flags)

    owner_cats = record.get("_owner_cats", [])
    if "lis_pendens" in owner_cats and "foreclosure" in owner_cats:
        score += 20

    amount = record.get("_amount_num", 0.0)
    if amount > 100_000:   score += 15
    elif amount > 50_000:  score += 10

    if "New this week" in flags:   score += 5
    if record.get("prop_address"): score += 5

    return min(score, 100)


# =============================================================================
# SECTION 4 – Enrichment & Assembly
# =============================================================================

def enrich_records(raw: list, parcel: KaneParcelLookup, today: datetime) -> list:
    enriched = []
    for r in raw:
        try:
            doc_type  = r.get("doc_type", "")
            cat_info  = DOC_TYPES.get(doc_type, ("Unknown", "unknown"))
            cat_label, cat = cat_info[0], cat_info[1]
            amount_str = r.get("amount", "")
            amount_num = _parse_amount(amount_str)
            owner      = (r.get("owner") or "").strip()

            # Try bulk index first, then assessor fallback
            p = parcel.lookup(owner) if owner else None
            if p is None and owner:
                p = parcel.lookup_from_assessor(owner)

            rec = {
                "doc_num":      r.get("doc_num", ""),
                "doc_type":     doc_type,
                "filed":        r.get("filed", ""),
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        owner,
                "grantee":      (r.get("grantee") or "").strip(),
                "amount":       amount_str,
                "legal":        (r.get("legal") or "").strip(),
                "prop_address": p["prop_address"] if p else "",
                "prop_city":    p["prop_city"]    if p else "",
                "prop_state":   p["prop_state"]   if p else "IL",
                "prop_zip":     p["prop_zip"]     if p else "",
                "mail_address": p["mail_address"] if p else "",
                "mail_city":    p["mail_city"]    if p else "",
                "mail_state":   p["mail_state"]   if p else "IL",
                "mail_zip":     p["mail_zip"]     if p else "",
                "clerk_url":    r.get("clerk_url", ""),
                "_amount_num":  amount_num,
                "_owner_cats":  [],
            }
            enriched.append(rec)
        except Exception as exc:
            log.debug(f"Enrich error: {exc}")

    # Post-pass: collect all cats per owner for combo bonus
    owner_cats: dict = {}
    for rec in enriched:
        owner_cats.setdefault(rec["owner"], []).append(rec["cat"])
    for rec in enriched:
        rec["_owner_cats"] = owner_cats.get(rec["owner"], [])

    for rec in enriched:
        flags = compute_flags(rec, today)
        score = compute_score(rec, flags)
        rec["flags"] = flags
        rec["score"] = score
        rec.pop("_amount_num", None)
        rec.pop("_owner_cats", None)

    enriched.sort(key=lambda x: x["score"], reverse=True)
    return enriched


# =============================================================================
# SECTION 5 – GHL CSV Export
# =============================================================================

GHL_COLUMNS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def _split_name(full_name: str):
    name = full_name.strip()
    if "," in name:
        parts = [p.strip().title() for p in name.split(",", 1)]
        return parts[1], parts[0]
    tokens = name.split()
    if len(tokens) >= 2:
        return " ".join(tokens[:-1]).title(), tokens[-1].title()
    return name.title(), ""


def export_ghl_csv(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=GHL_COLUMNS)
        writer.writeheader()
        for rec in records:
            first, last = _split_name(rec.get("owner", ""))
            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        rec.get("mail_address", ""),
                "Mailing City":           rec.get("mail_city", ""),
                "Mailing State":          rec.get("mail_state", "IL"),
                "Mailing Zip":            rec.get("mail_zip", ""),
                "Property Address":       rec.get("prop_address", ""),
                "Property City":          rec.get("prop_city", ""),
                "Property State":         rec.get("prop_state", "IL"),
                "Property Zip":           rec.get("prop_zip", ""),
                "Lead Type":              rec.get("cat_label", ""),
                "Document Type":          rec.get("doc_type", ""),
                "Date Filed":             rec.get("filed", ""),
                "Document Number":        rec.get("doc_num", ""),
                "Amount/Debt Owed":       rec.get("amount", ""),
                "Seller Score":           rec.get("score", 0),
                "Motivated Seller Flags": " | ".join(rec.get("flags", [])),
                "Source":                 "Kane County Recorder — lrs.kanecountyrecorder.net",
                "Public Records URL":     rec.get("clerk_url", ""),
            })
    log.info(f"GHL CSV saved: {path} ({len(records)} rows)")


# =============================================================================
# SECTION 6 – JSON Output
# =============================================================================

def save_json(records: list, date_from: str, date_to: str):
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Kane County Recorder — lrs.kanecountyrecorder.net",
        "county":       "Kane County, Illinois",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    for path in (DASHBOARD_JSON, DATA_JSON):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        log.info(f"JSON saved: {path}")


# =============================================================================
# SECTION 7 – Main
# =============================================================================

async def main():
    today     = datetime.utcnow()
    date_to   = today.strftime("%m/%d/%Y")
    date_from = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")

    log.info("=" * 60)
    log.info(f"Kane County, IL Lead Scraper  |  {date_from} -> {date_to}")
    log.info(f"Recorder : {RECORDER_BASE}")
    log.info(f"Assessor : {ASSESSOR_BASE}")
    log.info("=" * 60)

    # 1. Load parcel data
    parcel = KaneParcelLookup()
    parcel.load()

    # 2. Scrape recorder portal
    log.info("Scraping Kane County Recorder portal ...")
    raw_records = await scrape_recorder(date_from, date_to)

    # 3. Enrich + score
    log.info("Enriching and scoring records ...")
    records = enrich_records(raw_records, parcel, today)

    # 4. Save outputs
    save_json(records, date_from, date_to)
    export_ghl_csv(records, GHL_CSV)

    log.info(
        f"Done. {len(records)} leads | "
        f"{sum(1 for r in records if r.get('prop_address'))} with address"
    )


if __name__ == "__main__":
    asyncio.run(main())

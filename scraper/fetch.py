#!/usr/bin/env python3
"""
Kane County, Illinois – Motivated Seller Lead Scraper
Targets: Recorder of Deeds document search (Clerk portal)
Parcel data: Kane County GIS / Township Assessors bulk DBF
Look-back: 7 days   |   Output: dashboard/records.json + data/records.json
"""

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
import traceback
import urllib.parse
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("kane_leads")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CLERK_BASE = "https://recorder.kanecountyil.gov"
CLERK_SEARCH = f"{CLERK_BASE}/PT_Web_Search/PT_Web_Search.aspx"
# Kane County GIS Open Data – parcel shapefile / DBF download
PARCEL_DBF_URL = (
    "https://opendata.arcgis.com/datasets/"
    "5d39b7b4e2224955913cce5a8d8fc59b_0.zip"
)
# Fallback / alternative sources
PARCEL_ALT_URLS = [
    "https://gis.kanecountyil.gov/arcgis/rest/services/Parcel/MapServer/0/query"
    "?where=1%3D1&outFields=PIN%2COWNER%2CSITEADDR%2CSITE_CITY%2CSITE_ZIP%2CMAILADR1%2CMAILCITY%2CSTATE%2CMAILZIP"
    "&f=json&resultRecordCount=100000",
]

LOOK_BACK_DAYS = 7
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # seconds

# Document type mappings
DOC_TYPE_MAP = {
    # code -> (category, label, base score flags)
    "LP":      ("foreclosure",  "Lis Pendens",               ["Lis pendens", "Pre-foreclosure"]),
    "NOFC":    ("foreclosure",  "Notice of Foreclosure",      ["Pre-foreclosure"]),
    "TAXDEED": ("tax",          "Tax Deed",                   ["Tax lien"]),
    "JUD":     ("judgment",     "Judgment",                   ["Judgment lien"]),
    "CCJ":     ("judgment",     "Certified Judgment",         ["Judgment lien"]),
    "DRJUD":   ("judgment",     "Domestic Judgment",          ["Judgment lien"]),
    "LNCORPTX":("lien",         "Corp Tax Lien",              ["Tax lien", "LLC / corp owner"]),
    "LNIRS":   ("lien",         "IRS Lien",                   ["Tax lien"]),
    "LNFED":   ("lien",         "Federal Lien",               ["Tax lien"]),
    "LN":      ("lien",         "Lien",                       ["Mechanic lien"]),
    "LNMECH":  ("lien",         "Mechanic Lien",              ["Mechanic lien"]),
    "LNHOA":   ("lien",         "HOA Lien",                   ["Mechanic lien"]),
    "MEDLN":   ("lien",         "Medicaid Lien",              []),
    "PRO":     ("probate",      "Probate Document",           ["Probate / estate"]),
    "NOC":     ("construction", "Notice of Commencement",     []),
    "RELLP":   ("release",      "Release Lis Pendens",        []),
}

ALL_DOC_CODES = list(DOC_TYPE_MAP.keys())

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def retry(fn, attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY):
    """Call fn(); on exception wait and retry up to `attempts` times."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay)
    raise RuntimeError(f"All {attempts} attempts failed")


def safe_text(el) -> str:
    """Return stripped text from a BS4 element, or ''."""
    return el.get_text(strip=True) if el else ""


def parse_amount(raw: str) -> float:
    """Convert '$1,234.56' or '1234.56' to float."""
    cleaned = re.sub(r"[^\d.]", "", raw or "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").upper().strip())


def make_name_variants(name: str) -> List[str]:
    """Return ['FIRST LAST', 'LAST FIRST', 'LAST, FIRST'] variants."""
    n = normalize_name(name)
    variants = {n}
    # if comma separated: 'SMITH, JOHN'
    if "," in n:
        parts = [p.strip() for p in n.split(",", 1)]
        if len(parts) == 2:
            last, first = parts
            variants.add(f"{first} {last}")
            variants.add(f"{last} {first}")
            variants.add(f"{last}, {first}")
    else:
        parts = n.split()
        if len(parts) >= 2:
            first, last = parts[0], parts[-1]
            variants.add(f"{last} {first}")
            variants.add(f"{last}, {first}")
    return [v for v in variants if v]


# ---------------------------------------------------------------------------
# Parcel / Owner Lookup
# ---------------------------------------------------------------------------

class ParcelLookup:
    """
    Loads parcel data from Kane County GIS and builds an owner-name index.
    Supports both DBF (zipped shapefile) and JSON (ArcGIS REST) sources.
    """

    def __init__(self):
        self._by_owner: Dict[str, List[Dict]] = {}
        self._loaded = False

    # ------------------------------------------------------------------
    def load(self):
        if self._loaded:
            return
        log.info("Loading parcel data …")
        loaded = False
        # Try DBF via zip download
        try:
            self._load_dbf()
            loaded = True
        except Exception as exc:
            log.warning("DBF load failed: %s", exc)
        # Fallback: ArcGIS REST JSON
        if not loaded:
            try:
                self._load_arcgis_json()
                loaded = True
            except Exception as exc:
                log.warning("ArcGIS JSON load failed: %s", exc)
        if loaded:
            log.info("Parcel index built: %d unique owner keys", len(self._by_owner))
        else:
            log.warning("No parcel data loaded – address enrichment disabled")
        self._loaded = True

    # ------------------------------------------------------------------
    def _load_dbf(self):
        """Download zipped shapefile, extract .dbf, parse with dbfread."""
        from dbfread import DBF

        def fetch_zip():
            r = requests.get(PARCEL_DBF_URL, timeout=60)
            r.raise_for_status()
            return r.content

        content = retry(fetch_zip)
        with ZipFile(BytesIO(content)) as zf:
            dbf_name = next(
                (n for n in zf.namelist() if n.lower().endswith(".dbf")), None
            )
            if not dbf_name:
                raise FileNotFoundError("No .dbf in zip")
            log.info("Parsing DBF: %s", dbf_name)
            with zf.open(dbf_name) as f:
                raw = f.read()
        # dbfread needs a file path or file-like – write to tmp
        tmp = Path("/tmp/_parcels.dbf")
        tmp.write_bytes(raw)
        for rec in DBF(str(tmp), lowernames=True, ignore_missing_memofile=True):
            self._ingest_record(rec)

    # ------------------------------------------------------------------
    def _load_arcgis_json(self):
        """Load parcel data from ArcGIS REST feature service."""
        for url in PARCEL_ALT_URLS:
            try:
                def fetch_json():
                    r = requests.get(url, timeout=60)
                    r.raise_for_status()
                    return r.json()

                data = retry(fetch_json)
                features = data.get("features", [])
                if not features:
                    continue
                for feat in features:
                    attrs = feat.get("attributes", {})
                    # normalise keys to lowercase
                    rec = {k.lower(): v for k, v in attrs.items()}
                    self._ingest_record(rec)
                log.info("Loaded %d parcels from ArcGIS REST", len(features))
                return
            except Exception as e:
                log.warning("ArcGIS URL failed: %s – %s", url, e)
        raise RuntimeError("All ArcGIS sources failed")

    # ------------------------------------------------------------------
    def _col(self, rec: dict, *candidates) -> str:
        for c in candidates:
            v = rec.get(c) or rec.get(c.upper()) or rec.get(c.lower())
            if v:
                return str(v).strip()
        return ""

    def _ingest_record(self, rec: dict):
        owner = self._col(rec, "owner", "own1", "ownername")
        if not owner:
            return
        entry = {
            "owner_raw": owner,
            "site_addr": self._col(rec, "site_addr", "siteaddr", "address"),
            "site_city": self._col(rec, "site_city", "city"),
            "site_zip":  self._col(rec, "site_zip",  "zip"),
            "mail_addr": self._col(rec, "addr_1", "mailadr1", "mail_addr"),
            "mail_city": self._col(rec, "city",   "mailcity"),
            "mail_state":self._col(rec, "state",  "mailstate"),
            "mail_zip":  self._col(rec, "zip",    "mailzip"),
        }
        for variant in make_name_variants(owner):
            self._by_owner.setdefault(variant, []).append(entry)

    # ------------------------------------------------------------------
    def lookup(self, owner_name: str) -> Optional[Dict]:
        """Return best-match parcel record for owner_name, or None."""
        if not owner_name:
            return None
        for variant in make_name_variants(owner_name):
            results = self._by_owner.get(variant)
            if results:
                return results[0]
        return None


# ---------------------------------------------------------------------------
# Clerk Portal Scraper (Playwright async)
# ---------------------------------------------------------------------------

async def scrape_clerk_playwright(
    doc_codes: List[str],
    start_date: datetime,
    end_date: datetime,
) -> List[Dict]:
    """
    Use Playwright to search Kane County Recorder search portal
    for each document type in the date range.
    Returns list of raw record dicts.
    """
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    records: List[Dict] = []
    date_from = start_date.strftime("%m/%d/%Y")
    date_to   = end_date.strftime("%m/%d/%Y")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        for doc_code in doc_codes:
            try:
                recs = await _scrape_doc_type(
                    page, doc_code, date_from, date_to
                )
                log.info("  %s → %d records", doc_code, len(recs))
                records.extend(recs)
            except Exception:
                log.error("Error scraping %s:\n%s", doc_code, traceback.format_exc())

        await browser.close()

    return records


async def _scrape_doc_type(
    page, doc_code: str, date_from: str, date_to: str
) -> List[Dict]:
    """Navigate the recorder search form for one document type."""
    from playwright.async_api import TimeoutError as PWTimeout

    records: List[Dict] = []

    for attempt in range(RETRY_ATTEMPTS):
        try:
            await page.goto(CLERK_SEARCH, timeout=30000, wait_until="networkidle")
            await asyncio.sleep(1)
            # Fill search form – selectors observed on the Kane County Recorder portal
            # Document type dropdown
            try:
                await page.select_option('select[name*="DocType"], select[id*="DocType"]', label=doc_code, timeout=5000)
            except Exception:
                try:
                    await page.select_option('select[name*="DocType"], select[id*="DocType"]', value=doc_code, timeout=5000)
                except Exception:
                    # Try text search in the dropdown options
                    pass

            # Date range
            for sel in ['input[name*="DateFrom"], input[id*="DateFrom"]']:
                try:
                    await page.fill(sel, date_from, timeout=5000)
                    break
                except Exception:
                    pass
            for sel in ['input[name*="DateTo"], input[id*="DateTo"]']:
                try:
                    await page.fill(sel, date_to, timeout=5000)
                    break
                except Exception:
                    pass

            # Submit
            await page.click('input[type="submit"], button[type="submit"]', timeout=10000)
            await page.wait_for_load_state("networkidle", timeout=30000)
            break
        except PWTimeout:
            if attempt == RETRY_ATTEMPTS - 1:
                raise
            await asyncio.sleep(RETRY_DELAY)

    # Parse results
    html = await page.content()
    records = _parse_search_results(html, doc_code)

    # Handle pagination
    page_num = 2
    while True:
        try:
            next_btn = await page.query_selector(
                'a:has-text("Next"), input[value="Next >"], a[id*="Next"]'
            )
            if not next_btn:
                break
            await next_btn.click()
            await page.wait_for_load_state("networkidle", timeout=20000)
            html = await page.content()
            new_recs = _parse_search_results(html, doc_code)
            if not new_recs:
                break
            records.extend(new_recs)
            page_num += 1
        except Exception:
            break

    return records


def _parse_search_results(html: str, doc_code: str) -> List[Dict]:
    """Parse HTML results table from the clerk portal."""
    soup = BeautifulSoup(html, "lxml")
    records: List[Dict] = []

    # Look for results in a table (common pattern for these portals)
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        # Detect header row
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if not any(h in headers for h in ["doc", "document", "grantor", "date", "book"]):
            continue
        # Map header positions
        col = {h: i for i, h in enumerate(headers)}

        def gcol(row_cells, *names) -> str:
            for n in names:
                for hk, idx in col.items():
                    if n in hk and idx < len(row_cells):
                        return safe_text(row_cells[idx])
            return ""

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            try:
                doc_num  = gcol(cells, "doc", "document number", "instrument")
                filed    = gcol(cells, "date", "recorded", "filed")
                grantor  = gcol(cells, "grantor", "owner", "name")
                grantee  = gcol(cells, "grantee", "party")
                legal    = gcol(cells, "legal", "description")
                amount   = gcol(cells, "amount", "consideration", "value")
                # Build direct URL
                link_el  = row.find("a", href=True)
                href     = link_el["href"] if link_el else ""
                if href and not href.startswith("http"):
                    href = CLERK_BASE + "/" + href.lstrip("/")

                if not doc_num and not grantor:
                    continue

                records.append({
                    "doc_num":   doc_num,
                    "doc_code":  doc_code,
                    "filed":     filed,
                    "grantor":   grantor,
                    "grantee":   grantee,
                    "legal":     legal,
                    "amount":    parse_amount(amount),
                    "clerk_url": href or CLERK_SEARCH,
                })
            except Exception:
                pass
    return records


# ---------------------------------------------------------------------------
# Fallback: requests + BeautifulSoup for static portal pages
# ---------------------------------------------------------------------------

def scrape_clerk_requests(
    doc_codes: List[str],
    start_date: datetime,
    end_date: datetime,
) -> List[Dict]:
    """
    Requests-based fallback scraper for Kane County Recorder.
    Uses __doPostBack form submission pattern common on ASP.NET portals.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "Chrome/120 Safari/537.36"
        )
    })
    records: List[Dict] = []
    date_from = start_date.strftime("%m/%d/%Y")
    date_to   = end_date.strftime("%m/%d/%Y")

    for doc_code in doc_codes:
        try:
            recs = _requests_search_doc(session, doc_code, date_from, date_to)
            log.info("  %s (requests) → %d records", doc_code, len(recs))
            records.extend(recs)
        except Exception:
            log.error("Requests error for %s:\n%s", doc_code, traceback.format_exc())

    return records


def _requests_search_doc(
    session: requests.Session,
    doc_code: str,
    date_from: str,
    date_to: str,
) -> List[Dict]:
    """POST to the recorder search portal for a single doc type."""

    def get_page():
        r = session.get(CLERK_SEARCH, timeout=30)
        r.raise_for_status()
        return r

    resp = retry(get_page)
    soup = BeautifulSoup(resp.text, "lxml")

    # Extract ASP.NET ViewState
    viewstate     = _val(soup, "__VIEWSTATE")
    eventval      = _val(soup, "__EVENTVALIDATION")
    viewstategen  = _val(soup, "__VIEWSTATEGENERATOR")

    # Build POST payload – field names may vary; these are common patterns
    payload = {
        "__VIEWSTATE":          viewstate,
        "__EVENTVALIDATION":    eventval,
        "__VIEWSTATEGENERATOR": viewstategen,
        "__EVENTTARGET":        "",
        "__EVENTARGUMENT":      "",
        "ctl00$ContentPlaceHolder1$txtDateFrom": date_from,
        "ctl00$ContentPlaceHolder1$txtDateTo":   date_to,
        "ctl00$ContentPlaceHolder1$ddlDocType":  doc_code,
        "ctl00$ContentPlaceHolder1$btnSearch":   "Search",
    }

    def do_post():
        r = session.post(CLERK_SEARCH, data=payload, timeout=30)
        r.raise_for_status()
        return r

    try:
        resp2 = retry(do_post)
        records = _parse_search_results(resp2.text, doc_code)
    except Exception:
        records = []

    return records


def _val(soup, field_name: str) -> str:
    el = soup.find("input", {"name": field_name})
    return el["value"] if el else ""


# ---------------------------------------------------------------------------
# Scoring Engine
# ---------------------------------------------------------------------------

def compute_score(record: Dict) -> Tuple[int, List[str]]:
    """
    Compute a motivated-seller score (0-100) and flag list.
    Base: 30 pts; each flag: +10; LP+FC combo: +20; high amount bonus; etc.
    """
    score = 30
    flags: List[str] = list(record.get("_base_flags", []))
    doc_code = record.get("doc_code", "")
    amount   = record.get("amount", 0.0)
    filed    = record.get("filed", "")
    owner    = record.get("grantor", "")

    # Flag: LLC / Corp owner
    if re.search(r"\b(LLC|INC|CORP|L\.L\.C|CO\.|COMPANY|TRUST)\b", (owner or "").upper()):
        if "LLC / corp owner" not in flags:
            flags.append("LLC / corp owner")

    # Flag: New this week (filed within 7 days)
    try:
        filed_dt = datetime.strptime(re.sub(r"\s+", " ", filed.strip()), "%m/%d/%Y")
        cutoff = datetime.now() - timedelta(days=7)
        if filed_dt >= cutoff:
            flags.append("New this week")
    except Exception:
        pass

    # Flag: has property address
    has_addr = bool(record.get("prop_address"))

    # Score additions
    seen_flags = set(flags)
    score += 10 * len(seen_flags)

    # LP + Foreclosure combo
    all_flags_set = {f.lower() for f in seen_flags}
    if "lis pendens" in all_flags_set and "pre-foreclosure" in all_flags_set:
        score += 20

    # Amount bonuses
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10

    # New this week
    if "new this week" in {f.lower() for f in seen_flags}:
        score += 5

    # Has address
    if has_addr:
        score += 5

    return min(score, 100), flags


# ---------------------------------------------------------------------------
# Record Builder
# ---------------------------------------------------------------------------

def build_records(
    raw: List[Dict], parcel: ParcelLookup, run_dt: datetime
) -> List[Dict]:
    """Enrich raw scraper records with parcel data, scoring, etc."""
    output: List[Dict] = []
    cutoff = run_dt - timedelta(days=LOOK_BACK_DAYS)

    for raw_rec in raw:
        try:
            doc_code = raw_rec.get("doc_code", "")
            cat_info = DOC_TYPE_MAP.get(doc_code, ("other", doc_code, []))
            cat, cat_label, base_flags = cat_info

            # Filter by date
            filed_str = raw_rec.get("filed", "")
            try:
                filed_dt = datetime.strptime(re.sub(r"\s+", " ", filed_str.strip()), "%m/%d/%Y")
                if filed_dt < cutoff:
                    continue
            except Exception:
                pass  # Keep records with unparseable dates

            raw_rec["_base_flags"] = base_flags[:]
            owner = raw_rec.get("grantor", "")

            # Parcel lookup
            parcel_rec = parcel.lookup(owner)
            prop_addr  = ""
            prop_city  = ""
            prop_state = "IL"
            prop_zip   = ""
            mail_addr  = ""
            mail_city  = ""
            mail_state = ""
            mail_zip   = ""

            if parcel_rec:
                prop_addr  = parcel_rec.get("site_addr", "")
                prop_city  = parcel_rec.get("site_city", "")
                prop_zip   = parcel_rec.get("site_zip", "")
                mail_addr  = parcel_rec.get("mail_addr", "")
                mail_city  = parcel_rec.get("mail_city", "")
                mail_state = parcel_rec.get("mail_state", "IL")
                mail_zip   = parcel_rec.get("mail_zip", "")

            rec: Dict[str, Any] = {
                "doc_num":     raw_rec.get("doc_num", ""),
                "doc_type":    doc_code,
                "filed":       filed_str,
                "cat":         cat,
                "cat_label":   cat_label,
                "owner":       owner,
                "grantee":     raw_rec.get("grantee", ""),
                "amount":      raw_rec.get("amount", 0.0),
                "legal":       raw_rec.get("legal", ""),
                "prop_address":prop_addr,
                "prop_city":   prop_city,
                "prop_state":  prop_state,
                "prop_zip":    prop_zip,
                "mail_address":mail_addr,
                "mail_city":   mail_city,
                "mail_state":  mail_state,
                "mail_zip":    mail_zip,
                "clerk_url":   raw_rec.get("clerk_url", CLERK_SEARCH),
                "flags":       [],
                "score":       0,
            }

            score, flags = compute_score({**raw_rec, **rec})
            rec["score"] = score
            rec["flags"] = flags

            output.append(rec)
        except Exception:
            log.error("Error building record: %s", traceback.format_exc())

    return output


# ---------------------------------------------------------------------------
# GHL CSV Export
# ---------------------------------------------------------------------------

def export_ghl_csv(records: List[Dict], path: Path):
    """Export records to a GoHighLevel-compatible CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for rec in records:
            owner = rec.get("owner", "")
            # Try to split owner into First/Last
            name_parts = owner.replace(",", " ").split()
            first = name_parts[0] if name_parts else ""
            last  = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
            writer.writerow({
                "First Name":            first,
                "Last Name":             last,
                "Mailing Address":       rec.get("mail_address", ""),
                "Mailing City":          rec.get("mail_city", ""),
                "Mailing State":         rec.get("mail_state", ""),
                "Mailing Zip":           rec.get("mail_zip", ""),
                "Property Address":      rec.get("prop_address", ""),
                "Property City":         rec.get("prop_city", ""),
                "Property State":        rec.get("prop_state", "IL"),
                "Property Zip":          rec.get("prop_zip", ""),
                "Lead Type":             rec.get("cat_label", ""),
                "Document Type":         rec.get("doc_type", ""),
                "Date Filed":            rec.get("filed", ""),
                "Document Number":       rec.get("doc_num", ""),
                "Amount/Debt Owed":      rec.get("amount", 0),
                "Seller Score":          rec.get("score", 0),
                "Motivated Seller Flags":"; ".join(rec.get("flags", [])),
                "Source":                "Kane County Recorder",
                "Public Records URL":    rec.get("clerk_url", ""),
            })
    log.info("GHL CSV → %s (%d rows)", path, len(records))


# ---------------------------------------------------------------------------
# Output Writer
# ---------------------------------------------------------------------------

def write_output(records: List[Dict], run_dt: datetime):
    """Write dashboard/records.json and data/records.json."""
    start_dt = run_dt - timedelta(days=LOOK_BACK_DAYS)
    payload = {
        "fetched_at":    run_dt.isoformat(),
        "source":        "Kane County Recorder",
        "date_range":    {
            "from": start_dt.strftime("%Y-%m-%d"),
            "to":   run_dt.strftime("%Y-%m-%d"),
        },
        "total":         len(records),
        "with_address":  sum(1 for r in records if r.get("prop_address")),
        "records":       records,
    }
    for dir_name in ["dashboard", "data"]:
        p = Path(dir_name)
        p.mkdir(exist_ok=True)
        out = p / "records.json"
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Wrote %s (%d records)", out, len(records))
    # GHL CSV
    export_ghl_csv(records, Path("data/ghl_export.csv"))


# ---------------------------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------------------------

async def main():
    run_dt   = datetime.now(timezone.utc).replace(tzinfo=None)
    end_dt   = run_dt
    start_dt = run_dt - timedelta(days=LOOK_BACK_DAYS)

    log.info("=== Kane County Lead Scraper ===")
    log.info("Date range: %s → %s", start_dt.date(), end_dt.date())
    log.info("Doc types: %s", ALL_DOC_CODES)

    # 1. Load parcel data
    parcel = ParcelLookup()
    parcel.load()

    # 2. Scrape clerk portal via Playwright
    raw_records: List[Dict] = []
    try:
        log.info("Starting Playwright scrape …")
        raw_records = await scrape_clerk_playwright(
            ALL_DOC_CODES, start_dt, end_dt
        )
    except Exception:
        log.error("Playwright failed, falling back to requests:\n%s", traceback.format_exc())
        raw_records = scrape_clerk_requests(
            ALL_DOC_CODES, start_dt, end_dt
        )

    log.info("Raw records scraped: %d", len(raw_records))

    # 3. Enrich + score
    records = build_records(raw_records, parcel, run_dt)
    log.info("Enriched records: %d", len(records))

    # 4. Sort by score descending
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 5. Write output
    write_output(records, run_dt)
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())

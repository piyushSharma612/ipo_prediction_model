"""
IPO Master Scraper v6.0
━━━━━━━━━━━━━━━━━━━━━━
Full & final scraper for Chittorgarh IPO data (2010–2025).

KEY FIXES vs v5.1:
  [1] Promoter Holding:
      - v5.1 bug: single 'Promoter Holding' field captured pre-IPO 100%
      - v6 fix: scrape 'Share Holding Pre Issue' and 'Share Holding Post Issue'
        separately. If single 'Promoter Holding' = 100% → NaN (impossible post-IPO)
  [2] anchor_pct_of_qib:
      - v5.1 bug: picked up SEBI regulatory "up to 60%" text
      - v6 fix: look for actual allocated % only in the subscription data row,
        validate 0-100 range
  [3] GMP:
      - v5.1 bug: /ipo_gmp/ returns 404, set all to 0
      - v6 fix: attempt investorgain.com for recent IPOs
  [4] Multi-year:
      - v5.1 bug: only tested on 2023
      - v6 fix: handles empty years, varying page structures 2010-2025
  [5] New columns:
      - promoter_holding_pre, promoter_dilution_pct
      - ipo_open_date, ipo_close_date, ipo_days_open
      - lead_manager, listing_open_price
      - sector (from detail page)
      - shares_offered, market_cap_pre_ipo

INSTALL:
  pip install selenium beautifulsoup4 lxml pandas yfinance rapidfuzz requests

USAGE:
  python ipo_scraper_v6.py                          # full 2010-2025 run
  python ipo_scraper_v6.py --start 2023 --end 2023  # single year test
  python ipo_scraper_v6.py --reset-db               # clear cache and re-scrape
"""

import re
import os
import sys
import time
import json
import random
import sqlite3
import logging
import argparse
import tempfile
import warnings
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
from rapidfuzz import process as fuzz_process
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException

warnings.filterwarnings("ignore")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BASE         = "https://www.chittorgarh.com"
N_DRIVERS    = 3
SLEEP_MIN    = 2.5
SLEEP_MAX    = 5.5
MAX_RETRIES  = 3
BACKOFF_BASE = 2
DB_PATH      = "scrape_progress_v6.db"
OUT_PATH     = "ipo_master_2010_2025.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

NO_DATA_PHRASES = [
    "data not available", "no data found", "page not found",
    "information not available", "couldn't find that page",
    "anchor investor data not available", "no anchor investor",
]

TOP_ANCHOR_NAMES = [
    "SBI", "HDFC", "Nippon", "ICICI Prudential", "Kotak", "Axis",
    "DSP", "Mirae", "UTI", "Blackrock", "Fidelity", "Goldman",
    "Morgan Stanley", "Government Pension", "Motilal", "Aditya Birla",
    "Tata", "Franklin", "Edelweiss", "Abu Dhabi", "Nomura",
    "Canada Pension", "GIC", "Monetary Authority", "Vanguard",
]

# Sector inference from company name keywords
SECTOR_KEYWORDS = {
    "pharma|healthcare|hospital|medical|biotech|life science|drug|therapeut": "Healthcare & Pharma",
    "bank|finance|finserv|capital|credit|lending|insurance|broking|fintech": "Financial Services",
    "tech|software|digital|cyber|data|cloud|info|computer|consult": "Technology & IT",
    "infra|construct|engineer|cement|steel|metal|mining|power|energy|renew": "Infrastructure & Industrial",
    "food|agro|beverage|dairy|fmcg|consumer|retail|brand|cloth|textile|fashion": "FMCG & Consumer",
    "auto|vehicle|motor|tyre|transport|logistic": "Automobile & Transport",
    "real estate|property|housing|developer": "Real Estate",
    "chem|petro|oil|gas|refin|polymer|plastic": "Chemicals & Petrochemicals",
    "electr|device|semiconductor|sensor|circuit|component": "Electronics & Components",
    "media|entertain|film|broadcast|adverti": "Media & Entertainment",
    "telecom|communication|network": "Telecom",
    "education|learn|train|edtech": "Education",
    "hotel|travel|tour|hospitality|restaurant": "Hospitality & Travel",
    "jewel|gem|diamond|gold": "Jewellery & Gems",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper_v6.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SQLITE RESUME DB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS scraped_ipos (
        ipo_id TEXT PRIMARY KEY, slug TEXT, company TEXT,
        year INTEGER, scraped_at TEXT, status TEXT, data_json TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS scrape_errors (
        ipo_id TEXT, url TEXT, error TEXT, ts TEXT
    )""")
    conn.commit()
    return conn

def is_scraped(conn, ipo_id):
    row = conn.execute(
        "SELECT status FROM scraped_ipos WHERE ipo_id=?", (ipo_id,)
    ).fetchone()
    return row is not None and row[0] in ("ok", "partial")

def save_row(conn, ipo_id, slug, company, year, data, status="ok"):
    conn.execute(
        "INSERT OR REPLACE INTO scraped_ipos VALUES (?,?,?,?,?,?,?)",
        (ipo_id, slug, company, year,
         datetime.utcnow().isoformat(), status, json.dumps(data))
    )
    conn.commit()

def log_error(conn, ipo_id, url, error):
    conn.execute(
        "INSERT INTO scrape_errors VALUES (?,?,?,?)",
        (ipo_id, url, str(error), datetime.utcnow().isoformat())
    )
    conn.commit()

def reset_db():
    """Delete the old DB so all IPOs get re-scraped with v6 logic."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        log.info(f"Deleted old DB: {DB_PATH}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DRIVER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def setup_driver(ua_index=0):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,800")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Isolated temp profile per driver — prevents cxxbridge panic
    tmp_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={tmp_dir}")
    options.add_argument("--renderer-process-limit=1")
    options.add_argument("--js-flags=--max-old-space-size=512")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(f"--user-agent={USER_AGENTS[ua_index % len(USER_AGENTS)]}")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    driver.execute_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )
    return driver

def is_driver_alive(driver):
    """Check if the Selenium driver session is still responsive."""
    try:
        driver.title
        return True
    except Exception:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FETCH WITH RETRY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_soup(url, driver, retries=MAX_RETRIES, require_table=False):
    for attempt in range(retries):
        try:
            driver.get(url)
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

            if require_table:
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "table"))
                    )
                except TimeoutException:
                    log.debug(f"No <table> at {url}")
            else:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

            soup = BeautifulSoup(driver.page_source, "lxml")
            page_text = soup.get_text(" ", strip=True).lower()
            if any(p in page_text for p in NO_DATA_PHRASES):
                log.debug(f"No-data page: {url}")
                return None
            title = soup.find("title")
            if title and any(x in title.text.lower() for x in ["404", "not found", "error"]):
                log.debug(f"Error page: {url}")
                return None
            return soup

        except WebDriverException as e:
            err_msg = str(e).lower()
            if "invalid session id" in err_msg or "no such session" in err_msg or "communication" in err_msg:
                log.error(f"FATAL: Browser session lost during {url[:40]}...")
                raise e # Let the worker handle re-init

            wait = (BACKOFF_BASE ** attempt) * 2 + random.uniform(0, 2)
            log.warning(f"ChromeDriver {attempt+1}/{retries} [{url[:60]}]: {str(e)[:80]}")
            time.sleep(wait)
        except Exception as e:
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 1)
            log.warning(f"Attempt {attempt+1}/{retries} [{url[:60]}]: {str(e)[:80]}")
            time.sleep(wait)

    log.error(f"All retries failed: {url[:80]}")
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# VALUE CLEANERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_num(x):
    """
    Extracts first valid float. Handles ₹, commas, spaces.
    '₹448' → 448.0  |  '1,234.56' → 1234.56  |  'N/A' → None
    Does NOT strip % — use parse_pct() for percentages.
    """
    if not x:
        return None
    # Remove currency and formatting chars but NOT the decimal point or minus
    cleaned = re.sub(r"[₹Rs\s,]", "", str(x).strip())
    # Remove trailing non-numeric suffixes like 'Cr', 'per share', '%'
    cleaned = re.sub(r"[a-zA-Z%].*$", "", cleaned).strip()
    m = re.search(r"-?\d+\.?\d*", cleaned)
    if m:
        try:
            return float(m.group())
        except ValueError:
            return None
    return None


def parse_pct(text):
    """
    Extracts percentage value preserving sign and decimals.
    '20.85%' → 20.85  |  '-5.2%' → -5.2  |  '100%' → 100.0
    """
    if not text:
        return None
    # Strip everything except digits, dot, minus
    cleaned = re.sub(r"[^\d.\-]", "", str(text).strip())
    m = re.search(r"-?\d+\.?\d*", cleaned)
    return float(m.group()) if m else None


def extract_crore(text):
    """
    Extracts Crore amounts from strings like:
    '₹ 570 Cr'  → 570.0
    'agg. up to ₹ 570 Cr'  → 570.0
    '1,27,23,214 shares (agg. up to ₹ 570 Cr)'  → 570.0
    """
    if not text:
        return None
    m = re.search(r"(?:Rs\.?|₹)\s*([\d,]+\.?\d*)\s*(?:Cr(?:ore)?)", text, re.I)
    if m:
        return clean_num(m.group(1))
    # Fallback: just find number before Cr
    m2 = re.search(r"([\d,]+\.?\d*)\s*Cr(?:ore)?", text, re.I)
    if m2:
        return clean_num(m2.group(1))
    return None


def extract_shares(text):
    """
    Extracts share count from strings like:
    '1,27,23,214 shares (agg. up to ₹ 570 Cr)' → 12723214
    '26,88,000 shares' → 2688000
    """
    if not text:
        return None
    m = re.search(r"([\d,]+)\s*shares", text, re.I)
    if m:
        return int(m.group(1).replace(",", ""))
    return None


def _parse_date(date_str):
    """Tries multiple date formats. Returns datetime or None."""
    if not date_str or str(date_str).strip().lower() in ("", "nan", "none", "-"):
        return None
    s = str(date_str).strip()
    for fmt in (
        "%d %b %Y", "%Y-%m-%d", "%d-%m-%Y", "%b %d, %Y",
        "%d/%m/%Y", "%B %d, %Y", "%a, %b %d, %Y",
        "Fri, %b %d, %Y", "%a, %d %b %Y",
        "%b %d, %Y", "%d %B %Y",
        # Formats with day-of-week prefix like "Wed, Nov 15, 2023"
    ):
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, AttributeError):
            continue
    # Try stripping day-of-week prefix: "Mon, Jul 10, 2023" → "Jul 10, 2023"
    stripped = re.sub(r"^\w+,\s*", "", s)
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b %Y"):
        try:
            return datetime.strptime(stripped, fmt)
        except (ValueError, AttributeError):
            continue
    return None


def infer_sector(company_name, detail_sector=None):
    """
    Infers sector from company name keywords.
    Returns the detail page sector if available, else NLP-based guess.
    """
    if detail_sector and detail_sector.strip() and detail_sector.strip() != "-":
        return detail_sector.strip()
    name = company_name.lower()
    for pattern, sector in SECTOR_KEYWORDS.items():
        if re.search(pattern, name, re.I):
            return sector
    return "Other"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA QUALITY GUARDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def guard_promoter_holding(val, field_name="promoter_holding_post"):
    """
    Reject impossible promoter holding values.
    Post-IPO promoter holding of exactly 100% is pre-IPO, not post-IPO.
    """
    if val is None:
        return None
    if val > 100 or val < 0:
        log.debug(f"  {field_name}={val} out of range → NaN")
        return None
    if field_name == "promoter_holding_post" and val == 100.0:
        log.debug(f"  {field_name}=100% (pre-IPO value) → NaN")
        return None
    return val


def guard_anchor_pct(val):
    """
    anchor_pct_of_qib must be 0-100.
    Values like 142 or 190 are clearly wrong (wrong row scraped).
    """
    if val is None:
        return None
    if val < 0 or val > 100:
        log.debug(f"  anchor_pct_of_qib={val} out of range → NaN")
        return None
    return val


def guard_listing_gain(val):
    """
    listing_gain_pct should be a percentage (-100 to ~500).
    If it's in basis points (>500), divide by 100.
    """
    if val is None:
        return None
    if abs(val) > 500:
        val = round(val / 100, 2)
    return val


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHASE 1: YEAR INDEX
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Table headers (consistent 2010-2025):
# ['Company Name', 'Listed On', 'Issue Price',
#  'Listing Day Close', 'Listing Day Gain',
#  'Current Price', 'Profit/Loss']

def get_year_ipos(year, driver):
    url = f"{BASE}/ipo/ipo_perf_tracker.asp?year={year}"
    soup = get_soup(url, driver, require_table=True)
    if not soup:
        log.warning(f"Could not fetch index for {year}")
        return []

    out = []
    for table in soup.find_all("table"):
        headers_raw = [th.get_text(strip=True) for th in table.find_all("th")]
        headers = [h.lower() for h in headers_raw]

        # Only process the main IPO table (has 'company' in headers)
        if not any("company" in h or "ipo" in h for h in headers):
            continue

        # Build index map from actual header names
        col_idx = {}
        for i, h in enumerate(headers):
            if "company" in h or "name" in h:
                col_idx["name"] = i
            elif "listed on" in h or (("listed" in h or "listing") and "date" in h):
                col_idx["listing_date"] = i
            elif "issue price" in h:
                col_idx["issue_price"] = i
            elif "listing day close" in h or ("listing" in h and "close" in h):
                col_idx["listing_price"] = i
            elif "listing day gain" in h or "gain" in h:
                col_idx["listing_gain_pct"] = i
            elif "listing day open" in h or ("listing" in h and "open" in h):
                col_idx["listing_open_price"] = i

        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            # Company name — strip "IPO Detail | Stock Quotes" noise
            name_raw = cols[0].get_text(" ", strip=True)
            name = re.sub(r"IPO\s*Detail.*", "", name_raw, flags=re.I).strip()
            name = re.sub(r"\|.*$", "", name).strip()
            if not name:
                continue

            # Slug + id from first <a> link
            slug, ipo_id = None, None
            for a in row.find_all("a"):
                href = a.get("href", "")
                m = re.search(r"/ipo/([^/]+)/(\d+)/", href)
                if m:
                    slug, ipo_id = m.group(1), m.group(2)
                    break
            if not slug:
                continue

            row_data = {
                "company_name": name,
                "year":         year,
                "slug":         slug,
                "id":           ipo_id,
            }

            # Extract values using confirmed column indices
            for field, idx in col_idx.items():
                if field == "name":
                    continue
                if idx >= len(cols):
                    continue
                raw = cols[idx].get_text(strip=True)
                if not raw:
                    continue

                if field == "listing_date":
                    row_data[field] = raw  # keep as string, parse later

                elif field == "listing_gain_pct":
                    row_data[field] = guard_listing_gain(parse_pct(raw))

                elif field in ("issue_price", "listing_price", "listing_open_price"):
                    row_data[field] = clean_num(raw)

            out.append(row_data)

    log.info(f"Year {year} -> {len(out)} IPOs")
    return out


# ----------------------------------------------------------------------------
# PHASE 2A: DETAIL PAGE
# ----------------------------------------------------------------------------

def scrape_details(slug, ipo_id, driver):
    url = f"{BASE}/ipo/{slug}/{ipo_id}/"
    soup = get_soup(url, driver, require_table=True)
    if not soup:
        return {}

    data = {}
    sale_type_str = ""

    # Build a flat key->value dict from ALL table rows on the page
    kv = {}
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cols = [c.get_text(" ", strip=True) for c in row.find_all("td")]
            if len(cols) >= 2 and cols[0].strip():
                k = cols[0].strip()
                v = cols[1].strip()
                if k not in kv:  # first occurrence wins
                    kv[k] = v

    # -- Also extract from descriptive text blocks ------------------
    # Some pages have info in paragraph text rather than tables
    page_text = soup.get_text(" ", strip=True)

    # Now map using broad key matching for year-to-year robustness
    for k, v in kv.items():
        kl = k.lower().strip()

        # -- Issue structure ----------------------------------------
        if kl in ("sale type", "issue type"):
            sale_type_str = v.lower()
            data["sale_type"] = v

        elif kl == "issue price":
            data["offer_price"] = clean_num(v)

        elif kl == "price band":
            # '₹426 to ₹448' -> low and high
            nums = re.findall(r"[\d,]+", v.replace("₹", ""))
            prices = [float(n.replace(",", "")) for n in nums if n.replace(",", "").isdigit()]
            if len(prices) >= 2:
                data["price_band_low"]  = min(prices)
                data["price_band_high"] = max(prices)
            elif len(prices) == 1:
                data["offer_price"] = data.get("offer_price") or prices[0]

        elif kl == "face value":
            data["face_value"] = clean_num(v)

        elif kl == "lot size":
            data["lot_size"] = clean_num(v)

        elif kl == "total issue size":
            data["issue_size_cr"] = extract_crore(v)
            data["shares_offered"] = extract_shares(v)

        elif kl == "fresh issue":
            data["fresh_issue_cr"] = extract_crore(v)

        elif kl == "offer for sale":
            data["ofs_cr"] = extract_crore(v)

        elif kl == "net offered to public":
            data["net_offered_to_public"] = extract_shares(v)

        # -- Listing ------------------------------------------------
        elif kl in ("listed on", "listing date"):
            data["listing_date_raw"] = v

        elif kl == "listing at":
            data["exchange"] = v

        # -- IPO dates ----------------------------------------------
        elif kl in ("ipo date", "ipo open"):
            data["ipo_open_date"] = v

        elif kl in ("ipo close",):
            data["ipo_close_date"] = v

        # -- NSE ticker ---------------------------------------------
        elif "nse symbol" in kl or "nse script" in kl or ("bse" in kl and "nse" in kl):
            parts = v.split("/")
            if len(parts) >= 2:
                data["nse_symbol"] = parts[-1].strip()
                data["bse_code"]   = parts[0].strip()
            elif len(parts) == 1 and v.strip().isupper():
                data["nse_symbol"] = v.strip()

        # -- Financials — SINGLE values -----------------------------
        elif kl == "total income":
            data["revenue_cr"] = clean_num(v)

        elif kl in ("profit after tax", "pat"):
            data["pat_cr"] = clean_num(v)

        elif kl in ("net worth", "net worth (cr)", "networth"):
            data["net_worth_cr"] = clean_num(v)

        elif kl in ("assets", "total assets"):
            data["total_assets_cr"] = clean_num(v)

        elif kl in ("total borrowing", "total borrowings", "total debt"):
            data["total_debt_cr"] = clean_num(v)

        elif kl == "reserves and surplus":
            data["reserves_cr"] = clean_num(v)

        # -- Valuation ratios ---------------------------------------
        elif kl in ("roe", "return on equity"):
            data["roe"] = parse_pct(v)

        elif kl in ("roce", "return on capital employed"):
            data["roce"] = parse_pct(v)

        elif kl in ("ronw", "return on net worth", "rona"):
            data["ronw"] = parse_pct(v)

        elif kl in ("p/e (x)", "p/e ratio", "pe ratio", "pe"):
            if "pe_ratio" not in data:
                data["pe_ratio"] = clean_num(v)

        elif kl in ("p/b", "p/b ratio", "pb ratio"):
            if "pb_ratio" not in data:
                data["pb_ratio"] = clean_num(v)

        elif kl in ("pat margin", "pat margin (%)"):
            data["pat_margin_pct"] = parse_pct(v)

        elif kl in ("eps (₹)", "eps", "earnings per share"):
            data["eps"] = clean_num(v)

        elif kl in ("debt/equity", "debt / equity", "d/e ratio"):
            data["debt_equity"] = clean_num(v)

        elif "market cap" in kl:
            # Distinguish pre-IPO market cap from post-listing
            if "pre" in kl:
                data["market_cap_pre_ipo_cr"] = extract_crore(v) or clean_num(v)
            else:
                data["market_cap_cr"] = extract_crore(v) or clean_num(v)

        elif kl in ("ev/ebitda", "ev / ebitda"):
            data["ev_ebitda"] = clean_num(v)

        # -- Promoter holding — FIX: capture pre AND post separately --
        # Chittorgarh detail pages have these possible keys:
        #   'Promoter Holding' -> ambiguous (sometimes pre, sometimes post)
        #   'Share Holding Pre Issue' -> pre-IPO %
        #   'Share Holding Post Issue' -> post-IPO %
        elif "share holding pre" in kl or "shareholding pre" in kl or "pre issue" in kl:
            data["promoter_holding_pre"] = parse_pct(v)

        elif "share holding post" in kl or "shareholding post" in kl or "post issue" in kl:
            raw_post = parse_pct(v)
            data["promoter_holding_post"] = guard_promoter_holding(raw_post, "promoter_holding_post")

        elif "promoter holding" in kl or "promoter share" in kl:
            # This is the ambiguous single value
            raw_val = parse_pct(v)
            # Only use as post-IPO if we don't already have a specific post value
            if "promoter_holding_post" not in data:
                data["promoter_holding_post"] = guard_promoter_holding(raw_val, "promoter_holding_post")
            # Also use as pre-IPO if we don't have that
            if "promoter_holding_pre" not in data:
                data["promoter_holding_pre"] = raw_val

        # -- Anchor ------------------------------------------------
        elif "anchor portion" in kl:
            data["anchor_amount_cr"] = clean_num(v)

        # -- Sector/metadata ---------------------------------------
        elif kl in ("sector", "industry", "category"):
            if "sector" not in data:
                data["sector"] = v

        elif kl == "registrar":
            data["registrar"] = v

        elif kl == "isin":
            data["isin"] = v

    # -- Extract Lead Manager from page -----------------------------
    lead_mgr_section = soup.find(text=re.compile(r"Lead Manager", re.I))
    if lead_mgr_section:
        parent = lead_mgr_section.find_parent()
        if parent:
            # Look for links in the next sibling or nearby elements
            links = parent.find_all("a") if parent else []
            if not links:
                # Try the next sibling element
                nxt = parent.find_next_sibling()
                if nxt:
                    links = nxt.find_all("a")
            for a in links:
                href = a.get("href", "")
                if "lead-manager" in href or "ipo-lead" in href:
                    data["lead_manager"] = a.get_text(strip=True)
                    break

    # -- Extract market cap pre-IPO from text ----------------------
    if "market_cap_pre_ipo_cr" not in data:
        m = re.search(r"Market\s+Cap\s*\(Pre[- ]IPO\)\s*[₹Rs.\s]*([\d,.]+)\s*Cr", page_text, re.I)
        if m:
            data["market_cap_pre_ipo_cr"] = clean_num(m.group(1))

    # -- Extract IPO open/close dates from timetable section -------
    if "ipo_open_date" not in data:
        for k2, v2 in kv.items():
            k2l = k2.lower()
            if "ipo open" in k2l or "bid" in k2l and "open" in k2l:
                data["ipo_open_date"] = v2
            elif "ipo close" in k2l or "bid" in k2l and "clos" in k2l:
                data["ipo_close_date"] = v2

    # -- Derived: OFS % ---------------------------------------------
    if data.get("ofs_cr") and data.get("issue_size_cr") and data["issue_size_cr"] > 0:
        data["ofs_pct"] = round(data["ofs_cr"] / data["issue_size_cr"] * 100, 2)

    # Fresh issue fallback
    if "fresh_issue_cr" not in data and "issue_size_cr" in data:
        if "fresh" in sale_type_str and "offer for sale" not in sale_type_str:
            data["fresh_issue_cr"] = data["issue_size_cr"]
        elif data.get("ofs_cr") and data.get("issue_size_cr"):
            data["fresh_issue_cr"] = round(data["issue_size_cr"] - data["ofs_cr"], 2)

    # Derived: Promoter dilution
    if data.get("promoter_holding_pre") and data.get("promoter_holding_post"):
        data["promoter_dilution_pct"] = round(
            data["promoter_holding_pre"] - data["promoter_holding_post"], 2
        )

    # Derived: IPO days open
    if data.get("ipo_open_date") and data.get("ipo_close_date"):
        d1 = _parse_date(data["ipo_open_date"])
        d2 = _parse_date(data["ipo_close_date"])
        if d1 and d2:
            data["ipo_days_open"] = (d2 - d1).days + 1  # inclusive

    # Derived: PAT margin (if not given directly)
    if "pat_margin_pct" not in data and data.get("pat_cr") and data.get("revenue_cr"):
        if data["revenue_cr"] > 0:
            data["pat_margin_pct"] = round(data["pat_cr"] / data["revenue_cr"] * 100, 2)

    # Derived: Debt/equity (if not given directly)
    if "debt_equity" not in data and data.get("total_debt_cr") and data.get("net_worth_cr"):
        if data["net_worth_cr"] > 0:
            data["debt_equity"] = round(data["total_debt_cr"] / data["net_worth_cr"], 2)

    return data


# ----------------------------------------------------------------------------
# PHASE 2B: SUBSCRIPTION PAGE
# ----------------------------------------------------------------------------

def scrape_subscription(slug, ipo_id, driver):
    url = f"{BASE}/ipo_subscription/{slug}/{ipo_id}/"
    soup = get_soup(url, driver, require_table=True)
    if not soup:
        return {}

    tables = soup.find_all("table")
    data   = {}

    # -- A: Subscription x-times table ----------------------------
    CATEGORY_MAP = {
        "qualified institutional": "qib_sub",
        "non institutional":       "hni_sub",
        "retail individual":       "rii_sub",
        "employee":                "employee_sub",
        "total subscription":      "total_sub",
        "total":                   "total_sub",   # fallback
    }

    # Scan all tables, find the one with the most category keyword matches
    best_table_data  = {}
    best_match_count = 0

    for table in tables:
        candidate    = {}
        match_count  = 0

        for row in table.find_all("tr"):
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) < 2:
                continue
            col0 = cols[0].lower().strip()
            # Use the LAST column value — always the final subscription figure
            val  = clean_num(cols[-1])
            if val is None:
                continue

            for keyword, field in CATEGORY_MAP.items():
                if keyword in col0:
                    # Don't let 'total' overwrite 'total subscription'
                    if field == "total_sub" and "total_sub" in candidate:
                        break
                    candidate[field] = val
                    match_count += 1
                    break

        if match_count > best_match_count:
            best_match_count = match_count
            best_table_data  = candidate

    # Accept if we matched at least 2 categories
    if best_match_count >= 2:
        data.update(best_table_data)
    else:
        log.warning(f"  subscription: could not find x-times table for {slug}/{ipo_id}")

    # -- B: Anchor investor rows ----------------------------------
    anchor_names = []
    serial_count = 0

    for table in tables:
        candidate_anchors = []
        candidate_serials = 0

        for row in table.find_all("tr"):
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) < 3:
                continue
            if cols[0].strip().isdigit():
                candidate_serials += 1
                fund_name  = cols[1].strip()
                fund_house = cols[2].strip()
                if fund_name and len(fund_name) > 3:
                    candidate_anchors.append(fund_name)
                if fund_house and len(fund_house) > 3 and fund_house != fund_name:
                    candidate_anchors.append(fund_house)

        # Pick the table with the most serial-numbered rows
        if candidate_serials > serial_count:
            serial_count  = candidate_serials
            anchor_names  = candidate_anchors

    # -- C: Anchor metadata ----------------------------------------
    # FIX: Be more precise about anchor_pct_of_qib —
    # reject the "up to 60%" SEBI regulatory text
    for table in tables:
        for row in table.find_all("tr"):
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) < 2:
                continue
            k = cols[0].lower()
            v = cols[1].strip()

            # Only capture actual anchor allocation %, not regulatory ceiling
            if "anchor" in k and "%" in k and "qib" in k:
                # SKIP if the text says "up to" — that's the regulatory ceiling
                if "up to" in k.lower() or "up to" in v.lower():
                    continue
                candidate_pct = parse_pct(v)
                data["anchor_pct_of_qib"] = guard_anchor_pct(candidate_pct)

            elif "anchor investor price" in k or "anchor price" in k:
                data["anchor_price"] = clean_num(v)

            elif "anchor investor" in k and "amount" in k:
                if "anchor_amount_cr" not in data:
                    data["anchor_amount_cr"] = extract_crore(v) or clean_num(v)

    # -- D: Anchor quality scoring ---------------------------------
    if anchor_names:
        data["anchor_data_available"]  = 1
        data["anchor_investors_count"] = serial_count
        has_top = any(
            any(top.lower() in name.lower() for top in TOP_ANCHOR_NAMES)
            for name in anchor_names
        )
        data["has_top_anchor"]       = int(has_top)
        data["anchor_names_sample"]  = " | ".join(
            list(dict.fromkeys(anchor_names))[:5]
        )
    else:
        data["anchor_data_available"] = 0
        data["has_top_anchor"]        = 0

    # -- E: Derived subscription features -------------------------
    qib = data.get("qib_sub") or 0
    hni = data.get("hni_sub") or 0
    rii = data.get("rii_sub") or 0

    if data.get("qib_sub") and data.get("rii_sub") and data["rii_sub"] > 0:
        data["qib_retail_ratio"] = round(data["qib_sub"] / data["rii_sub"], 3)

    if qib or hni or rii:
        data["sub_intensity_score"] = round(
            qib * 0.5 + hni * 0.3 + rii * 0.2, 2
        )

    return data


# ----------------------------------------------------------------------------
# PHASE 2C: GMP (from InvestorGain)
# ----------------------------------------------------------------------------

def scrape_gmp(slug, ipo_id, year):
    """
    Attempt to scrape GMP data from InvestorGain.
    Only for recent IPOs (2018+) where GMP data is likely available.
    Uses requests (no Selenium) since it's a simple page.
    """
    if year < 2018:
        return {"gmp_data_available": 0}

    url = f"https://www.investorgain.com/chr-gmp/{slug}/{ipo_id}"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return {"gmp_data_available": 0}

        soup = BeautifulSoup(resp.text, "lxml")
        page_text = soup.get_text(" ", strip=True).lower()

        # Check if page actually has GMP data
        if any(p in page_text for p in ["no gmp", "not available", "page not found"]):
            return {"gmp_data_available": 0}

        gmp_data = {"gmp_data_available": 0}

        # Look for GMP values in tables
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cols) < 2:
                    continue
                label = cols[0].lower()
                if "gmp" in label or "grey market" in label:
                    val = clean_num(cols[-1])
                    if val is not None:
                        gmp_data["gmp_data_available"] = 1
                        if "peak" in label or "high" in label or "max" in label:
                            gmp_data["gmp_peak"] = val
                        elif "low" in label or "min" in label:
                            gmp_data["gmp_min"] = val
                        elif "listing" in label or "latest" in label or "current" in label:
                            gmp_data["gmp_listing_day"] = val
                        elif "gmp_listing_day" not in gmp_data:
                            gmp_data["gmp_listing_day"] = val

        # If we found any GMP entries in a different format (list items, spans)
        if gmp_data["gmp_data_available"] == 0:
            gmp_spans = soup.find_all(text=re.compile(r"₹\s*[+-]?\d+", re.I))
            gmp_values = []
            for span in gmp_spans:
                val = clean_num(span)
                if val is not None and abs(val) < 5000:  # sanity check
                    gmp_values.append(val)
            if gmp_values:
                gmp_data["gmp_data_available"] = 1
                gmp_data["gmp_peak"] = max(gmp_values)
                gmp_data["gmp_min"] = min(gmp_values)
                gmp_data["gmp_listing_day"] = gmp_values[-1]  # last entry

        time.sleep(random.uniform(1, 2))  # polite delay
        return gmp_data

    except Exception as e:
        log.debug(f"GMP fetch failed for {slug}: {e}")
        return {"gmp_data_available": 0}


# ----------------------------------------------------------------------------
# PHASE 3: POST-LISTING RETURNS (yfinance)
# ----------------------------------------------------------------------------

_NSE_TICKER_MAP = {}

def build_nse_ticker_map():
    global _NSE_TICKER_MAP
    try:
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        df = pd.read_csv(url)
        _NSE_TICKER_MAP = {
            row["NAME OF COMPANY"].lower().strip(): row["SYMBOL"].strip()
            for _, row in df.iterrows()
        }
        log.info(f"NSE ticker map: {len(_NSE_TICKER_MAP)} companies")
    except Exception as e:
        log.warning(f"NSE ticker map failed: {e}")


def get_nse_ticker(company_name, nse_symbol_from_page=None):
    """
    Priority 1: Use NSE symbol scraped directly from the detail page
    Priority 2: Fuzzy match against NSE equity list
    """
    if nse_symbol_from_page and len(nse_symbol_from_page) <= 20:
        sym = nse_symbol_from_page.strip().upper()
        if re.match(r"^[A-Z0-9&\-]+$", sym):
            return sym

    if not _NSE_TICKER_MAP:
        return None
    name_lower = company_name.lower().strip()
    match = fuzz_process.extractOne(name_lower, _NSE_TICKER_MAP.keys(), score_cutoff=80)
    return _NSE_TICKER_MAP[match[0]] if match else None


def get_post_listing_returns(company_name, listing_date_str, nse_symbol=None):
    ticker = get_nse_ticker(company_name, nse_symbol)
    if not ticker:
        return {}

    listing_date = _parse_date(listing_date_str)
    if not listing_date:
        return {}

    try:
        end_date = listing_date + timedelta(days=400)
        hist = yf.Ticker(f"{ticker}.NS").history(
            start=listing_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
        )
        if hist.empty:
            return {}

        base = hist["Close"].iloc[0]
        returns = {
            "nse_ticker":           ticker,
            "listing_price_actual": round(base, 2),
        }

        for label, td in [("1M", 21), ("3M", 63), ("6M", 126), ("1Y", 252)]:
            if len(hist) >= td:
                returns[f"return_{label}"] = round(
                    (hist["Close"].iloc[td] / base - 1) * 100, 2
                )

        # Alpha vs NIFTY
        nifty = yf.Ticker("^NSEI").history(
            start=listing_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
        )
        if not nifty.empty:
            nb = nifty["Close"].iloc[0]
            for label, td in [("1M", 21), ("3M", 63), ("6M", 126), ("1Y", 252)]:
                if len(nifty) >= td and f"return_{label}" in returns:
                    nr = (nifty["Close"].iloc[td] / nb - 1) * 100
                    returns[f"alpha_{label}"] = round(returns[f"return_{label}"] - nr, 2)

        return returns

    except Exception as e:
        log.warning(f"yfinance failed for {ticker}: {e}")
        return {}


def get_macro_at_listing(listing_date_str):
    listing_date = _parse_date(listing_date_str)
    if not listing_date:
        return {}
    try:
        start = listing_date - timedelta(days=45)
        end   = listing_date + timedelta(days=2)
        nifty = yf.Ticker("^NSEI").history(
            start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d")
        )
        vix = yf.Ticker("^INDIAVIX").history(
            start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d")
        )
        macro = {}
        if not nifty.empty:
            macro["nifty_at_listing"] = round(nifty["Close"].iloc[-1], 2)
            if len(nifty) >= 22:
                macro["nifty_30d_return_pct"] = round(
                    (nifty["Close"].iloc[-1] / nifty["Close"].iloc[-22] - 1) * 100, 2
                )
        if not vix.empty:
            macro["vix_at_listing"] = round(vix["Close"].iloc[-1], 2)
        return macro
    except Exception as e:
        log.warning(f"Macro failed for {listing_date_str}: {e}")
        return {}


# ----------------------------------------------------------------------------
# WORKER
# ----------------------------------------------------------------------------

def scrape_one_ipo(ipo_meta, driver, db_conn):
    slug    = ipo_meta["slug"]
    ipo_id  = ipo_meta["id"]
    name    = ipo_meta["company_name"]
    year    = ipo_meta["year"]
    uid     = f"{year}_{ipo_id}"

    if is_scraped(db_conn, uid):
        log.info(f"SKIP: {name} ({year})")
        return None

    log.info(f"Scraping: {name} ({year})")

    row = {
        "name":   name,
        "year":   year,
        "ipo_id": uid,
        "slug":   slug,
        "url":    f"{BASE}/ipo/{slug}/{ipo_id}/",
        **{k: ipo_meta.get(k) for k in
           ["listing_gain_pct", "issue_price", "listing_price",
            "listing_date", "listing_open_price"]},
    }

    listing_date_str = str(ipo_meta.get("listing_date", ""))

    try:
        # Detail page
        details = scrape_details(slug, ipo_id, driver)
        row.update(details)

        # Infer sector from company name if not found on page
        row["sector"] = infer_sector(name, details.get("sector"))

        # Subscription page
        sub = scrape_subscription(slug, ipo_id, driver)
        row.update(sub)

        # GMP from InvestorGain (lightweight HTTP, no Selenium)
        gmp = scrape_gmp(slug, ipo_id, year)
        row.update(gmp)

        # yfinance returns — use NSE symbol from detail page if available
        nse_sym = details.get("nse_symbol")
        if listing_date_str and listing_date_str != "nan":
            row.update(get_post_listing_returns(name, listing_date_str, nse_sym))
            row.update(get_macro_at_listing(listing_date_str))

        # ML target variables
        if row.get("listing_gain_pct") is not None:
            row["target_listing_positive"] = int(row["listing_gain_pct"] > 0)
            row["target_listing_gt10"]     = int(row["listing_gain_pct"] > 10)

        if row.get("return_1Y") is not None:
            row["target_1y_positive"]  = int(row["return_1Y"] > 0)
        if row.get("alpha_1Y") is not None:
            row["target_beat_nifty"]   = int(row["alpha_1Y"] > 0)

        status = "ok"

    except Exception as e:
        log.error(f"Partial failure {name} ({year}): {e}")
        log_error(db_conn, uid, row["url"], e)
        status = "partial"

    save_row(db_conn, uid, slug, name, year, row, status)
    return row


# ----------------------------------------------------------------------------
# CONCURRENT EXECUTION
# ----------------------------------------------------------------------------

def run_worker_batch(year_ipos, worker_id):
    db_conn = init_db()
    results = []
    driver  = setup_driver(ua_index=worker_id)
    
    # Periodic restart settings
    RESTART_EVERY = 25 
    
    try:
        for i, ipo in enumerate(year_ipos):
            # 1. Periodic rotation to prevent memory leaks
            if i > 0 and i % RESTART_EVERY == 0:
                log.info(f"Worker {worker_id}: Periodic browser rotation...")
                driver.quit()
                driver = setup_driver(ua_index=worker_id)

            # 2. Robust execution with auto-recovery for dead drivers
            max_recovery_attempts = 2
            for attempt in range(max_recovery_attempts):
                try:
                    # Verify health before starting
                    if not is_driver_alive(driver):
                        log.warning(f"Worker {worker_id}: Driver dead before scrape. Restarting...")
                        driver.quit()
                        driver = setup_driver(ua_index=worker_id)
                    
                    res = scrape_one_ipo(ipo, driver, db_conn)
                    if res:
                        results.append(res)
                    break # Success!
                except WebDriverException as e:
                    if attempt < max_recovery_attempts - 1:
                        log.error(f"Worker {worker_id}: Session lost during scrape. Re-initializing driver...")
                        try: driver.quit()
                        except: pass
                        driver = setup_driver(ua_index=worker_id)
                        continue
                    else:
                        log.error(f"Worker {worker_id}: Failed to recover driver after {max_recovery_attempts} tries.")
                        # Log the error but don't crash the whole batch
                        uid = f"{ipo['year']}_{ipo['id']}"
                        log_error(db_conn, uid, f"{BASE}/ipo/{ipo['slug']}/{ipo['id']}/", e)
    finally:
        if driver:
            try: driver.quit()
            except: pass
        db_conn.close()
    return results


# ----------------------------------------------------------------------------
# POST-PROCESSING SANITY CHECKS
# ----------------------------------------------------------------------------

def apply_sanity_checks(df):
    """
    Final data quality pass on the assembled DataFrame.
    Catches anything the per-row guards might have missed.
    """
    # Promoter holding post: reject 100%
    if "promoter_holding_post" in df.columns:
        mask = df["promoter_holding_post"] == 100.0
        if mask.any():
            log.info(f"  Sanity: {mask.sum()} rows with promoter_holding_post=100% -> NaN")
            df.loc[mask, "promoter_holding_post"] = np.nan

    # anchor_pct_of_qib: must be 0-100
    if "anchor_pct_of_qib" in df.columns:
        mask = (df["anchor_pct_of_qib"] > 100) | (df["anchor_pct_of_qib"] < 0)
        if mask.any():
            log.info(f"  Sanity: {mask.sum()} rows with anchor_pct_of_qib out of range -> NaN")
            df.loc[mask, "anchor_pct_of_qib"] = np.nan

    # listing_gain_pct: should be percentage not basis points
    if "listing_gain_pct" in df.columns:
        mask = df["listing_gain_pct"].abs() > 500
        if mask.any():
            log.info(f"  Sanity: {mask.sum()} rows with listing_gain_pct > 500 -> dividing by 100")
            df.loc[mask, "listing_gain_pct"] = df.loc[mask, "listing_gain_pct"] / 100

    # Offer price cross-validation: index vs detail page
    if "offer_price" in df.columns and "issue_price" in df.columns:
        mask = df["offer_price"].isna() & df["issue_price"].notna()
        if mask.any():
            df.loc[mask, "offer_price"] = df.loc[mask, "issue_price"]

    return df


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="IPO Scraper v6.0")
    parser.add_argument("--start",    type=int, default=2010)
    parser.add_argument("--end",      type=int, default=2025)
    parser.add_argument("--workers",  type=int, default=N_DRIVERS)
    parser.add_argument("--reset-db", "--fresh", action="store_true",
                        help="Delete cached DB and re-scrape everything")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"IPO Scraper v6.0 | {args.start}-{args.end} | {args.workers} workers")
    log.info("=" * 60)

    if args.reset_db:
        reset_db()

    build_nse_ticker_map()

    # Phase 1: collect IPO index
    log.info("Phase 1: Building IPO index...")
    index_driver = setup_driver()
    all_ipos = []
    
    try:
        for year in range(args.start, args.end + 1):
            max_index_retries = 2
            for attempt in range(max_index_retries):
                try:
                    # Health check for the index driver
                    if not is_driver_alive(index_driver):
                        log.warning(f"Index driver dead. Restarting for year {year}...")
                        index_driver.quit()
                        index_driver = setup_driver()

                    ipos = get_year_ipos(year, index_driver)
                    
                    # If it returned empty because of a stealth session loss that get_soup didn't catch
                    # (though get_soup now throws WebDriverException for session loss)
                    all_ipos.extend(ipos)
                    if not ipos:
                        log.info(f"  Year {year}: 0 mainboard IPOs (expected for some years)")
                    break # Success for this year
                    
                except WebDriverException as e:
                    if attempt < max_index_retries - 1:
                        log.error(f"Index driver lost session for year {year}. Restarting driver...")
                        try: index_driver.quit()
                        except: pass
                        index_driver = setup_driver()
                        continue
                    else:
                        log.error(f"Phase 1 failed for year {year} after {max_index_retries} attempts.")
                        # Move to next year rather than crashing everything
    finally:
        if index_driver:
            try: index_driver.quit()
            except: pass

    log.info(f"Total IPOs to scrape: {len(all_ipos)}")

    if not all_ipos:
        log.error("No IPOs found. Check if the site is accessible.")
        return

    # Phase 2: concurrent scraping
    log.info(f"Phase 2: Scraping {len(all_ipos)} IPOs...")
    batches = [all_ipos[i::args.workers] for i in range(args.workers)]
    all_results = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(run_worker_batch, batch, i): i
            for i, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            wid = futures[future]
            try:
                r = future.result()
                all_results.extend(r)
                log.info(f"Worker {wid} done: {len(r)} IPOs")
            except Exception as e:
                log.error(f"Worker {wid} crashed: {e}")

    # Phase 3: merge with prior runs from SQLite
    db_conn = init_db()
    db_rows = db_conn.execute(
        "SELECT data_json FROM scraped_ipos WHERE status IN ('ok','partial')"
    ).fetchall()
    db_conn.close()

    seen = {r["ipo_id"] for r in all_results if r}
    for (djson,) in db_rows:
        row = json.loads(djson)
        if row.get("ipo_id") not in seen:
            all_results.append(row)
            seen.add(row.get("ipo_id"))

    # Phase 4: build DataFrame
    df = pd.DataFrame([r for r in all_results if r])
    if df.empty:
        log.error("No data. Check scraper_v6.log")
        return

    # Ensure consistent columns
    for col in ["revenue_cr", "pat_cr", "net_worth_cr", "total_debt_cr", "sector", "nse_symbol"]:
        if col not in df.columns:
            df[col] = np.nan

    # Ensure numeric types
    num_cols = [
        "offer_price", "issue_price", "listing_price", "listing_open_price",
        "listing_gain_pct",
        "issue_size_cr", "fresh_issue_cr", "ofs_cr", "ofs_pct",
        "face_value", "lot_size", "shares_offered",
        "revenue_cr", "pat_cr", "net_worth_cr", "total_assets_cr",
        "total_debt_cr", "reserves_cr",
        "roe", "roce", "ronw", "pe_ratio", "pb_ratio",
        "pat_margin_pct", "eps", "debt_equity", "ev_ebitda",
        "market_cap_cr", "market_cap_pre_ipo_cr",
        "promoter_holding_pre", "promoter_holding_post", "promoter_dilution_pct",
        "qib_sub", "hni_sub", "rii_sub", "total_sub", "employee_sub",
        "qib_retail_ratio", "sub_intensity_score",
        "anchor_amount_cr", "anchor_pct_of_qib", "anchor_price",
        "anchor_investors_count", "has_top_anchor",
        "gmp_peak", "gmp_min", "gmp_listing_day",
        "return_1M", "return_3M", "return_6M", "return_1Y",
        "alpha_1M", "alpha_3M", "alpha_6M", "alpha_1Y",
        "nifty_at_listing", "nifty_30d_return_pct", "vix_at_listing",
        "ipo_days_open",
    ]
    for col in num_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Apply final sanity checks
    df = apply_sanity_checks(df)

    # Sort and save
    df.sort_values(["year", "name"], inplace=True)
    df.to_csv(OUT_PATH, index=False)
    df.to_parquet(OUT_PATH.replace(".csv", ".parquet"), index=False)

    log.info("=" * 60)
    log.info(f"DONE - {len(df)} IPOs | {len(df.columns)} columns -> {OUT_PATH}")
    log.info("=" * 60)

    # -- Summary stats ---------------------------------------------
    print("\n-- Null counts (non-zero only) --")
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0].sort_values(ascending=False)
    print(nulls.to_string() if not nulls.empty else "None")

    print(f"\n-- Year breakdown --")
    print(df["year"].value_counts().sort_index().to_string())

    if "listing_gain_pct" in df.columns:
        print(f"\n-- listing_gain_pct sanity check --")
        print(df["listing_gain_pct"].describe().round(2).to_string())

    if "promoter_holding_post" in df.columns:
        print(f"\n── promoter_holding_post sanity check ──")
        print(df["promoter_holding_post"].describe().round(2).to_string())
        n100 = (df["promoter_holding_post"] == 100.0).sum()
        print(f"  Values = 100%: {n100} (should be 0)")

    if "anchor_pct_of_qib" in df.columns:
        print(f"\n── anchor_pct_of_qib sanity check ──")
        desc = df["anchor_pct_of_qib"].dropna()
        if not desc.empty:
            print(f"  Range: {desc.min():.1f}% – {desc.max():.1f}%")
            print(f"  Values > 100: {(desc > 100).sum()} (should be 0)")

    if "sector" in df.columns:
        print(f"\n── Sector distribution ──")
        print(df["sector"].value_counts().head(15).to_string())

    if "gmp_data_available" in df.columns:
        n_gmp = (df["gmp_data_available"] == 1).sum()
        print(f"\n── GMP data available: {n_gmp}/{len(df)} IPOs ──")

    print(f"\n── Columns ({len(df.columns)}) ──")
    print(", ".join(sorted(df.columns)))


if __name__ == "__main__":
    main()

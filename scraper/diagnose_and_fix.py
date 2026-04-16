"""
diagnose_and_fix.py
════════════════════
Run this FIRST before the full scraper.

What it does:
  1. Opens the Chittorgarh perf tracker for 2023
  2. Prints the EXACT table structure (headers, sample rows)
  3. Scrapes 3 IPOs fully and shows you exactly what's captured
  4. Saves a verified sample CSV so you can confirm data looks right

This tells us the exact column indices for listing_date, listing_price,
listing_gain_pct — which vary by year on Chittorgarh.
Run this, share the output, then we fix the main scraper accordingly.

Usage:
    python diagnose_and_fix.py
"""

import re
import time
import random
import tempfile
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

BASE       = "https://www.chittorgarh.com"
TEST_YEAR  = 2023
MAX_TEST_IPOS = 3   # only scrape 3 IPOs for diagnosis

# ─────────────────────────────────────────────
# DRIVER
# ─────────────────────────────────────────────

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,800")
    options.add_argument("--disable-blink-features=AutomationControlled")
    tmp_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={tmp_dir}")
    options.add_argument("--renderer-process-limit=1")
    options.add_argument("--js-flags=--max-old-space-size=512")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def get_page(url, driver, need_table=True):
    driver.get(url)
    time.sleep(random.uniform(3, 5))
    if need_table:
        try:
            WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except TimeoutException:
            pass
    else:
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            pass
    return BeautifulSoup(driver.page_source, "lxml")


# ─────────────────────────────────────────────
# STEP 1 — DIAGNOSE PERF TRACKER TABLE STRUCTURE
# ─────────────────────────────────────────────

def diagnose_perf_tracker(driver):
    url = f"{BASE}/ipo/ipo_perf_tracker.asp?year={TEST_YEAR}"
    log.info(f"Fetching perf tracker: {url}")
    soup = get_page(url, driver)

    print("\n" + "═"*70)
    print(f"STEP 1 — PERF TRACKER TABLE STRUCTURE (year={TEST_YEAR})")
    print("═"*70)

    tables = soup.find_all("table")
    print(f"Total tables on page: {len(tables)}\n")

    ipos = []

    for t_idx, table in enumerate(tables):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        rows    = table.find_all("tr")
        data_rows = [r for r in rows if r.find("td")]

        if not data_rows:
            continue

        print(f"Table #{t_idx}:")
        print(f"  Headers ({len(headers)}): {headers}")

        # Print first 3 data rows raw
        for i, row in enumerate(data_rows[:3]):
            cols = [c.get_text(" ", strip=True) for c in row.find_all("td")]
            print(f"  Row {i}: {cols}")

            # Try to extract slug/id
            for a in row.find_all("a"):
                href = a.get("href", "")
                m = re.search(r"/ipo/([^/]+)/(\d+)/", href)
                if m and i == 0:  # only from first row for brevity
                    print(f"  → Link found: slug={m.group(1)}, id={m.group(2)}")

        # Collect all IPO links from this table
        for row in data_rows:
            name_raw = row.find("td")
            if not name_raw:
                continue
            name = re.sub(r"IPO\s*Detail.*", "", name_raw.get_text(" ", strip=True), flags=re.I).strip()
            for a in row.find_all("a"):
                href = a.get("href", "")
                m = re.search(r"/ipo/([^/]+)/(\d+)/", href)
                if m:
                    all_cols = [c.get_text(strip=True) for c in row.find_all("td")]
                    ipos.append({
                        "name":    name,
                        "slug":    m.group(1),
                        "id":      m.group(2),
                        "raw_cols": all_cols,
                        "headers": headers,
                    })
                    break
        print()

    print(f"Total IPOs found in perf tracker: {len(ipos)}")
    return ipos


# ─────────────────────────────────────────────
# STEP 2 — DIAGNOSE DETAIL PAGE STRUCTURE
# ─────────────────────────────────────────────

def diagnose_detail_page(slug, ipo_id, name, driver):
    url = f"{BASE}/ipo/{slug}/{ipo_id}/"
    log.info(f"  Fetching detail page: {url}")
    soup = get_page(url, driver)

    print(f"\n{'─'*60}")
    print(f"DETAIL PAGE: {name}")
    print(f"URL: {url}")
    print(f"{'─'*60}")

    tables = soup.find_all("table")
    print(f"Tables on page: {len(tables)}")

    # Print every key-value pair found in all tables
    found = {}
    for t_idx, table in enumerate(tables):
        for row in table.find_all("tr"):
            cols = [c.get_text(" ", strip=True) for c in row.find_all("td")]
            if len(cols) >= 2:
                k = cols[0].strip()
                v = cols[1].strip()
                if k and v and k not in found:
                    found[k] = v

    print(f"\nAll key-value pairs found ({len(found)}):")
    for k, v in found.items():
        print(f"  [{k}] = [{v}]")

    return found


# ─────────────────────────────────────────────
# STEP 3 — DIAGNOSE SUBSCRIPTION PAGE
# ─────────────────────────────────────────────

def diagnose_subscription_page(slug, ipo_id, name, driver):
    url = f"{BASE}/ipo_subscription/{slug}/{ipo_id}/"
    log.info(f"  Fetching subscription page: {url}")
    soup = get_page(url, driver, need_table=True)

    print(f"\nSUBSCRIPTION PAGE: {name}")
    tables = soup.find_all("table")
    print(f"Tables: {len(tables)}")

    for t_idx, table in enumerate(tables):
        rows = table.find_all("tr")
        print(f"\n  Table #{t_idx} ({len(rows)} rows):")
        for row in rows[:8]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if cols:
                print(f"    {cols}")


# ─────────────────────────────────────────────
# STEP 4 — DIAGNOSE GMP PAGE
# ─────────────────────────────────────────────

def diagnose_gmp_page(slug, ipo_id, name, driver):
    url = f"{BASE}/ipo_gmp/{slug}/{ipo_id}/"
    log.info(f"  Fetching GMP page: {url}")
    soup = get_page(url, driver, need_table=False)

    print(f"\nGMP PAGE: {name}")
    tables = soup.find_all("table")
    print(f"Tables: {len(tables)}")
    if tables:
        for row in tables[0].find_all("tr")[:6]:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if cols:
                print(f"  {cols}")
    else:
        # Show raw text to understand what's on the page
        print(f"  Page text preview: {soup.get_text()[:300]}")


# ─────────────────────────────────────────────
# STEP 5 — SMART COLUMN MAPPER
# Based on what we find, map columns correctly
# ─────────────────────────────────────────────

def smart_map_columns(headers, raw_cols):
    """
    Maps raw column values to semantic fields based on
    actual header text found on the page.
    Prints the mapping so you can verify it's correct.
    """
    print(f"\nSMART COLUMN MAPPING:")
    print(f"  Headers: {headers}")
    print(f"  Values:  {raw_cols}")

    mapping = {}
    for i, h in enumerate(headers):
        h_lower = h.lower()
        if i >= len(raw_cols):
            break
        val = raw_cols[i]

        if any(x in h_lower for x in ["company", "ipo name", "name"]):
            mapping["company_name"] = val
        elif any(x in h_lower for x in ["issue price", "price band", "issue_price"]):
            mapping["issue_price_raw"] = val
        elif any(x in h_lower for x in ["listing price", "listing_price", "open price"]):
            mapping["listing_price_raw"] = val
        elif any(x in h_lower for x in ["listing gain", "gain", "return", "%", "listing%"]):
            mapping["listing_gain_raw"] = val
        elif any(x in h_lower for x in ["listing date", "date", "listed on"]):
            mapping["listing_date_raw"] = val
        elif any(x in h_lower for x in ["issue size", "size"]):
            mapping["issue_size_raw"] = val
        elif any(x in h_lower for x in ["subscription", "subscr"]):
            mapping["subscription_raw"] = val
        elif any(x in h_lower for x in ["status", "current", "category"]):
            mapping["status_raw"] = val
        else:
            mapping[f"col_{i}_{h[:15]}"] = val

    for k, v in mapping.items():
        print(f"  {k} → '{v}'")

    return mapping


# ─────────────────────────────────────────────
# STEP 6 — FIX LISTING GAIN (basis points → %)
# ─────────────────────────────────────────────

def fix_listing_gain(val_str):
    """
    Chittorgarh shows listing gain as e.g. '23.45%' or '23.45' or '2345'
    The old scraper was stripping % and getting 2345 (basis points).
    This fixes it.
    """
    if not val_str:
        return None
    # Remove % sign and spaces
    cleaned = str(val_str).replace("%", "").replace(" ", "").replace(",", "")
    try:
        val = float(cleaned)
        # If value is suspiciously large (>500 or <-200), it's likely basis points
        if abs(val) > 500:
            val = round(val / 100, 2)
        return val
    except:
        return None


# ─────────────────────────────────────────────
# MAIN DIAGNOSIS
# ─────────────────────────────────────────────

def main():
    print("\n" + "═"*70)
    print("IPO SCRAPER DIAGNOSTICS")
    print("This will show you EXACTLY what Chittorgarh's pages look like")
    print("so we can fix the column mapping before the full 2010-2024 run")
    print("═"*70 + "\n")

    driver = setup_driver()

    try:
        # ── Step 1: Perf tracker structure ──────────────────────────
        ipos = diagnose_perf_tracker(driver)

        if not ipos:
            print("\n❌ No IPOs found in perf tracker. Check if the site is accessible.")
            return

        print(f"\n✓ Found {len(ipos)} IPOs")

        # Show column mapping for first IPO
        if ipos:
            first = ipos[0]
            smart_map_columns(first["headers"], first["raw_cols"])

        # ── Step 2-4: Detail, subscription, GMP for first 3 IPOs ────
        print(f"\n{'═'*70}")
        print(f"STEP 2-4 — DETAIL PAGES FOR FIRST {MAX_TEST_IPOS} IPOs")
        print("═"*70)

        sample_data = []
        for ipo in ipos[:MAX_TEST_IPOS]:
            slug = ipo["slug"]
            ipo_id = ipo["id"]
            name = ipo["name"]

            print(f"\n{'█'*60}")
            print(f"IPO: {name}")

            # Detail page
            detail_kv = diagnose_detail_page(slug, ipo_id, name, driver)

            # Subscription page
            diagnose_subscription_page(slug, ipo_id, name, driver)

            # GMP page
            diagnose_gmp_page(slug, ipo_id, name, driver)

            # Build sample row with what we found
            row = {
                "name": name,
                "slug": slug,
                "id":   ipo_id,
            }

            # Try to map detail page keys
            KEY_MAP = {
                # What Chittorgarh shows → what we want to store
                "Issue Price":          "offer_price",
                "Issue Size":           "issue_size_cr",
                "Fresh Issue":          "fresh_issue_cr",
                "Offer for Sale":       "ofs_cr",
                "Face Value":           "face_value",
                "Lot Size":             "lot_size",
                "Listing Date":         "listing_date",
                "Listing At":           "exchange",
                "IPO Price":            "offer_price",
                "Price Band":           "price_band",
                "Category":             "sector",
                "Sector":               "sector",
                "ROE":                  "roe",
                "ROCE":                 "roce",
                "P/E Ratio":            "pe_ratio",
                "P/B Ratio":            "pb_ratio",
                "Market Cap":           "market_cap_cr",
                "Promoter Holding":     "promoter_holding",
                "EV/EBITDA":            "ev_ebitda",
            }

            for page_key, field in KEY_MAP.items():
                # Fuzzy match — Chittorgarh key names vary slightly
                for found_key, found_val in detail_kv.items():
                    if page_key.lower() in found_key.lower():
                        row[field] = found_val
                        break

            sample_data.append(row)
            time.sleep(random.uniform(2, 4))

        # ── Step 5: Save sample ──────────────────────────────────────
        if sample_data:
            df = pd.DataFrame(sample_data)
            df.to_csv("diagnosis_sample.csv", index=False)
            print(f"\n\n{'═'*70}")
            print("✓ Sample saved to diagnosis_sample.csv")
            print(f"\nSample DataFrame:")
            print(df.to_string())

        # ── Step 6: Summary of what needs fixing ─────────────────────
        print(f"\n\n{'═'*70}")
        print("DIAGNOSIS SUMMARY — THINGS TO CHECK")
        print("═"*70)
        print("""
1. Check 'listing_gain_raw' in the column mapping above.
   - If it looks like '23.45%'  → scraper needs to strip % and keep decimal
   - If it looks like '2345'    → scraper is treating basis points as %
   - If it's empty/missing      → column index is wrong

2. Check 'listing_date_raw' in the column mapping above.
   - If empty → the date column header doesn't match 'date'
   - Note the EXACT header text and tell me

3. Check the DETAIL PAGE key-value pairs.
   - Note which keys are present for financials (revenue, PAT)
   - Note if they appear as 'Total Income' or 'Revenue from Operations'

4. Check GMP page.
   - If 'Tables: 0' → the page exists but has no table (expected for some IPOs)
   - If 'Page text preview' shows 'data not available' → confirmed no data

Share this full output and I'll fix the exact column mappings.
        """)

    finally:
        driver.quit()
        print("\nDriver closed.")


if __name__ == "__main__":
    main()

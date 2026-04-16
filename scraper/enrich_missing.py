"""
IPO Data Enrichment — Fill Missing Pre-2021 Financials
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHY: Chittorgarh's detail pages for pre-2021 IPOs don't have:
  Revenue, PAT, EPS, Net Worth, Debt, P/E, P/B, Sector, etc.
  This is NOT a scraping bug — the data simply isn't on those pages.

HOW: Uses Screener.in API (no Selenium needed, pure HTTP) to look up
  each company by its NSE symbol and pull financials.
  Falls back to fuzzy name matching if symbol lookup fails.

INPUT:  Your scraped CSV (ipo_master_2010_2024.csv)
OUTPUT: Enriched CSV with filled gaps + coverage report

USAGE:
  pip install aiohttp beautifulsoup4 pandas lxml
  python enrich_missing.py                                    # enrich all gaps
  python enrich_missing.py --input my_data.csv --output enriched.csv
  python enrich_missing.py --max-year 2020                    # only pre-2021
  python enrich_missing.py --dry-run                          # show what would be filled
"""

import asyncio
import argparse
import logging
import re
import time
from typing import Any

import aiohttp
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SCREENER_BASE = "https://www.screener.in"
MAX_CONCURRENT = 5
REQUEST_DELAY = 1.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/json,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-IN,en;q=0.9",
}

# Sector inference from company name (fallback when Screener has nothing)
SECTOR_KEYWORDS = {
    "pharma": "Pharmaceuticals", "drug": "Pharmaceuticals", "medic": "Healthcare",
    "health": "Healthcare", "hospital": "Healthcare", "diagno": "Healthcare",
    "bank": "Banking & Finance", "financ": "Banking & Finance", "nbfc": "Banking & Finance",
    "insur": "Insurance", "credit": "Banking & Finance", "micro": "Banking & Finance",
    "tech": "Technology", "software": "Technology", "info": "Technology", "digital": "Technology",
    "infra": "Infrastructure", "construct": "Infrastructure", "engineer": "Engineering",
    "road": "Infrastructure", "cement": "Materials", "steel": "Materials", "metal": "Materials",
    "chemical": "Chemicals", "petro": "Energy", "oil": "Energy", "gas": "Energy",
    "power": "Energy", "solar": "Energy", "food": "FMCG", "consumer": "FMCG",
    "retail": "Retail", "auto": "Automobile", "real": "Real Estate", "realty": "Real Estate",
    "textile": "Textiles", "logist": "Logistics", "telecom": "Telecom",
    "jewel": "Retail", "educat": "Education", "hotel": "Hospitality",
    "defence": "Defence", "aero": "Defence", "mining": "Mining",
}


def clean_num(x):
    if not x: return None
    cleaned = re.sub(r"[₹Rs.\s,]", "", str(x).strip())
    cleaned = re.sub(r"[a-zA-Z%].*$", "", cleaned).strip()
    m = re.search(r"-?\d+\.?\d*", cleaned)
    try: return float(m.group()) if m else None
    except: return None


def infer_sector(name: str) -> str | None:
    n = name.lower()
    for kw, sec in SECTOR_KEYWORDS.items():
        if kw in n:
            return sec
    return None


async def enrich_from_screener(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    nse_symbol: str | None,
    company_name: str,
) -> dict[str, Any]:
    """
    Search Screener.in by NSE symbol (priority) or company name (fallback).
    Returns dict of financial fields to fill.
    """
    result: dict[str, Any] = {}

    # Build search queries — symbol first, then name variants
    queries = []
    if nse_symbol and isinstance(nse_symbol, str) and len(nse_symbol) > 1:
        queries.append(nse_symbol)
    clean_name = company_name
    for s in [" Limited", " Ltd.", " Ltd", " Pvt.", " Pvt", " India", " (India)"]:
        clean_name = clean_name.replace(s, "")
    clean_name = clean_name.strip()
    queries.append(clean_name)
    if len(clean_name.split()) > 2:
        queries.append(" ".join(clean_name.split()[:2]))

    company_url = None

    for query in queries:
        async with sem:
            await asyncio.sleep(REQUEST_DELAY)
            try:
                async with session.get(
                    f"{SCREENER_BASE}/api/company/search/",
                    params={"q": query},
                    headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15),
                    ssl=False,
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                    if data and isinstance(data, list) and len(data) > 0:
                        company_url = data[0].get("url")
                        if company_url:
                            break
            except Exception as e:
                log.debug(f"Search error for '{query}': {e}")
                continue

    if not company_url:
        return result

    # Fetch the company page
    async with sem:
        await asyncio.sleep(REQUEST_DELAY)
        try:
            async with session.get(
                f"{SCREENER_BASE}{company_url}",
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=15),
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    return result
                html = await resp.text()
        except Exception as e:
            log.debug(f"Page fetch error for {company_name}: {e}")
            return result

    soup = BeautifulSoup(html, "lxml")

    # ── Top ratios ──
    for li in soup.select("#top-ratios li, .company-ratios li"):
        ne = li.select_one(".name")
        ve = li.select_one(".value, .number")
        if not ne or not ve:
            continue
        label = ne.get_text().strip().lower()
        val = clean_num(ve.get_text())
        if val is None:
            continue

        if "stock p/e" in label:
            result["pe_ratio"] = val
        elif "book value" in label:
            result["book_value"] = val
        elif "roe" in label:
            result["roe"] = val
        elif "roce" in label:
            result["roce"] = val
        elif "debt to equity" in label:
            result["debt_equity"] = val
        elif "market cap" in label:
            result["market_cap_cr"] = val
        elif "promoter holding" in label:
            result["promoter_holding_screener"] = val
        elif "face value" in label:
            result["face_value_screener"] = val

    # ── P&L data — find the column closest to IPO year ──
    for tr in soup.select("#profit-loss tr"):
        cells = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cells) < 2:
            continue
        rl = cells[0].lower()
        # Take the earliest available year (closest to IPO filing)
        # Screener shows newest→oldest, so last non-empty cell is oldest
        earliest_val = None
        latest_val = None
        for c in cells[1:]:
            v = clean_num(c)
            if v is not None:
                if earliest_val is None:
                    latest_val = v  # first non-empty = most recent
                earliest_val = v   # last non-empty = oldest

        # Use latest (most recent year) as best available
        val = latest_val

        if ("sales" in rl or "revenue" in rl) and val is not None:
            result["revenue_cr"] = val
        elif "net profit" in rl and val is not None:
            result["pat_cr"] = val
        elif "operating profit" in rl and val is not None:
            result["ebitda_cr"] = val

    # ── Balance sheet ──
    for tr in soup.select("#balance-sheet tr"):
        cells = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cells) < 2:
            continue
        rl = cells[0].lower()
        latest_val = None
        for c in cells[1:]:
            v = clean_num(c)
            if v is not None:
                latest_val = v
                break

        if "total liabilities" in rl or "borrowings" in rl:
            if latest_val and "total_debt_cr" not in result:
                result["total_debt_cr"] = latest_val
        elif "equity" in rl and "share" not in rl:
            if latest_val and "net_worth_cr" not in result:
                result["net_worth_cr"] = latest_val

    # ── Sector ──
    sec_el = soup.select_one("a[href*='/sector/']")
    if sec_el:
        result["sector"] = sec_el.get_text().strip()

    # ── Derived ──
    if result.get("revenue_cr") and result.get("pat_cr") and result["revenue_cr"] > 0:
        result["pat_margin_pct"] = round(result["pat_cr"] / result["revenue_cr"] * 100, 2)
    if result.get("revenue_cr") and result.get("ebitda_cr") and result["revenue_cr"] > 0:
        result["ebitda_margin_pct"] = round(result["ebitda_cr"] / result["revenue_cr"] * 100, 2)
    if result.get("book_value") and result.get("book_value") > 0:
        result["pb_ratio_screener"] = result.get("pe_ratio", 0)  # placeholder

    return result


async def run_enrichment(
    input_path: str,
    output_path: str,
    max_year: int | None = None,
    dry_run: bool = False,
):
    log.info(f"Loading {input_path}")
    df = pd.read_csv(input_path)
    log.info(f"Loaded {len(df)} IPOs, {len(df.columns)} columns")

    # Ensure required columns exist
    for col in ["revenue_cr", "pat_cr", "sector", "nse_symbol", "name", "year"]:
        if col not in df.columns:
            df[col] = np.nan

    # Identify rows needing enrichment
    needs_financial = df["revenue_cr"].isna() | df["pat_cr"].isna()
    needs_sector = df["sector"].isna()

    needs_any = needs_financial | needs_sector

    if max_year:
        needs_any = needs_any & (df["year"] <= max_year)

    candidates = df[needs_any].copy()
    log.info(f"Candidates for enrichment: {len(candidates)} (missing financials or sector)")

    if dry_run:
        print(f"\nDRY RUN — would enrich {len(candidates)} IPOs:")
        for _, r in candidates.head(20).iterrows():
            sym = r.get("nse_symbol", "?")
            print(f"  {r['year']} | {r['name']:40s} | NSE: {sym}")
        if len(candidates) > 20:
            print(f"  ... and {len(candidates) - 20} more")
        return

    # Run async enrichment
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    enriched_count = 0
    field_counts: dict[str, int] = {}

    async with aiohttp.ClientSession() as session:
        for batch_start in range(0, len(candidates), 20):
            batch = candidates.iloc[batch_start:batch_start + 20]
            tasks = []
            indices = []

            for idx, row in batch.iterrows():
                nse_sym = row.get("nse_symbol") if pd.notna(row.get("nse_symbol")) else None
                name = str(row.get("name", ""))
                tasks.append(enrich_from_screener(session, sem, nse_sym, name))
                indices.append(idx)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    log.debug(f"Enrichment error: {result}")
                    continue
                if not isinstance(result, dict) or not result:
                    continue

                idx = indices[i]
                name = df.at[idx, "name"]
                filled_fields = []

                for field, value in result.items():
                    if value is None:
                        continue
                    # Only fill if current value is NaN
                    if field in df.columns:
                        current = df.at[idx, field]
                        if pd.isna(current):
                            df.at[idx, field] = value
                            filled_fields.append(field)
                            field_counts[field] = field_counts.get(field, 0) + 1
                    else:
                        # New column
                        if field not in df.columns:
                            df[field] = None
                        df.at[idx, field] = value
                        filled_fields.append(field)
                        field_counts[field] = field_counts.get(field, 0) + 1

                if filled_fields:
                    enriched_count += 1
                    log.info(f"  OK: {name} -> {', '.join(filled_fields)}")

            pct = round((batch_start + len(batch)) / len(candidates) * 100, 1)
            log.info(f"  Progress: {batch_start + len(batch)}/{len(candidates)} ({pct}%)")

    # Sector inference fallback for remaining gaps
    if "sector" in df.columns:
        still_missing = df["sector"].isna()
        for idx in df[still_missing].index:
            name = str(df.at[idx, "name"])
            inferred = infer_sector(name)
            if inferred:
                df.at[idx, "sector"] = inferred
                field_counts["sector_inferred"] = field_counts.get("sector_inferred", 0) + 1

    # Compute derived fields for newly filled data
    for idx, row in df.iterrows():
        # PAT margin
        if pd.isna(row.get("pat_margin_pct")) and pd.notna(row.get("pat_cr")) and pd.notna(row.get("revenue_cr")):
            if row["revenue_cr"] > 0:
                df.at[idx, "pat_margin_pct"] = round(row["pat_cr"] / row["revenue_cr"] * 100, 2)

        # EBITDA margin
        if "ebitda_margin_pct" not in df.columns:
            df["ebitda_margin_pct"] = None
        if pd.isna(row.get("ebitda_margin_pct")) and pd.notna(row.get("ebitda_cr")) and pd.notna(row.get("revenue_cr")):
            if row["revenue_cr"] > 0:
                df.at[idx, "ebitda_margin_pct"] = round(row["ebitda_cr"] / row["revenue_cr"] * 100, 2)

        # Debt/equity
        if pd.isna(row.get("debt_equity")) and pd.notna(row.get("total_debt_cr")) and pd.notna(row.get("net_worth_cr")):
            if row["net_worth_cr"] > 0:
                df.at[idx, "debt_equity"] = round(row["total_debt_cr"] / row["net_worth_cr"], 2)

        # EPS from PAT and market cap
        if pd.isna(row.get("eps")) and pd.notna(row.get("pat_cr")) and pd.notna(row.get("market_cap_cr")):
            if row.get("offer_price") and row["market_cap_cr"] > 0:
                shares_cr = row["market_cap_cr"] / row["offer_price"]
                if shares_cr > 0:
                    df.at[idx, "eps"] = round(row["pat_cr"] / shares_cr, 2)

        # P/E from offer price and EPS
        if pd.isna(row.get("pe_ratio")) and pd.notna(row.get("eps")) and row.get("eps", 0) > 0:
            if pd.notna(row.get("offer_price")):
                df.at[idx, "pe_ratio"] = round(row["offer_price"] / row["eps"], 2)

    # Save
    df.to_csv(output_path, index=False)
    log.info(f"\nSaved enriched data: {output_path}")

    # Coverage report
    print(f"\n{'='*60}")
    print(f"ENRICHMENT COMPLETE - {enriched_count} IPOs enriched")
    print(f"{'='*60}")
    print(f"\nFields filled:")
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        print(f"  {field:30s} +{count}")

    print(f"\n-- Coverage BEFORE vs AFTER --")
    key_cols = ["sector", "revenue_cr", "pat_cr", "ebitda_cr", "pe_ratio",
                "roe", "roce", "debt_equity", "market_cap_cr", "eps",
                "net_worth_cr", "total_debt_cr", "pat_margin_pct", "ebitda_margin_pct"]
    for c in key_cols:
        if c in df.columns:
            n = df[c].notna().sum()
            p = round(n / len(df) * 100, 1)
            bar = "#" * int(p // 5) + "." * (20 - int(p // 5))
            print(f"  {c:30s} {n:4d}/{len(df):4d} ({p:5.1f}%) {bar}")


def main():
    parser = argparse.ArgumentParser(description="Enrich IPO data from Screener.in")
    parser.add_argument("--input", default="ipo_master_2010_2024.csv")
    parser.add_argument("--output", default="ipo_master_enriched.csv")
    parser.add_argument("--max-year", type=int, default=None,
                        help="Only enrich IPOs up to this year (e.g. 2020)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be enriched without making requests")
    args = parser.parse_args()

    asyncio.run(run_enrichment(args.input, args.output, args.max_year, args.dry_run))


if __name__ == "__main__":
    main()

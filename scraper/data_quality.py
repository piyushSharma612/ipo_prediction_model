"""
data_quality.py
═══════════════
Missing Data Handler for IPO Master Dataset

This module runs AFTER scrape_ipo_v2.py has built the base CSV.
It does three things:
  1. AUDIT     — tells you exactly which fields are missing and by how much
  2. FALLBACK  — tries to fill missing fields from secondary sources
  3. IMPUTE    — for fields that can't be fetched, applies the statistically
                 correct imputation strategy per field type

Why not just use df.fillna(mean)?
  Because mean imputation on financial data is wrong. P/E of a loss-making company
  is undefined — filling it with the mean P/E of profitable companies is misleading.
  GMP missing for a 2010 IPO means GMP tracking didn't exist yet — that's structurally
  different from a 2023 IPO with missing GMP.
  Each field needs its own strategy.
"""

import re
import time
import logging
import requests
import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from sklearn.experimental import enable_iterative_imputer   # noqa
from sklearn.impute import IterativeImputer, KNNImputer
from sklearn.ensemble import RandomForestRegressor

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

INPUT_CSV  = "ipo_master_2010_2025.csv"
OUTPUT_CSV = "ipo_master_clean.csv"
AUDIT_CSV  = "missing_data_audit.csv"

# ─────────────────────────────────────────────────────────────────────
# SECTION 1 — FIELD CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────
#
# Every field in the dataset falls into one of 4 categories:
#
#   CRITICAL    — model cannot run without these. If missing, try every
#                 fallback. If still missing, DROP the IPO row entirely.
#
#   DERIVABLE   — can be computed from other columns in the same row.
#                 No scraping needed.
#
#   FETCHABLE   — can be retrieved from a secondary source (yfinance,
#                 Screener, NSE). Worth the API call.
#
#   IMPUTABLE   — no fallback exists. Apply statistical imputation.
#                 Strategy varies by field (explained below).
#
#   STRUCTURAL  — expected to be missing for certain IPO cohorts.
#                 e.g., GMP data didn't exist pre-2015. Flag with a
#                 binary indicator column, then fill with 0.

FIELD_TAXONOMY = {

    # ── CRITICAL ────────────────────────────────────────────────────
    "critical": [
        "offer_price",          # Without this, can't compute any valuation
        "listing_gain_pct",     # Core target variable (short-term)
        "issue_size_cr",        # Fundamental deal structure
        "year",                 # Temporal feature
        "name",                 # Identity
    ],

    # ── DERIVABLE (compute from existing columns) ────────────────────
    "derivable": {
        # If issue_size and ofs are known, fresh = issue - ofs
        "fresh_issue_cr":       ("issue_size_cr", "ofs_cr",
                                 lambda sz, ofs: sz - ofs if pd.notna(sz) and pd.notna(ofs) else sz),

        # OFS % from absolute values
        "ofs_pct":              ("ofs_cr", "issue_size_cr",
                                 lambda ofs, sz: round(ofs / sz * 100, 2) if pd.notna(ofs) and sz else None),

        # PAT margin (only if not already scraped)
        "pat_margin_pct":       ("pat_cr", "revenue_cr",
                                 lambda p, r: round(p/r*100, 2) if pd.notna(p) and pd.notna(r) and r > 0 else None),

        # QIB/Retail divergence signal
        "qib_retail_ratio":     ("qib_sub", "rii_sub",
                                 lambda q, r: round(q/r, 3) if pd.notna(q) and pd.notna(r) and r > 0 else None),

        # v6: Promoter dilution = holding_pre - holding_post
        "promoter_dilution_pct": ("promoter_holding_pre", "promoter_holding_post",
                                  lambda pre, post: round(pre - post, 2) if pd.notna(pre) and pd.notna(post) else None),

        # v6: IPO days open = close - open + 1
        "ipo_days_open":        ("ipo_open_date", "ipo_close_date",
                                 lambda o, c: None),  # handled specially in fill_derivable_v6
    },

    # ── FETCHABLE (secondary source APIs) ───────────────────────────
    "fetchable": [
        "return_1M", "return_3M", "return_6M", "return_1Y",  # yfinance
        "alpha_1M",  "alpha_3M",  "alpha_6M",  "alpha_1Y",   # yfinance vs NIFTY
        "nifty_at_listing",                                    # yfinance
        "nifty_30d_return_pct",                                # yfinance
        "vix_at_listing",                                      # yfinance
        "eps_ttm",                                             # Screener
        "revenue_latest_qtr",                                  # Screener
    ],

    # ── STRUCTURAL MISSINGNESS (expected, flag + fill with 0) ───────
    # GMP tracking became common only ~2018. Earlier IPOs won't have it.
    # Anchor investor allotment rule was introduced by SEBI in Oct 2009,
    # so pre-2010 IPOs won't have it (we start 2010 so edge case only).
    "structural": [
        "gmp_peak", "gmp_min", "gmp_listing_day",
        "gmp_trend", "gmp_std", "gmp_entries",
        "anchor_amount_cr", "anchor_investors_count",
        "anchor_pct_of_qib", "has_top_anchor",
    ],

    # ── IMPUTABLE (statistical fill) ────────────────────────────────
    # These can't be fetched but are needed for the model.
    # Each has its own strategy — see impute_fields() comments below.
    "imputable": [
        "roe", "roce", "pe_ratio", "pb_ratio",
        "ev_ebitda", "promoter_holding_pre", "promoter_holding_post",
        "qib_sub", "hni_sub", "rii_sub", "total_sub",
        "market_cap_cr", "market_cap_pre_ipo_cr",
    ],

    # ── METADATA (not imputed, just tracked) ────────────────────────
    "metadata": [
        "lead_manager", "registrar", "sector",
        "ipo_open_date", "ipo_close_date",
        "shares_offered", "net_offered_to_public",
    ],
}


# ─────────────────────────────────────────────────────────────────────
# SECTION 2 — AUDIT
# ─────────────────────────────────────────────────────────────────────

def audit_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates a detailed missing data report per field.
    Shows: total missing, % missing, which years are most affected,
    and recommended strategy.
    
    Run this first to understand your data quality before touching anything.
    """
    records = []
    for col in df.columns:
        n_missing = df[col].isna().sum()
        pct_missing = round(n_missing / len(df) * 100, 1)

        # Find which years have the most missingness for this field
        if "year" in df.columns and n_missing > 0:
            worst_years = (
                df[df[col].isna()]["year"]
                .value_counts()
                .head(3)
                .index.tolist()
            )
        else:
            worst_years = []

        # Classify field
        category = "unknown"
        for cat, fields in FIELD_TAXONOMY.items():
            if cat == "derivable":
                if col in fields:
                    category = "derivable"
            else:
                if col in fields:
                    category = cat
                    break

        # Recommend strategy
        if pct_missing == 0:
            strategy = "✓ complete"
        elif category == "critical":
            strategy = "⚠ DROP ROW if missing"
        elif category == "derivable":
            strategy = "→ compute from other columns"
        elif category == "fetchable":
            strategy = "↓ fetch from secondary source"
        elif category == "structural":
            strategy = "~ add indicator flag + fill 0"
        elif category == "imputable":
            if pct_missing > 60:
                strategy = "✗ too sparse — drop column"
            elif pct_missing > 30:
                strategy = "→ median by sector+year"
            else:
                strategy = "→ KNN / MICE imputation"
        else:
            strategy = "? review manually"

        records.append({
            "field":        col,
            "n_missing":    n_missing,
            "pct_missing":  pct_missing,
            "category":     category,
            "worst_years":  str(worst_years),
            "strategy":     strategy,
        })

    audit_df = pd.DataFrame(records).sort_values("pct_missing", ascending=False)
    audit_df.to_csv(AUDIT_CSV, index=False)
    log.info(f"Audit saved to {AUDIT_CSV}")

    # Print summary to console
    print("\n" + "═"*70)
    print("MISSING DATA AUDIT")
    print("═"*70)
    print(f"Total rows: {len(df)} | Total columns: {len(df.columns)}")
    print(f"Rows with ANY missing value: {df.isnull().any(axis=1).sum()} "
          f"({df.isnull().any(axis=1).mean()*100:.1f}%)")
    print("\nTop 15 fields by missingness:")
    print(audit_df[["field","pct_missing","category","strategy"]].head(15).to_string(index=False))
    print("═"*70 + "\n")

    return audit_df


# ─────────────────────────────────────────────────────────────────────
# SECTION 3 — DERIVABLE FIELDS
# ─────────────────────────────────────────────────────────────────────

def fill_derivable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills fields that can be computed from existing columns.
    No external calls needed. Run this before any imputation.
    """
    log.info("Filling derivable fields...")
    filled = 0

    for col, (src1, src2, fn) in FIELD_TAXONOMY["derivable"].items():
        if col not in df.columns:
            df[col] = None
        mask = df[col].isna() & df[src1].notna()
        if src2 in df.columns:
            mask = mask & df[src2].notna()
            df.loc[mask, col] = df.loc[mask].apply(
                lambda r: fn(r[src1], r[src2]), axis=1
            )
        else:
            df.loc[mask, col] = df.loc[mask, src1].apply(lambda v: fn(v, None))
        n = mask.sum()
        if n > 0:
            log.info(f"  {col}: filled {n} rows from derivation")
            filled += n

    log.info(f"Derivable fill complete: {filled} total cells filled")
    return df


# ─────────────────────────────────────────────────────────────────────
# SECTION 4 — SECONDARY SOURCE FALLBACKS
# ─────────────────────────────────────────────────────────────────────

def fetch_screener_financials(nse_ticker: str) -> dict:
    """
    Fetches latest financials from Screener.in public page.
    Only called for IPOs missing key financial ratios.
    
    Screener's public pages (no login) have TTM EPS, current P/E,
    revenue/profit for last 4 quarters.
    
    Rate limit: add 2-3s sleep between calls.
    """
    if not nse_ticker:
        return {}
    
    url = f"https://www.screener.in/company/{nse_ticker}/consolidated/"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            # Try standalone (non-consolidated)
            url = f"https://www.screener.in/company/{nse_ticker}/"
            resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return {}

        soup = BeautifulSoup(resp.text, "lxml")
        data = {}

        # Screener shows key ratios in a #top-ratios div
        ratios_div = soup.find(id="top-ratios")
        if ratios_div:
            for li in ratios_div.find_all("li"):
                name  = li.find("span", class_="name")
                value = li.find("span", class_="value") or li.find("span", class_="number")
                if name and value:
                    k = name.get_text(strip=True).lower()
                    v = value.get_text(strip=True)
                    if "p/e" in k:
                        data["pe_ratio_screener"] = _safe_float(v)
                    elif "p/b" in k or "price/book" in k:
                        data["pb_ratio_screener"] = _safe_float(v)
                    elif "roe" in k:
                        data["roe_screener"] = _safe_float(v.replace("%",""))
                    elif "roce" in k:
                        data["roce_screener"] = _safe_float(v.replace("%",""))
                    elif "eps" in k:
                        data["eps_ttm"] = _safe_float(v)

        time.sleep(2.5)  # polite delay
        return data

    except Exception as e:
        log.warning(f"Screener fetch failed for {nse_ticker}: {e}")
        return {}


def fetch_missing_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    For rows missing yfinance returns, tries to re-fetch.
    Skips rows where listing_date is unparseable or ticker is unknown.
    Only refetches IPOs from before the data cutoff (can't have 1Y return for recent IPOs).
    """
    log.info("Fetching missing returns from yfinance...")
    
    return_cols = ["return_1M", "return_3M", "return_6M", "return_1Y"]
    missing_mask = df[return_cols].isnull().all(axis=1)
    missing_rows = df[missing_mask & df["year"].lt(2025)]
    
    log.info(f"  {len(missing_rows)} IPOs with missing returns")

    for idx, row in missing_rows.iterrows():
        ticker = row.get("nse_ticker")
        listing_date_str = str(row.get("listing_date_raw", row.get("listing_date", "")))
        
        if not ticker or not listing_date_str or listing_date_str == "nan":
            continue

        try:
            listing_date = _parse_date(listing_date_str)
            if not listing_date:
                continue

            end_date = listing_date + timedelta(days=400)
            hist = yf.Ticker(f"{ticker}.NS").history(
                start=listing_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
            )
            if hist.empty:
                continue

            base = hist["Close"].iloc[0]
            for label, td in [("1M",21),("3M",63),("6M",126),("1Y",252)]:
                if len(hist) >= td:
                    df.at[idx, f"return_{label}"] = round(
                        (hist["Close"].iloc[td] / base - 1) * 100, 2
                    )

            log.info(f"  Fetched returns for {row['name']}")
            time.sleep(0.5)

        except Exception as e:
            log.warning(f"  Returns fetch failed for {row.get('name')}: {e}")

    return df


def fill_from_screener(df: pd.DataFrame) -> pd.DataFrame:
    """
    For IPOs missing critical ratios (pe_ratio, pb_ratio, roe, roce),
    attempts to fetch from Screener.in.
    
    Only runs on IPOs that have an nse_ticker (needed to form Screener URL).
    Expensive (1 HTTP call per IPO) — only runs on rows with missing ratios.
    """
    ratio_cols = [c for c in ["pe_ratio", "pb_ratio", "roe", "roce"] if c in df.columns]
    needs_screener = df[ratio_cols].isnull().any(axis=1) & df["nse_ticker"].notna()
    targets = df[needs_screener]
    
    log.info(f"Fetching Screener data for {len(targets)} IPOs with missing ratios...")

    for idx, row in targets.iterrows():
        ticker = row["nse_ticker"]
        screener_data = fetch_screener_financials(ticker)
        
        if screener_data.get("pe_ratio_screener") and pd.isna(df.at[idx, "pe_ratio"]):
            df.at[idx, "pe_ratio"] = screener_data["pe_ratio_screener"]
        if screener_data.get("pb_ratio_screener") and pd.isna(df.at[idx, "pb_ratio"]):
            df.at[idx, "pb_ratio"] = screener_data["pb_ratio_screener"]
        if screener_data.get("roe_screener") and pd.isna(df.at[idx, "roe"]):
            df.at[idx, "roe"] = screener_data["roe_screener"]
        if screener_data.get("roce_screener") and pd.isna(df.at[idx, "roce"]):
            df.at[idx, "roce"] = screener_data["roce_screener"]

    return df


# ─────────────────────────────────────────────────────────────────────
# SECTION 5 — STRUCTURAL MISSINGNESS
# ─────────────────────────────────────────────────────────────────────

def handle_structural_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles fields that are EXPECTED to be missing for certain cohorts.
    
    Strategy: Add a binary indicator column _was_missing, then fill with 0.
    
    Why add the indicator?
    The ML model needs to know the difference between:
      - gmp_peak = 0   (GMP existed and was exactly zero — bearish signal)
      - gmp_peak = NaN (GMP data didn't exist in 2011 — no signal either way)
    Without the indicator, both become 0 and the model can't distinguish them.
    The indicator column lets XGBoost learn: "when gmp_was_missing=1, ignore gmp features"
    
    GMP tracking became mainstream ~2015. Pre-2015 missingness is structural.
    """
    log.info("Handling structural missingness...")

    GMP_CUTOFF_YEAR = 2015  # before this, GMP data is largely unavailable

    for col in FIELD_TAXONOMY["structural"]:
        if col not in df.columns:
            df[col] = np.nan

        # Add indicator BEFORE filling
        indicator_col = f"{col}_was_missing"
        df[indicator_col] = df[col].isna().astype(int)

        # Pre-2015 GMP: structural — fill 0, indicator=2 (structural, not just missing)
        if col.startswith("gmp") and "year" in df.columns:
            pre_cutoff = df["year"] < GMP_CUTOFF_YEAR
            df.loc[pre_cutoff & df[col].isna(), indicator_col] = 2  # 2 = structural gap
            df.loc[df[col].isna(), col] = 0

        else:
            df[col] = df[col].fillna(0)

    return df


# ─────────────────────────────────────────────────────────────────────
# SECTION 6 — STATISTICAL IMPUTATION
# ─────────────────────────────────────────────────────────────────────

def impute_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Statistical imputation for fields that can't be fetched or derived.
    
    Different strategies per field type — explained inline.
    """
    log.info("Applying statistical imputation...")

    # ── Strategy A: Median by SECTOR + YEAR ─────────────────────────
    # For valuation ratios (P/E, P/B, EV/EBITDA):
    # A tech company's P/E should be compared to other tech IPOs in the same year,
    # not the global dataset median. Sector+year grouping is much more accurate.
    #
    # Why median not mean? P/E ratios have extreme outliers (loss-making companies,
    # hypergrowth cos with P/E of 300x). Median is robust to these.
    
    sector_year_median_cols = ["pe_ratio", "pb_ratio", "ev_ebitda", "market_cap_cr"]
    
    for col in sector_year_median_cols:
        if col not in df.columns:
            continue
        n_before = df[col].isna().sum()
        
        # Fill with sector+year median
        df[col] = df.groupby(
            ["sector", "year"], group_keys=False
        )[col].apply(lambda x: x.fillna(x.median()))
        
        # If still missing (e.g., entire sector-year group is null), fall back to year median
        df[col] = df.groupby("year", group_keys=False)[col].apply(
            lambda x: x.fillna(x.median())
        )
        
        # Final fallback: global median
        df[col] = df[col].fillna(df[col].median())
        
        n_after = df[col].isna().sum()
        log.info(f"  {col}: sector+year median filled {n_before - n_after} rows")

    # ── Strategy B: Median by SECTOR only ───────────────────────────
    # For ROE, ROCE, promoter holding:
    # These are more structural to the sector than the year.
    # A manufacturing company's ROE doesn't change dramatically year to year
    # in terms of what's "normal" for that sector.
    
    sector_median_cols = ["roe", "roce", "promoter_holding_pre", "promoter_holding_post"]
    
    for col in sector_median_cols:
        if col not in df.columns:
            continue
        n_before = df[col].isna().sum()

        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df.groupby("sector", group_keys=False)[col].apply(
            lambda x: x.fillna(x.median())
        )
        df[col] = df[col].fillna(df[col].median())

        n_after = df[col].isna().sum()
        log.info(f"  {col}: sector median filled {n_before - n_after} rows")

    # ── Strategy C: Subscription rates — special case ───────────────
    # Missing subscription rates are different from missing P/E.
    # If an IPO has total_sub but missing qib_sub:
    #   → we can estimate qib_sub from typical QIB/Total ratio for that year
    # If ALL subscription is missing: the IPO might be SME (different exchange,
    #   different rules). Flag these separately.
    
    sub_cols = ["qib_sub", "hni_sub", "rii_sub", "total_sub"]
    for col in sub_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Rows with no subscription data at all
    all_sub_missing = df[sub_cols].isnull().all(axis=1)
    df["subscription_data_missing"] = all_sub_missing.astype(int)
    
    # For partial missing: fill from total using historical split ratios
    # Typical QIB:HNI:Retail allocation split is roughly 50%:15%:35% of total issue
    # but actual subscription can vary wildly — use year-wise medians
    for col in ["qib_sub", "hni_sub", "rii_sub"]:
        if col in df.columns:
            df[col] = df.groupby("year", group_keys=False)[col].apply(
                lambda x: x.fillna(x.median())
            )
            df[col] = df[col].fillna(df[col].median())

    if "total_sub" in df.columns:
        # Recompute total from parts where total is missing
        mask = df["total_sub"].isna() & df["qib_sub"].notna()
        if mask.any():
            df.loc[mask, "total_sub"] = (
                df.loc[mask, "qib_sub"] * 0.5 +
                df.loc[mask, "hni_sub"].fillna(0) * 0.15 +
                df.loc[mask, "rii_sub"].fillna(0) * 0.35
            )

    # ── Strategy D: MICE for correlated financial features ───────────
    # MICE = Multiple Imputation by Chained Equations
    # Works by treating each missing field as a regression problem,
    # using all other features as predictors. Iterates until convergence.
    # Best for fields that are correlated with each other.
    # e.g., revenue_fy1 and pat_fy1 are correlated — MICE uses one to predict the other.
    #
    # We only run MICE on numeric financial columns with <40% missingness.
    
    mice_cols = [
        c for c in ["revenue_fy1", "revenue_fy2", "revenue_fy3",
                     "pat_fy1", "pat_fy2", "pat_fy3",
                     "ebitda_fy1", "total_assets_fy1",
                     "revenue_cagr_2y", "pat_margin_pct"]
        if c in df.columns and df[c].isna().mean() < 0.4
    ]

    if mice_cols:
        log.info(f"  Running MICE on {len(mice_cols)} financial columns...")
        mice_df = df[mice_cols].copy()
        for col in mice_cols:
            mice_df[col] = pd.to_numeric(mice_df[col], errors="coerce")

        imputer = IterativeImputer(
            estimator=RandomForestRegressor(n_estimators=50, random_state=42),
            max_iter=10,
            random_state=42,
            verbose=0
        )
        imputed = imputer.fit_transform(mice_df)
        df[mice_cols] = imputed
        log.info(f"  MICE complete")

    return df


# ─────────────────────────────────────────────────────────────────────
# SECTION 7 — DROP DECISIONS
# ─────────────────────────────────────────────────────────────────────

def drop_unrecoverable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops rows that are unrecoverable even after all fallbacks.
    
    Rules:
    1. Missing CRITICAL fields → drop
    2. Missing target variable (listing_gain_pct AND return_1Y) → drop
       for ML training set (keep for inference/display)
    3. Columns with >60% missingness after all imputation → drop the COLUMN
    """
    n_start = len(df)
    
    # Drop rows with missing critical fields
    for col in FIELD_TAXONOMY["critical"]:
        if col in df.columns:
            before = len(df)
            df = df[df[col].notna()]
            dropped = before - len(df)
            if dropped:
                log.info(f"  Dropped {dropped} rows: missing critical field '{col}'")

    # Separate ML training set (has target) from display set (no target needed)
    df["has_target"] = (
        df["listing_gain_pct"].notna() |
        df.get("return_1Y", pd.Series(dtype=float)).notna()
    ).astype(int)

    log.info(f"  {df['has_target'].sum()} rows usable for ML training")
    log.info(f"  {(df['has_target']==0).sum()} rows display-only (no return data)")

    # Drop columns with >60% missingness — too sparse to be useful
    col_null_pct = df.isnull().mean()
    drop_cols = col_null_pct[col_null_pct > 0.6].index.tolist()
    if drop_cols:
        log.info(f"  Dropping {len(drop_cols)} sparse columns: {drop_cols}")
        df.drop(columns=drop_cols, inplace=True)

    log.info(f"  Rows: {n_start} → {len(df)} after drop decisions")
    return df


# ─────────────────────────────────────────────────────────────────────
# SECTION 8 — FINAL TYPE COERCION + SANITY CHECKS
# ─────────────────────────────────────────────────────────────────────

def coerce_types_and_sanity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures all numeric columns are actually numeric.
    Removes physically impossible values (negative issue sizes,
    P/E of 10000x, etc.) — these are scraping artifacts, not real data.
    """
    log.info("Type coercion and sanity checks...")

    numeric_cols = [
        "offer_price", "listing_gain_pct", "issue_size_cr", "fresh_issue_cr",
        "ofs_cr", "ofs_pct", "roe", "roce", "pe_ratio", "pb_ratio", "ev_ebitda",
        "market_cap_cr", "promoter_holding_pre", "promoter_holding_post",
        "qib_sub", "hni_sub", "rii_sub", "total_sub",
        "revenue_fy1", "revenue_fy2", "revenue_fy3",
        "pat_fy1", "pat_fy2", "pat_fy3",
        "gmp_peak", "gmp_listing_day", "gmp_trend",
        "return_1M", "return_3M", "return_6M", "return_1Y",
        "alpha_1M", "alpha_3M", "alpha_6M", "alpha_1Y",
        "nifty_30d_return_pct", "vix_at_listing",
        "revenue_cagr_2y", "pat_margin_pct", "qib_retail_ratio",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Sanity bounds — values outside these are scraping artifacts
    BOUNDS = {
        "offer_price":            (0.5, 100_000),
        "issue_size_cr":          (0.1, 50_000),
        "ofs_pct":                (0, 100),
        "promoter_holding_pre":   (0, 100),
        "promoter_holding_post":  (0, 100),
        "pe_ratio":               (0, 1000),      # P/E of 1000+ is meaningless
        "pb_ratio":               (0, 200),
        "roe":                    (-200, 500),     # Extreme but possible in restructurings
        "vix_at_listing":         (5, 100),
        "total_sub":              (0, 1000),       # 1000x oversubscribed is realistic max
        "qib_sub":                (0, 3000),
        "listing_gain_pct":       (-100, 500),
    }

    for col, (lo, hi) in BOUNDS.items():
        if col in df.columns:
            out_of_bounds = ((df[col] < lo) | (df[col] > hi)) & df[col].notna()
            n = out_of_bounds.sum()
            if n:
                log.warning(f"  {col}: {n} values outside [{lo}, {hi}] → set to NaN")
                df.loc[out_of_bounds, col] = np.nan

    return df


# ─────────────────────────────────────────────────────────────────────
# SECTION 9 — BUILD FINAL FEATURE MATRIX
# ─────────────────────────────────────────────────────────────────────

def build_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the final ML-ready feature matrix.
    Separates:
      - ml_features.parquet  → training set with all features + targets
      - display_data.csv     → full dataset for website (includes display-only IPOs)
    
    Also one-hot encodes categorical columns (sector, exchange, sale_type).
    """
    log.info("Building final feature matrix...")

    # Target variables
    target_cols = [
        "listing_gain_pct",          # regression target: listing day return
        "return_1M", "return_3M",
        "return_6M", "return_1Y",    # regression targets: post-listing returns  
        "alpha_1Y",                  # alpha vs NIFTY
    ]

    # Derived binary targets for classification
    if "listing_gain_pct" in df.columns:
        df["target_listing_positive"] = (df["listing_gain_pct"] > 0).astype(int)
        df["target_listing_gt10"]     = (df["listing_gain_pct"] > 10).astype(int)
    if "return_1Y" in df.columns:
        df["target_1y_positive"]      = (df["return_1Y"] > 0).astype(int)
    if "alpha_1Y" in df.columns:
        df["target_beat_nifty"]       = (df["alpha_1Y"] > 0).astype(int)

    # One-hot encode categoricals
    cat_cols = [c for c in ["sector", "exchange", "sale_type"] if c in df.columns]
    if cat_cols:
        df = pd.get_dummies(df, columns=cat_cols, prefix=cat_cols, drop_first=True)

    # Save ML training set (only rows with at least one target)
    ml_df = df[df["has_target"] == 1].copy()
    ml_df.to_parquet("ml_features.parquet", index=False)
    log.info(f"  ML feature matrix: {len(ml_df)} rows × {len(ml_df.columns)} cols → ml_features.parquet")

    return df


# ─────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────

def _safe_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except:
        return None

def _parse_date(s):
    for fmt in ("%d %b %Y", "%Y-%m-%d", "%d-%m-%Y", "%b %d, %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt)
        except:
            continue
    return None


def run_pipeline():
    if not Path(INPUT_CSV).exists():
        log.error(f"{INPUT_CSV} not found. Run scrape_ipo_v2.py first.")
        return

    log.info(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    log.info(f"Loaded: {len(df)} rows × {len(df.columns)} columns")

    # Step 1: Audit — understand the damage first
    audit_missing(df)

    # Step 2: Fix what can be computed from existing data
    df = fill_derivable(df)

    # Step 3: Handle structurally missing fields (GMP pre-2015, etc.)
    df = handle_structural_missing(df)

    # Step 4: Sanity check types and bounds before imputation
    df = coerce_types_and_sanity(df)

    # Step 5: Fetch missing returns from yfinance
    df = fetch_missing_returns(df)

    # Step 6: Fetch missing ratios from Screener
    # (Comment out if you want to skip the HTTP calls and just impute)
    df = fill_from_screener(df)

    # Step 7: Statistical imputation for everything else
    df = impute_fields(df)

    # Step 8: Drop unrecoverable rows + sparse columns
    df = drop_unrecoverable(df)

    # Step 9: Build ML feature matrix + save
    df = build_feature_matrix(df)

    # Save clean full dataset
    df.to_csv(OUTPUT_CSV, index=False)
    log.info(f"\nClean dataset saved: {OUTPUT_CSV}")

    # Final audit
    print("\n── Final Null Check ──")
    remaining_nulls = df.isnull().sum()
    remaining_nulls = remaining_nulls[remaining_nulls > 0]
    if remaining_nulls.empty:
        print("✓ Zero nulls remaining in ML feature matrix")
    else:
        print(remaining_nulls.to_string())

    print(f"\n── Dataset Summary ──")
    print(f"Total IPOs:          {len(df)}")
    print(f"ML-ready IPOs:       {df['has_target'].sum()}")
    print(f"Features:            {len(df.columns)}")
    print(f"Year range:          {int(df['year'].min())}–{int(df['year'].max())}")
    if "sector" in df.columns or any("sector_" in c for c in df.columns):
        print(f"Sectors covered:     see {OUTPUT_CSV}")


if __name__ == "__main__":
    run_pipeline()
"""
api.py  (v3 — strict + bulletproof)
───────────────────────────────────
FastAPI gateway for the IPO Sentiment Terminal.
Strict 404 on unknown IPO; tightened sanitiser; opt-in mock only.
"""

from __future__ import annotations

import logging
import math
import os
import sys
from datetime import datetime
from typing import Any, Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("api")

app = FastAPI(
    title="IPO Sentiment Terminal API",
    version="5.0.0",
    description="Strictly-validated, ML-ready sentiment & market context for Indian IPOs.",
)
app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"])

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
DEMO_MODE    = os.environ.get("DEMO_MODE", "0") == "1"

FEATURE_STORE_PATH = os.environ.get("FEATURE_STORE_PATH", "storage/features/ipo_features.parquet")
IPO_MASTER_PATH    = os.environ.get("IPO_MASTER_PATH", "data/Initial_Public_Offering.xlsx")


# ── Sanitiser ────────────────────────────────────────────────────────────────
def sanitize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float):
        return None if (math.isnan(v) or math.isinf(v)) else round(v, 6)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 6)
    if isinstance(v, (pd.Timestamp, datetime)):
        try: return v.isoformat()
        except Exception: return str(v)
    if isinstance(v, dict):
        return sanitize_payload(v)
    if hasattr(v, "tolist"):
        return sanitize_value(v.tolist())
    if isinstance(v, (list, tuple)):
        return [sanitize_value(item) for item in v]
    return v


def sanitize_payload(payload: dict) -> dict:
    return {k: sanitize_value(v) for k, v in payload.items()}


# ── IPO Master ───────────────────────────────────────────────────────────────
_ipo_index: Optional[pd.DataFrame] = None

# Column map: enriched CSV field names → canonical API names
_CSV_COL_MAP = {
    "name":             "IPO_Name",
    "listing_date":     "Date",
    "listing_gain_pct": "Listing Gain",
    "issue_size_cr":    "Issue_Size(crores)",
    "offer_price":      "Offer Price",
    "listing_price":    "List Price",
    "total_sub":        "Total",
    "qib_sub":          "QIB",
    "hni_sub":          "HNI",
    "rii_sub":          "RII",
}

# Candidate paths tried in order; first existing file wins
_MASTER_CANDIDATES = [
    (IPO_MASTER_PATH,                    "xlsx"),
    ("data/ipo_master_enriched.csv",     "csv"),
    ("data/Initial_Public_Offering.csv", "csv"),
]


def _load_ipo_index() -> pd.DataFrame:
    global _ipo_index
    if _ipo_index is not None:
        return _ipo_index

    raw = None
    for path, fmt in _MASTER_CANDIDATES:
        if not os.path.exists(path):
            continue
        try:
            raw = (pd.read_excel(path, engine="openpyxl") if fmt == "xlsx"
                   else pd.read_csv(path, low_memory=False))
            logger.info(f"IPO master loaded from {path} ({fmt})")
            break
        except Exception as e:
            logger.warning(f"Could not read {path}: {e}")

    if raw is None:
        logger.error("No IPO master file found — unvalidated fallback mode active.")
        _ipo_index = pd.DataFrame()
        return _ipo_index

    df = raw.copy()
    df.columns = [c.strip() for c in df.columns]

    # Remap enriched-CSV column names to canonical names when xlsx names are absent
    if "IPO_Name" not in df.columns:
        df = df.rename(columns=_CSV_COL_MAP)

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "IPO_Name"])

    df["_search_key"]       = df["IPO_Name"].astype(str).str.lower().str.strip()
    df["_listing_date_str"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["_display_label"]    = (
        df["IPO_Name"].astype(str)
        + " (" + df["Date"].dt.strftime("%b %Y").fillna("Unknown") + ")"
    )

    numeric_cols = ["Issue_Size(crores)", "QIB", "HNI", "RII", "Total",
                    "Offer Price", "List Price", "Listing Gain",
                    "CMP(BSE)", "CMP(NSE)", "Current Gains"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace("%", "").str.replace(",", ""),
                errors="coerce")

    df.sort_values("Date", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    _ipo_index = df
    logger.info(f"IPO master ready: {len(df)} records")
    return _ipo_index


def _validate_ipo(ipo_name: str, listing_date: str) -> Optional[dict]:
    df = _load_ipo_index()

    # 1. Try master file (xlsx or csv)
    if not df.empty:
        name_l = ipo_name.lower().strip()
        mask = (df["_search_key"].str.contains(name_l, na=False, regex=False)
                & (df["_listing_date_str"] == listing_date))
        matches = df[mask]
        if not matches.empty:
            return matches.iloc[0].to_dict()

    # 2. Fallback: check parquet (covers IPOs pre-run via orchestrator.py)
    cached = _get_cached_features(ipo_name, listing_date)
    if cached:
        logger.info(f"Validation via parquet cache: {ipo_name} @ {listing_date}")
        return {
            "IPO_Name": cached.get("ipo_name", ipo_name),
            "_listing_date_str": listing_date,
            "_search_key": ipo_name.lower().strip(),
            "_display_label": f"{ipo_name} ({listing_date})",
        }

    # 3. Master file not loaded at all — allow any query through
    if df.empty:
        logger.warning(
            f"IPO master not loaded — allowing unvalidated run: {ipo_name} @ {listing_date}"
        )
        return {
            "IPO_Name": ipo_name,
            "_listing_date_str": listing_date,
            "_search_key": ipo_name.lower().strip(),
            "_display_label": f"{ipo_name} ({listing_date})",
        }

    return None


# ── Feature store ────────────────────────────────────────────────────────────
def _read_feature_store() -> pd.DataFrame:
    if os.path.exists(FEATURE_STORE_PATH):
        try: return pd.read_parquet(FEATURE_STORE_PATH)
        except Exception as e: logger.error(f"Feature store unreadable: {e}")
    return pd.DataFrame()


def _get_cached_features(ipo_name: str, listing_date: str) -> Optional[dict]:
    store = _read_feature_store()
    if store.empty: return None
    mask = ((store["ipo_name"].str.lower() == ipo_name.lower())
            & (store["listing_date"] == listing_date))
    matches = store[mask]
    if matches.empty: return None

    raw_row = matches.iloc[-1].to_dict()

    # Convert potentially serialized list/dict fields back from numpy/pandas if needed
    # (Though read_parquet usually keeps them as objects)
    payload = sanitize_payload(raw_row)

    # These fields are now persisted in v4.1+
    payload.setdefault("articles", [])
    payload.setdefault("nifty_price_series", [])
    payload.setdefault("sentiment_momentum_series", [])

    # Backwards compatibility: rows produced before Llama / FRED columns
    # existed should still render in the UI without undefined errors.
    payload.setdefault("llama_score", 0.0)
    payload.setdefault("llama_summary", "")
    payload.setdefault("avg_flesch_score", 0.0)
    payload.setdefault("macro_available", False)
    payload.setdefault("macro_score", 0.0)
    payload.setdefault("macro_briefing", "")
    payload.setdefault("macro_risk_regime", "unknown")
    payload.setdefault("macro_rate_regime", "unknown")
    payload.setdefault("macro_dollar_regime", "unknown")
    return payload


# ── Live pipeline ────────────────────────────────────────────────────────────
def _run_live_pipeline(ipo_name: str, listing_date: str) -> dict:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from workers import sentiment_worker, market_worker
    try:
        from workers import fred_worker
    except ImportError:
        fred_worker = None
    from utils.feature_aggregator import (
        compute_composite_score, to_feature_row, save_to_parquet,
        compute_visuals,
    )
    sentiment = sentiment_worker.run(
        ipo_name=ipo_name, listing_date=listing_date,
        news_api_key=NEWS_API_KEY, groq_api_key=GROQ_API_KEY,
        lookback_days=30,
    )
    market = market_worker.run(listing_date=listing_date, lookback_days=60)

    # Macro snapshot (FRED) — graceful no-op if FRED_API_KEY unset.
    if fred_worker is not None:
        try:
            macro = fred_worker.run(listing_date=listing_date)
            macro_dict = {
                "available":      macro.available,
                "snapshot":       macro.snapshot,
                "macro_score":    macro.macro_score,
                "macro_briefing": macro.macro_briefing,
                "regime":         macro.regime,
            }
        except Exception as e:
            logger.warning(f"FRED worker failed: {e}")
            macro_dict = {"available": False, "snapshot": {}, "macro_score": 0.0,
                          "macro_briefing": "Macro fetch failed.", "regime": {}}
    else:
        macro_dict = {"available": False, "snapshot": {}, "macro_score": 0.0,
                      "macro_briefing": "fred_worker not available.", "regime": {}}

    composite = compute_composite_score(
        sentiment, market,
        macro_score=macro_dict["macro_score"],
        macro_available=macro_dict["available"],
    )

    # Pre-compute visuals for the aggregator
    articles_minified, momentum_series = compute_visuals(sentiment)

    row = to_feature_row(
        ipo_name, listing_date, sentiment, market, composite,
        macro=macro_dict,
        articles=articles_minified,
        nifty_price_series=getattr(market, "price_series", []),
        sentiment_momentum_series=momentum_series
    )

    row["ml_features"] = [
        "article_count","avg_positive","avg_negative","avg_neutral",
        "sentiment_momentum","groq_score","llama_score","avg_flesch_score",
        "nifty_return_1d","nifty_return_5d","nifty_return_window",
        "nifty_sma_20","nifty_ema_12","nifty_ema_26","nifty_macd",
        "nifty_above_sma","nifty_price_t1",
        "vix_t1","vix_avg_window","vix_trend",
        "market_mood_score","macro_score","composite_score",
        "fred_cboe_vix_t1","fred_us_10y_yield_t1","fred_inr_usd_t1",
        "fred_dxy_t1","fred_oil_brent_t1",
    ]
    row["ml_feature_version"] = "4.1.0"
    save_to_parquet(row)
    return sanitize_payload(row)


# ── Endpoints ────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "version": app.version,
            "timestamp": datetime.utcnow().isoformat(),
            "demo_mode": DEMO_MODE,
            "master_loaded": not _load_ipo_index().empty}


@app.get("/api/search")
async def search_ipos(
    q: str = Query(""),
    limit: int = Query(20, ge=1, le=100),
    type: str = Query("", description="Optional filter; pass 'ipo' to enforce a valid listing_date"),
):
    df = _load_ipo_index()
    enforce_ipo = (type or "").strip().lower() == "ipo"

    # When master is empty fall back to parquet feature store
    if df.empty:
        store = _read_feature_store()
        if not store.empty:
            query = q.strip().lower()
            if query:
                mask = store["ipo_name"].str.lower().str.contains(query, na=False, regex=False)
                subset = store[mask].head(limit)
            else:
                subset = store.head(limit)
            results = []
            for _, r in subset.iterrows():
                ld = str(r.get("listing_date", "") or "")
                if enforce_ipo:
                    # Must look like a valid YYYY-MM-DD listing date
                    try:
                        datetime.strptime(ld, "%Y-%m-%d")
                    except ValueError:
                        continue
                results.append(sanitize_payload({
                    "ipo_name":           str(r.get("ipo_name", "")),
                    "listing_date":       ld,
                    "display_label":      f"{r.get('ipo_name','')} ({ld})",
                    "result_type":        "ipo",   # NEW: always tag as IPO
                    "issue_size_cr":      None,
                    "listing_gain":       None,
                    "offer_price":        None,
                    "total_subscription": None,
                }))
            return {"results": results, "total": len(results), "source": "parquet_cache"}
        return {"results": [], "total": 0, "warning": "IPO master not loaded"}

    query = q.strip().lower()
    if not query:
        subset = df.head(limit)
    else:
        prefix = df["_search_key"].str.startswith(query, na=False)
        sub    = df["_search_key"].str.contains(query, na=False, regex=False)
        subset = pd.concat([df[prefix], df[sub & ~prefix]]).head(limit)

    results = []
    for _, r in subset.iterrows():
        ld = str(r.get("_listing_date_str", "") or "")
        if enforce_ipo:
            # type=ipo enforces a parseable listing date — future-proofs the
            # endpoint against rows that might lack one.
            try:
                datetime.strptime(ld, "%Y-%m-%d")
            except ValueError:
                continue
        results.append(sanitize_payload({
            "ipo_name":           str(r.get("IPO_Name", "")),
            "listing_date":       ld,
            "display_label":      str(r.get("_display_label", "")),
            "result_type":        "ipo",   # tag as IPO
            "issue_size_cr":      r.get("Issue_Size(crores)"),
            "listing_gain":       r.get("Listing Gain"),
            "offer_price":        r.get("Offer Price"),
            "total_subscription": r.get("Total"),
        }))

    # ── v5: parquet-cache supplementation ─────────────────────────────────
    # Surface IPOs that were processed via orchestrator/allow_custom but
    # are not in the master CSV (covers IPOs added after the CSV was last
    # refreshed — e.g. anything from 2025 onward, ad-hoc analyses, etc).
    if query:
        seen_keys = {(r["ipo_name"].lower().strip(), r["listing_date"]) for r in results}
        store = _read_feature_store()
        if not store.empty and "ipo_name" in store.columns:
            mask = store["ipo_name"].str.lower().str.contains(query, na=False, regex=False)
            for _, row in store[mask].head(limit).iterrows():
                key = (str(row.get("ipo_name", "")).lower().strip(),
                       str(row.get("listing_date", "")))
                if key in seen_keys or not key[0] or not key[1]:
                    continue
                seen_keys.add(key)
                results.append({
                    "ipo_name":      str(row.get("ipo_name", "")),
                    "listing_date":  str(row.get("listing_date", "")),
                    "display_label": f"{row.get('ipo_name','')} ({row.get('listing_date','')})",
                    "result_type":   "ipo",
                    "issue_size_cr":      None,
                    "listing_gain":       None,
                    "offer_price":        None,
                    "total_subscription": None,
                    "_from_cache": True,
                })

    # ── v5: live-analyze suggestion ───────────────────────────────────────
    # Always append a "live mode" placeholder for any non-empty query so
    # the user can analyze an IPO that isn't in the CSV (e.g. Zomato when
    # the CSV doesn't have it, or a 2027 IPO that doesn't exist yet).
    # The frontend renders this as a ⚡ Analyze chip and opens a date
    # picker, then calls /api/sentiment with allow_custom=true.
    if query:
        results.append({
            "ipo_name":      q.strip(),
            "listing_date":  "",            # frontend will prompt for a date
            "display_label": f'Analyze "{q.strip()}" via live pipeline',
            "result_type":   "live_analyze",
            "issue_size_cr": None,
            "listing_gain":  None,
            "offer_price":   None,
            "total_subscription": None,
        })

    return {"results": results, "total": len(results)}


@app.get("/api/sentiment/{ipo_name}")
async def get_sentiment(
    ipo_name: str,
    listing_date: str = Query(..., description="Listing date YYYY-MM-DD"),
    force_refresh: bool = Query(False),
    mock: bool = Query(False),
    allow_custom: bool = Query(False,
        description="Bypass master validation for upcoming/future IPOs"),
):
    try:
        datetime.strptime(listing_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail={
            "error": "Invalid date format", "expected": "YYYY-MM-DD",
            "got": listing_date})

    master_row = _validate_ipo(ipo_name, listing_date)
    if master_row is None:
        if not allow_custom:
            raise HTTPException(status_code=404, detail={
                "error": "IPO not found in master dataset",
                "ipo_name": ipo_name, "listing_date": listing_date,
                "hint": "Verify name and listing date via /api/search. "
                        "For upcoming/future IPOs retry with ?allow_custom=true."})
        logger.info(f"allow_custom=true: unvalidated run for {ipo_name} @ {listing_date}")
        master_row = {
            "IPO_Name": ipo_name,
            "_listing_date_str": listing_date,
            "_search_key": ipo_name.lower().strip(),
            "_display_label": f"{ipo_name} ({listing_date})",
        }

    canonical_name = str(master_row["IPO_Name"])

    if not force_refresh:
        cached = _get_cached_features(canonical_name, listing_date)
        if cached:
            cached.setdefault("articles", [])
            cached.setdefault("nifty_price_series", [])
            cached.setdefault("sentiment_momentum_series", [])
            return JSONResponse(content=cached)

    if mock or DEMO_MODE:
        return JSONResponse(content=_generate_mock_features(canonical_name, listing_date))

    if not GROQ_API_KEY:
        raise HTTPException(status_code=503, detail={
            "error": "Pipeline unavailable", "reason": "GROQ_API_KEY not configured",
            "hint": "Set GROQ_API_KEY env var or call with ?mock=true"})

    try:
        return JSONResponse(content=_run_live_pipeline(canonical_name, listing_date))
    except Exception as e:
        logger.error(f"Pipeline failed for {canonical_name}: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail={
            "error": "Upstream pipeline failure", "reason": str(e)[:300]})


@app.get("/api/ipo/{ipo_name}/fundamentals")
async def get_fundamentals(ipo_name: str, listing_date: str = Query(...)):
    master_row = _validate_ipo(ipo_name, listing_date)
    if master_row is None:
        raise HTTPException(status_code=404, detail={
            "error": "IPO not found or invalid date",
            "ipo_name": ipo_name, "listing_date": listing_date})
    return JSONResponse(content=sanitize_payload({
        "ipo_name":            str(master_row.get("IPO_Name", "")),
        "listing_date":        str(master_row.get("_listing_date_str", "")),
        "issue_size_cr":       master_row.get("Issue_Size(crores)"),
        "offer_price":         master_row.get("Offer Price"),
        "list_price":          master_row.get("List Price"),
        "listing_gain_pct":    master_row.get("Listing Gain"),
        "current_gains_pct":   master_row.get("Current Gains"),
        "qib_subscription":    master_row.get("QIB"),
        "hni_subscription":    master_row.get("HNI"),
        "rii_subscription":    master_row.get("RII"),
        "total_subscription":  master_row.get("Total"),
        "cmp_bse":             master_row.get("CMP(BSE)"),
        "cmp_nse":             master_row.get("CMP(NSE)"),
    }))


@app.get("/api/ml/feature_vector/{ipo_name}")
async def get_ml_feature_vector(ipo_name: str, listing_date: str = Query(...)):
    master_row = _validate_ipo(ipo_name, listing_date)
    if master_row is None:
        raise HTTPException(status_code=404, detail={
            "error": "IPO not found or invalid date",
            "ipo_name": ipo_name, "listing_date": listing_date})
    canonical_name = str(master_row["IPO_Name"])

    full = _get_cached_features(canonical_name, listing_date)
    if full is None:
        if not GROQ_API_KEY:
            raise HTTPException(status_code=503,
                detail={"error": "Pipeline unavailable: GROQ_API_KEY not set"})
        try:
            full = _run_live_pipeline(canonical_name, listing_date)
        except Exception as e:
            raise HTTPException(status_code=502, detail={
                "error": "Upstream pipeline failure", "reason": str(e)[:300]})

    ml_keys = full.get("ml_features", [])
    vector = {"ipo_name": canonical_name, "listing_date": listing_date}
    for key in ml_keys:
        vector[key] = full.get(key)

    vector["issue_size_cr"]      = sanitize_value(master_row.get("Issue_Size(crores)"))
    vector["offer_price"]        = sanitize_value(master_row.get("Offer Price"))
    vector["qib_subscription"]   = sanitize_value(master_row.get("QIB"))
    vector["hni_subscription"]   = sanitize_value(master_row.get("HNI"))
    vector["rii_subscription"]   = sanitize_value(master_row.get("RII"))
    vector["total_subscription"] = sanitize_value(master_row.get("Total"))

    for k, v in list(vector.items()):
        if not (v is None or isinstance(v, (int, float, str, bool))):
            vector[k] = None

    return JSONResponse(content=sanitize_payload(vector))


# ── Mock generator (opt-in) ──────────────────────────────────────────────────
def _generate_mock_features(ipo_name: str, listing_date: str) -> dict:
    import random
    from datetime import timedelta
    random.seed(hash(ipo_name + listing_date) % 2**32)
    avg_pos = round(random.uniform(0.35, 0.70), 4)
    avg_neg = round(random.uniform(0.10, 0.30), 4)
    avg_neu = round(max(0.0, 1.0 - avg_pos - avg_neg), 4)
    groq_score   = round(random.uniform(-0.3, 0.8), 4)
    market_mood  = round(random.uniform(-0.5, 0.7), 4)
    momentum     = round(random.uniform(-0.2, 0.4), 4)
    nifty_price  = round(random.uniform(14000, 25000), 2)
    nifty_5d     = round(random.uniform(-0.03, 0.04), 4)
    vix          = round(random.uniform(10, 28), 2)
    composite    = max(-1.0, min(1.0,
        0.30 * groq_score + 0.25 * (avg_pos - avg_neg) + 0.25 * market_mood
        + 0.10 * min(1.0, max(-1.0, momentum * 5))
        + 0.10 * (np.log1p(random.randint(5, 50)) / np.log1p(50) * 2 - 1)
    ))
    base_date = datetime.strptime(listing_date, "%Y-%m-%d")
    headlines = [
        ("IPO oversubscribed on final day", "positive"),
        ("Institutional interest near record for listing", "positive"),
        ("Analysts split on issue price valuation", "neutral"),
        ("Grey market premium reaches 50% above issue price", "positive"),
        ("Brokerages issue 'subscribe' calls ahead of listing", "positive"),
    ]
    n_articles = random.randint(8, 40)
    articles = []
    for i, (h, lbl) in enumerate(headlines[:n_articles]):
        pub = base_date - timedelta(days=random.randint(1, 28))
        pos = round(random.uniform(0.3, 0.8) if lbl == "positive" else random.uniform(0.05, 0.3), 4)
        neg = round(random.uniform(0.3, 0.7) if lbl == "negative" else random.uniform(0.05, 0.2), 4)
        articles.append({
            "title": f"{ipo_name} {h}", "published_at": pub.isoformat(),
            "source": random.choice(["Moneycontrol", "Economic Times", "LiveMint"]),
            "sentiment_label": lbl,
            "finbert_positive": pos, "finbert_negative": neg,
            "finbert_neutral":  round(max(0.0, 1 - pos - neg), 4),
        })
    articles.sort(key=lambda x: x["published_at"])
    nifty_series = []
    p = nifty_price - random.uniform(500, 1200)
    for d in range(30):
        p += random.uniform(-50, 60)
        nifty_series.append({
            "label": f"T-{30-d}", "close": round(p, 2),
            "date": (base_date - timedelta(days=30-d)).strftime("%Y-%m-%d")})
    momentum_series = []
    pos_score = random.uniform(0.35, 0.45)
    for d in range(30):
        pos_score = max(0.2, min(0.9, pos_score + random.uniform(-0.02, 0.03)))
        momentum_series.append({
            "label": f"T-{30-d}", "positive_score": round(pos_score, 4),
            "date": (base_date - timedelta(days=30-d)).strftime("%Y-%m-%d")})

    return sanitize_payload({
        "ipo_name": ipo_name, "listing_date": listing_date,
        "run_timestamp": datetime.utcnow().isoformat(), "_demo": True,
        "composite_score": round(composite, 4),
        "article_count": n_articles,
        "avg_positive": avg_pos, "avg_negative": avg_neg, "avg_neutral": avg_neu,
        "dominant_sentiment": max(
            {"positive": avg_pos, "negative": avg_neg, "neutral": avg_neu},
            key=lambda k: {"positive": avg_pos, "negative": avg_neg, "neutral": avg_neu}[k]),
        "sentiment_momentum": momentum,
        "groq_score": groq_score,
        "groq_summary": f"[DEMO] Pre-IPO coverage on {ipo_name} is "
                        f"{'positive' if groq_score > 0.3 else 'mixed'}. "
                        f"LLaMA-3.3-70B aggregate (synthetic).",
        "llama_score": round(max(-1.0, min(1.0, groq_score + random.uniform(-0.15, 0.15))), 4),
        "llama_summary": f"[DEMO] LLaMA-3.1-8B fast aggregate on {ipo_name}: tone is "
                         f"{'constructive' if groq_score > 0.2 else 'cautious'} (synthetic).",
        "nifty_return_1d": round(random.uniform(-0.02, 0.02), 4),
        "nifty_return_5d": nifty_5d,
        "nifty_return_window": round(random.uniform(-0.05, 0.08), 4),
        "nifty_sma_20": round(nifty_price - random.uniform(-200, 200), 2),
        "nifty_ema_12": round(nifty_price + random.uniform(-100, 100), 2),
        "nifty_ema_26": round(nifty_price - random.uniform(-150, 150), 2),
        "nifty_macd": round(random.uniform(-100, 150), 2),
        "nifty_above_sma": 1 if nifty_5d > 0 else 0,
        "nifty_price_t1": nifty_price, "vix_t1": vix,
        "vix_avg_window": round(vix + random.uniform(-2, 2), 2),
        "vix_trend": round(random.uniform(-0.5, 0.3), 4),
        "market_mood_score": market_mood,
        "avg_flesch_score": round(random.uniform(30, 75), 1),
        # ── FRED macro (mock) ─────────────────────────────────────────
        "macro_available": True,
        "macro_score":     round(random.uniform(-0.5, 0.6), 4),
        "macro_briefing":  f"[DEMO] Global VIX {round(random.uniform(13,22),1)} (moderate risk). "
                           f"US 10Y yield at {round(random.uniform(2.5,4.8),2)}%. "
                           f"INR/USD at ₹{round(random.uniform(74,84),2)}. "
                           f"Brent crude at ${round(random.uniform(70,95),0):.0f}/bbl.",
        "macro_risk_regime":   random.choice(["risk_on", "neutral", "risk_off"]),
        "macro_rate_regime":   random.choice(["low", "normal", "high"]),
        "macro_dollar_regime": random.choice(["weak", "stable", "strong"]),
        "fred_cboe_vix_t1":      round(random.uniform(13, 25), 2),
        "fred_us_10y_yield_t1":  round(random.uniform(2.5, 4.8), 2),
        "fred_inr_usd_t1":       round(random.uniform(74, 84), 2),
        "fred_inr_usd_30d_chg":  round(random.uniform(-0.03, 0.03), 4),
        "fred_dxy_t1":           round(random.uniform(95, 110), 2),
        "fred_dxy_30d_chg":      round(random.uniform(-0.04, 0.04), 4),
        "fred_oil_brent_t1":     round(random.uniform(65, 95), 2),
        "fred_oil_brent_30d_chg": round(random.uniform(-0.10, 0.10), 4),
        "articles": articles,
        "nifty_price_series": nifty_series,
        "sentiment_momentum_series": momentum_series,
        "ml_feature_version": "4.0.0-demo",
        "ml_features": [
            "article_count","avg_positive","avg_negative","avg_neutral",
            "sentiment_momentum","groq_score","llama_score","avg_flesch_score",
            "nifty_return_1d","nifty_return_5d",
            "nifty_return_window","nifty_sma_20","nifty_ema_12","nifty_ema_26",
            "nifty_macd","nifty_above_sma","nifty_price_t1","vix_t1",
            "vix_avg_window","vix_trend","market_mood_score","macro_score",
            "composite_score","fred_cboe_vix_t1","fred_us_10y_yield_t1",
            "fred_inr_usd_t1","fred_dxy_t1","fred_oil_brent_t1"],
    })


@app.on_event("startup")
async def startup():
    _load_ipo_index()
    logger.info("API ready. Feature store: %s | Demo mode: %s",
                FEATURE_STORE_PATH, DEMO_MODE)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
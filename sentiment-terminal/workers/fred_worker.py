"""
fred_worker.py
──────────────
Macroeconomic context from FRED (Federal Reserve Economic Data).
For India IPOs we care about: global risk appetite (VIX), US rates,
USD strength, INR/USD, and oil (India imports ~85% of consumption).

All values are pulled as of T-1 IST (leak-safe). If FRED_API_KEY is
not set the worker returns an empty dict and the rest of the pipeline
treats macro signals as unavailable (same pattern as news_source).
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Series IDs at https://fred.stlouisfed.org/
INDIA_RELEVANT_SERIES = {
    "us_10y_yield":   "DGS10",          # Risk-free rate benchmark
    "us_fed_funds":   "DFF",            # Global liquidity proxy
    "cboe_vix":       "VIXCLS",         # Global risk appetite (CBOE VIX, NOT India VIX)
    "inr_usd":        "DEXINUS",        # INR per USD exchange rate
    "us_cpi_yoy":     "CPIAUCSL",       # Global inflation (derived: yoy change)
    "dxy":            "DTWEXBGS",       # Dollar strength → EM outflows
    "oil_brent":      "DCOILBRENTEU",   # India imports 85% of oil
}


@dataclass
class MacroFeatures:
    """Container for macro snapshot. All optional — None when FRED unavailable."""
    listing_date: str
    snapshot: dict = field(default_factory=dict)        # raw values & 30d changes
    macro_score: float = 0.0                            # composite [-1, +1]
    macro_briefing: str = ""                            # human-readable summary
    regime: dict = field(default_factory=dict)          # categorical regimes
    available: bool = False                             # True iff FRED_API_KEY set


def _fred_client():
    """Lazy-load FRED client so the worker imports cleanly even without the key."""
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        return None
    try:
        from fredapi import Fred
        return Fred(api_key=api_key)
    except Exception as e:
        logger.warning(f"FRED client init failed: {e}")
        return None


def fetch_fred_snapshot(listing_date: str) -> dict:
    """Returns macro features as of T-1 (leak-safe). Empty dict if FRED disabled."""
    fred = _fred_client()
    if fred is None:
        logger.info("FRED_API_KEY not set — skipping macro snapshot.")
        return {}

    target = pd.to_datetime(listing_date) - timedelta(days=1)
    start = target - timedelta(days=60)
    out: dict = {}
    for feature_name, series_id in INDIA_RELEVANT_SERIES.items():
        try:
            s = fred.get_series(series_id, start, target).dropna()
            if s.empty:
                out[f"{feature_name}_t1"] = None
                out[f"{feature_name}_30d_change"] = None
                continue
            latest = float(s.iloc[-1])
            out[f"{feature_name}_t1"] = latest
            if len(s) >= 20:
                out[f"{feature_name}_30d_change"] = float((latest / s.iloc[-20]) - 1)
            else:
                out[f"{feature_name}_30d_change"] = None
        except Exception as e:
            logger.warning(f"FRED series {series_id} failed: {e}")
            out[f"{feature_name}_t1"] = None
            out[f"{feature_name}_30d_change"] = None
    return out


def build_macro_briefing(snap: dict) -> str:
    """Turn FRED numbers into a short factual paragraph for the LLM / UI."""
    if not snap:
        return "Macro context unavailable — FRED_API_KEY not configured."
    parts = []
    # NOTE: key is 'cboe_vix_t1', not 'vix_t1' (was a bug in the previous version).
    if snap.get("cboe_vix_t1") is not None:
        vix = snap["cboe_vix_t1"]
        regime = "elevated" if vix > 25 else "low" if vix < 15 else "moderate"
        parts.append(f"Global VIX is {vix:.1f} ({regime} risk).")
    if snap.get("us_10y_yield_t1") is not None:
        parts.append(f"US 10Y yield at {snap['us_10y_yield_t1']:.2f}%.")
    if snap.get("inr_usd_t1") is not None:
        parts.append(f"INR/USD at ₹{snap['inr_usd_t1']:.2f}.")
    if snap.get("inr_usd_30d_change") is not None:
        chg = snap["inr_usd_30d_change"] * 100
        parts.append(f"INR moved {chg:+.1f}% vs USD in past 30d.")
    if snap.get("oil_brent_t1") is not None:
        parts.append(f"Brent crude at ${snap['oil_brent_t1']:.0f}/bbl.")
    if snap.get("dxy_30d_change") is not None:
        chg = snap["dxy_30d_change"] * 100
        parts.append(f"Dollar index {chg:+.1f}% in past 30d.")
    return " ".join(parts) if parts else "FRED returned no usable series."


def derive_regime(snap: dict) -> dict:
    """Categorical regime tags useful for ML feature engineering."""
    if not snap:
        return {"macro_risk_regime": "unknown", "rate_regime": "unknown",
                "dollar_regime": "unknown"}
    vix = snap.get("cboe_vix_t1") or 20
    ten_y = snap.get("us_10y_yield_t1") or 4.0
    dxy_chg = snap.get("dxy_30d_change") or 0.0
    return {
        "macro_risk_regime": "risk_off" if vix > 25 else "risk_on" if vix < 15 else "neutral",
        "rate_regime":       "high"    if ten_y > 4.5 else "low"   if ten_y < 3 else "normal",
        "dollar_regime":     "strong"  if dxy_chg > 0.02 else "weak" if dxy_chg < -0.02 else "stable",
    }


def compute_macro_score(snap: dict) -> float:
    """
    Composite macro score in [-1, +1] from an India-importer perspective.
    Bullish (positive) when:
      - Low CBOE VIX (risk-on)
      - Low/falling US 10Y (cheap global liquidity)
      - Weak/falling DXY (EM-friendly)
      - Falling Brent (India is net oil importer)
      - Stable INR
    Returns 0.0 when FRED data unavailable.
    """
    if not snap:
        return 0.0

    parts = []

    # 1. Global VIX: <15 bullish, >25 bearish
    vix = snap.get("cboe_vix_t1")
    if vix is not None:
        if vix < 15:    parts.append(+0.5)
        elif vix > 30:  parts.append(-0.7)
        elif vix > 25:  parts.append(-0.4)
        elif vix > 20:  parts.append(-0.1)
        else:           parts.append(+0.2)

    # 2. US 10Y yield level
    ten_y = snap.get("us_10y_yield_t1")
    if ten_y is not None:
        if ten_y < 2.5:   parts.append(+0.4)
        elif ten_y > 5.0: parts.append(-0.5)
        elif ten_y > 4.5: parts.append(-0.2)
        else:             parts.append(0.0)

    # 3. DXY 30d change (strong dollar = EM outflows = bearish)
    dxy_chg = snap.get("dxy_30d_change")
    if dxy_chg is not None:
        parts.append(float(max(-0.5, min(0.5, -dxy_chg * 10))))

    # 4. Brent oil 30d change (India imports 85% — rising oil = bearish)
    oil_chg = snap.get("oil_brent_30d_change")
    if oil_chg is not None:
        parts.append(float(max(-0.5, min(0.5, -oil_chg * 5))))

    # 5. INR/USD 30d change (depreciation = bearish for foreign capital)
    inr_chg = snap.get("inr_usd_30d_change")
    if inr_chg is not None:
        parts.append(float(max(-0.4, min(0.4, -inr_chg * 20))))

    if not parts:
        return 0.0
    score = sum(parts) / len(parts)
    return float(max(-1.0, min(1.0, score)))


def run(listing_date: str) -> MacroFeatures:
    """Single entry point — mirrors market_worker.run() / sentiment_worker.run()."""
    snap = fetch_fred_snapshot(listing_date)
    available = bool(snap)
    return MacroFeatures(
        listing_date=listing_date,
        snapshot=snap,
        macro_score=compute_macro_score(snap),
        macro_briefing=build_macro_briefing(snap),
        regime=derive_regime(snap),
        available=available,
    )

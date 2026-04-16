"""
feature_aggregator.py
─────────────────────
Merges sentiment + market + macro features into a single ML-ready row.
Writes to storage/features/ipo_features.parquet (deduplicated by IPO+date).

Composite score now includes:
  - Dual LLM ensemble (Groq LLaMA-3.3-70B + LLaMA-3.1-8B averaged)
  - FRES (readability) signal
  - Macro context from FRED (CBOE VIX, US 10Y, INR/USD, Brent, DXY)

If sentiment.news_source == "unavailable" (pre-2013 IPOs), sentiment
weights are reallocated to market+macro features so the score isn't
biased toward neutral by missing news.
"""

import logging
from typing import Optional
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from workers.sentiment_worker import SentimentFeatures
from workers.market_worker import MarketFeatures

logger = logging.getLogger(__name__)

FEATURE_STORE_PATH = Path("storage/features/ipo_features.parquet")


# ── default weights ──────────────────────────────────────────────────────────
# llm_ensemble = average(groq_score, llama_score) — counted as a single signal.
# These sum to 1.0.
DEFAULT_WEIGHTS = {
    "llm_ensemble":  0.28,   # Groq LLaMA-3.3-70B + LLaMA-3.1-8B averaged
    "finbert_net":   0.22,   # avg_pos − avg_neg (FinBERT)
    "market_mood":   0.22,   # Nifty + India VIX
    "macro_fred":    0.08,   # Global macro (CBOE VIX, 10Y, DXY, oil, INR)
    "sent_momentum": 0.10,   # Improving / declining tone
    "coverage":      0.05,   # Log-scaled article count
    "flesch":        0.05,   # Readability of pre-IPO coverage
}


def compute_composite_score(
    sentiment: SentimentFeatures,
    market: MarketFeatures,
    macro_score: float = 0.0,
    macro_available: bool = False,
    weights: Optional[dict] = None,
) -> float:
    """
    Composite Sentiment & Market Score in [-1, 1].

    `macro_score` is the FRED macro composite from fred_worker.compute_macro_score().
    `macro_available` is False when FRED_API_KEY is missing — its weight is
    redistributed to market_mood in that case.

    If sentiment.news_source == "unavailable" (pre-2013), sentiment weights
    collapse onto market_mood + macro_fred.
    """
    if weights is None:
        weights = dict(DEFAULT_WEIGHTS)

    # ── component signals ────────────────────────────────────────────────
    finbert_net = sentiment.avg_positive - sentiment.avg_negative
    coverage = float(np.clip(np.log1p(sentiment.article_count) / np.log1p(50), 0, 1))
    coverage_signal = coverage * 2 - 1
    momentum_clamped = float(np.clip(sentiment.sentiment_momentum * 5, -1, 1))

    # FRES (0-100) → [-1, +1]: 60 = neutral baseline.
    avg_flesch = float(getattr(sentiment, "avg_flesch_score", 0.0) or 0.0)
    flesch_norm = float(np.clip((avg_flesch - 60) / 40, -1, 1))

    # LLM ensemble: average of the two models. If one is missing (0.0)
    # the average gracefully degrades.
    groq_s  = float(getattr(sentiment, "groq_score", 0.0) or 0.0)
    llama_s = float(getattr(sentiment, "llama_score", 0.0) or 0.0)
    llm_ensemble = (groq_s + llama_s) / 2 if (groq_s != 0.0 or llama_s != 0.0) else 0.0

    # ── pre-2013 fallback: market + macro only ───────────────────────────
    if getattr(sentiment, "news_source", "ok") == "unavailable":
        if macro_available:
            score = 0.7 * market.market_mood_score + 0.3 * macro_score
        else:
            score = market.market_mood_score
        return float(np.clip(score, -1.0, 1.0))

    # ── if macro unavailable, redistribute its 8% to market_mood ─────────
    w = dict(weights)
    if not macro_available:
        w["market_mood"] = w["market_mood"] + w["macro_fred"]
        w["macro_fred"] = 0.0

    score = (
        w["llm_ensemble"]  * llm_ensemble
      + w["finbert_net"]   * finbert_net
      + w["market_mood"]   * market.market_mood_score
      + w["macro_fred"]    * macro_score
      + w["sent_momentum"] * momentum_clamped
      + w["coverage"]      * coverage_signal
      + w["flesch"]        * flesch_norm
    )
    return float(np.clip(score, -1.0, 1.0))


def compute_visuals(sentiment: SentimentFeatures) -> tuple[list[dict], list[dict]]:
    """
    Minify articles and compute daily sentiment momentum series for visuals.
    Returns (articles_minified, sentiment_momentum_series)
    """
    # 1. Minify articles
    articles_minified = [{
        "title": a.title,
        "published_at": a.published_at_ist.isoformat() if a.published_at_ist else None,
        "source": a.source,
        "sentiment_label": a.finbert_label,
        "finbert_positive": a.finbert_positive,
        "finbert_negative": a.finbert_negative,
        "finbert_neutral":  a.finbert_neutral,
    } for a in getattr(sentiment, "articles", [])]

    # 2. Build momentum series (group by day)
    from collections import defaultdict
    day_scores = defaultdict(list)
    for a in getattr(sentiment, "articles", []):
        if a.published_at_ist:
            day_key = a.published_at_ist.strftime("%b %d")
            day_scores[day_key].append(a.finbert_positive)
    
    momentum_series = []
    for day_label, scores in sorted(day_scores.items(), key=lambda x: x[0]):
        momentum_series.append({
            "label": day_label,
            "positive_score": round(sum(scores) / len(scores), 4),
        })
    
    return articles_minified, momentum_series


def to_feature_row(
    ipo_name: str,
    listing_date: str,
    sentiment: SentimentFeatures,
    market: MarketFeatures,
    composite_score: float,
    macro: Optional[dict] = None,
    articles: Optional[list] = None,
    nifty_price_series: Optional[list] = None,
    sentiment_momentum_series: Optional[list] = None,
) -> dict:
    """
    Build the ML-ready row. Includes visual data for persistence.
    """
    macro = macro or {}
    snap = macro.get("snapshot", {}) or {}
    regime = macro.get("regime", {}) or {}

    # Serialize articles (minified)
    if articles is None and hasattr(sentiment, "articles"):
        articles = [{
            "title": a.title,
            "published_at": a.published_at_ist.isoformat() if a.published_at_ist else None,
            "source": a.source,
            "sentiment_label": a.finbert_label,
            "finbert_positive": a.finbert_positive,
            "finbert_negative": a.finbert_negative,
            "finbert_neutral":  a.finbert_neutral,
        } for a in sentiment.articles]

    row = {
        "ipo_name": ipo_name,
        "listing_date": listing_date,
        "run_timestamp": datetime.utcnow().isoformat(),
        "composite_score": composite_score,

        # ── Visuals / Time-series (JSON-serializable lists) ───────────
        "articles":                  articles or [],
        "nifty_price_series":        nifty_price_series or getattr(market, "price_series", []),
        "sentiment_momentum_series": sentiment_momentum_series or [],

        # ── Sentiment ──────────────────────────────────────────────────
        "article_count": sentiment.article_count,
        "avg_positive": sentiment.avg_positive,
        "avg_negative": sentiment.avg_negative,
        "avg_neutral":  sentiment.avg_neutral,
        # ... (rest of the fields)
        "dominant_sentiment": sentiment.dominant_sentiment,
        "sentiment_momentum": sentiment.sentiment_momentum,
        "groq_score":   sentiment.groq_score,
        "groq_summary": sentiment.groq_summary,
        "llama_score":   getattr(sentiment, "llama_score", 0.0),
        "llama_summary": getattr(sentiment, "llama_summary", ""),
        "news_source": getattr(sentiment, "news_source", "ok"),
        "articles_filtered_irrelevant": getattr(sentiment, "articles_filtered_irrelevant", 0),
        "avg_flesch_score": getattr(sentiment, "avg_flesch_score", 0.0),

        # ── Market ─────────────────────────────────────────────────────
        "nifty_return_1d": market.nifty_return_1d,
        "nifty_return_5d": market.nifty_return_5d,
        "nifty_return_window": market.nifty_return_window,
        "nifty_sma_20": market.nifty_sma_20,
        "nifty_ema_12": market.nifty_ema_12,
        "nifty_ema_26": market.nifty_ema_26,
        "nifty_macd": market.nifty_macd,
        "nifty_above_sma": market.nifty_above_sma,
        "nifty_price_t1": market.nifty_price_t1,
        "vix_t1": market.vix_t1,
        "vix_avg_window": market.vix_avg_window,
        "vix_trend": market.vix_trend,
        "market_mood_score": market.market_mood_score,

        # ── Macro / FRED ───────────────────────────────────────────────
        "macro_available": bool(macro.get("available", False)),
        "macro_score":     float(macro.get("macro_score", 0.0) or 0.0),
        "macro_briefing":  str(macro.get("macro_briefing", "") or ""),
        "macro_risk_regime": regime.get("macro_risk_regime", "unknown"),
        "macro_rate_regime": regime.get("rate_regime", "unknown"),
        "macro_dollar_regime": regime.get("dollar_regime", "unknown"),
        # Individual indicators
        "fred_cboe_vix_t1":      snap.get("cboe_vix_t1"),
        "fred_us_10y_yield_t1":  snap.get("us_10y_yield_t1"),
        "fred_us_fed_funds_t1":  snap.get("us_fed_funds_t1"),
        "fred_inr_usd_t1":       snap.get("inr_usd_t1"),
        "fred_inr_usd_30d_chg":  snap.get("inr_usd_30d_change"),
        "fred_dxy_t1":           snap.get("dxy_t1"),
        "fred_dxy_30d_chg":      snap.get("dxy_30d_change"),
        "fred_oil_brent_t1":     snap.get("oil_brent_t1"),
        "fred_oil_brent_30d_chg": snap.get("oil_brent_30d_change"),
    }
    return row


def save_to_parquet(row: dict) -> None:
    FEATURE_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame([row])
    if FEATURE_STORE_PATH.exists():
        df_existing = pd.read_parquet(FEATURE_STORE_PATH)
        # Schema-tolerant concat: align columns so new fields don't crash old rows.
        all_cols = sorted(set(df_existing.columns) | set(df_new.columns))
        df_existing = df_existing.reindex(columns=all_cols)
        df_new = df_new.reindex(columns=all_cols)
        mask = ~(
            (df_existing["ipo_name"] == row["ipo_name"]) &
            (df_existing["listing_date"] == row["listing_date"])
        )
        df_combined = pd.concat([df_existing[mask], df_new], ignore_index=True)
    else:
        df_combined = df_new
    df_combined.to_parquet(FEATURE_STORE_PATH, index=False, engine="pyarrow")
    logger.info(f"Feature store updated: {FEATURE_STORE_PATH} ({len(df_combined)} total rows)")

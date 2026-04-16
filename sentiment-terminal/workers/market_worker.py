"""
market_worker.py  (v3 — bulletproofed)
──────────────────────────────────────
Fetches Nifty 50 + India VIX via yfinance with NaN-safe momentum features.
Note: India VIX data only starts ~2008-03 from yfinance, so 2010+ is fine.
"""

import logging
import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

from utils.time_utils import yfinance_date_range

logger = logging.getLogger(__name__)

NIFTY50_TICKER   = "^NSEI"
INDIA_VIX_TICKER = "^INDIAVIX"


@dataclass
class MarketFeatures:
    listing_date: str
    nifty_return_1d: Optional[float]
    nifty_return_5d: Optional[float]
    nifty_return_window: Optional[float]
    nifty_sma_20: Optional[float]
    nifty_ema_12: Optional[float]
    nifty_ema_26: Optional[float]
    nifty_macd: Optional[float]
    nifty_above_sma: Optional[int]
    nifty_price_t1: Optional[float]
    vix_t1: Optional[float]
    vix_avg_window: Optional[float]
    vix_trend: Optional[float]
    market_mood_score: float
    price_series: list[dict] = field(default_factory=list)


def _safe_float(x) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def _download(ticker: str, start: str, end: str) -> pd.DataFrame:
    df = yf.download(
        ticker, start=start, end=end,
        auto_adjust=True, progress=False, threads=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = _flatten_yf_columns(df)
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    df = df.dropna(how="all")
    return df


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _normalised_slope(series: pd.Series) -> Optional[float]:
    s = series.dropna()
    if len(s) < 2:
        return None
    x = np.arange(len(s))
    try:
        slope, _ = np.polyfit(x, s.values, 1)
    except (np.linalg.LinAlgError, ValueError):
        return None
    mean = float(s.mean())
    if mean == 0 or math.isnan(mean):
        return None
    return float(slope / mean)


def fetch_nifty(start: str, end: str) -> pd.DataFrame:
    df = _download(NIFTY50_TICKER, start, end)
    if df.empty or "Close" not in df.columns:
        raise ValueError(f"No Nifty 50 data for [{start}, {end})")
    logger.info(f"Nifty 50: {len(df)} trading days fetched")
    return df


def fetch_vix(start: str, end: str) -> pd.DataFrame:
    df = _download(INDIA_VIX_TICKER, start, end)
    if df.empty:
        logger.warning("India VIX unavailable for window — features will be None.")
    return df


def run(listing_date: str, lookback_days: int = 60) -> MarketFeatures:
    start, end = yfinance_date_range(listing_date, lookback_days)
    logger.info(f"[Market] Fetching from {start} to {end} (exclusive)")

    nifty = fetch_nifty(start, end)
    close = nifty["Close"].astype(float).dropna()

    if len(close) < 2:
        raise ValueError(f"Insufficient Nifty data ({len(close)} rows) for {listing_date}")

    daily_returns = close.pct_change()
    nifty_return_1d     = _safe_float(daily_returns.iloc[-1]) if len(daily_returns) >= 1 else None
    nifty_return_5d     = _safe_float((close.iloc[-1] / close.iloc[-6]) - 1) if len(close) >= 6 else None
    nifty_return_window = _safe_float((close.iloc[-1] / close.iloc[0])  - 1) if len(close) >= 2 else None

    sma_20 = close.rolling(window=20, min_periods=20).mean()
    ema_12 = _ema(close, 12)
    ema_26 = _ema(close, 26)

    nifty_sma_20 = _safe_float(sma_20.iloc[-1])
    nifty_ema_12 = _safe_float(ema_12.iloc[-1])
    nifty_ema_26 = _safe_float(ema_26.iloc[-1])
    nifty_macd   = _safe_float((ema_12 - ema_26).iloc[-1])
    nifty_price_t1 = _safe_float(close.iloc[-1])
    nifty_above_sma = (
        int(nifty_price_t1 > nifty_sma_20)
        if (nifty_price_t1 is not None and nifty_sma_20 is not None) else None
    )

    vix_df = fetch_vix(start, end)
    if not vix_df.empty and "Close" in vix_df.columns:
        vix_close = vix_df["Close"].astype(float).dropna()
        vix_t1    = _safe_float(vix_close.iloc[-1]) if len(vix_close) else None
        vix_avg   = _safe_float(vix_close.mean())   if len(vix_close) else None
        vix_trend = _normalised_slope(vix_close)
    else:
        vix_t1 = vix_avg = vix_trend = None

    momentum_signal = math.tanh((nifty_return_5d or 0.0) * 10)
    macd_signal = (math.tanh(nifty_macd / nifty_price_t1 * 1000)
                   if nifty_macd is not None and nifty_price_t1 else 0.0)
    sma_signal = 0.2 if nifty_above_sma == 1 else (-0.2 if nifty_above_sma == 0 else 0.0)

    if vix_t1 is None:    vix_signal = 0.0
    elif vix_t1 < 15:     vix_signal = 0.3
    elif vix_t1 > 25:     vix_signal = -0.3
    elif vix_t1 > 20:     vix_signal = -0.1
    else:                 vix_signal = 0.0

    market_mood_score = float(np.clip(
        0.35 * momentum_signal + 0.25 * macd_signal + 0.2 * sma_signal + 0.2 * vix_signal,
        -1.0, 1.0,
    ))

    # Raw price series for visuals (last 30 days)
    price_series = []
    series_subset = close.tail(30)
    for i, (dt, val) in enumerate(series_subset.items()):
        price_series.append({
            "label": f"T-{len(series_subset)-1-i}",
            "close": round(float(val), 2),
            "date": dt.strftime("%Y-%m-%d")
        })

    return MarketFeatures(
        listing_date=listing_date,
        nifty_return_1d=nifty_return_1d,
        nifty_return_5d=nifty_return_5d,
        nifty_return_window=nifty_return_window,
        nifty_sma_20=nifty_sma_20,
        nifty_ema_12=nifty_ema_12,
        nifty_ema_26=nifty_ema_26,
        nifty_macd=nifty_macd,
        nifty_above_sma=nifty_above_sma,
        nifty_price_t1=nifty_price_t1,
        vix_t1=vix_t1,
        vix_avg_window=vix_avg,
        vix_trend=vix_trend,
        market_mood_score=market_mood_score,
        price_series=price_series,
    )

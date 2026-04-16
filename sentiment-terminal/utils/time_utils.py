"""
time_utils.py  (v3 — leak-proof)
────────────────────────────────
Single source of truth for ALL timezone logic.

Rule: For a listing on Date T (IST), every data window is:
    start = T − N days at 00:00:00 IST
    end   = T − 1 day  at 23:59:59 IST   ← strict T-1 cutoff
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pytz

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc


def parse_listing_date(date_str: str) -> datetime:
    """Parse YYYY-MM-DD into IST-aware midnight datetime."""
    if not date_str or not isinstance(date_str, str):
        raise ValueError(f"listing_date must be YYYY-MM-DD string, got: {date_str!r}")
    try:
        naive = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid listing_date {date_str!r}; expected YYYY-MM-DD") from e
    return IST.localize(naive.replace(hour=0, minute=0, second=0, microsecond=0))


def get_window(listing_date_str: str, lookback_days: int = 30) -> tuple[datetime, datetime]:
    T = parse_listing_date(listing_date_str)
    window_end   = T - timedelta(seconds=1)            # 23:59:59 IST on T-1
    window_start = T - timedelta(days=lookback_days)   # 00:00:00 IST on T-N
    return window_start, window_end


def to_ist(dt: datetime, assume_utc_if_naive: bool = True) -> datetime:
    if dt.tzinfo is None:
        if assume_utc_if_naive:
            logger.warning("to_ist received naive datetime %s — assuming UTC", dt)
            dt = UTC.localize(dt)
        else:
            raise ValueError("to_ist requires timezone-aware datetime")
    return dt.astimezone(IST)


def is_before_cutoff(dt: datetime, cutoff: datetime) -> bool:
    # Inclusive of the cutoff instant. window_end is already T - 1 second
    # (23:59:59 IST on T-1), so <= keeps articles published at exactly the
    # cutoff second instead of dropping them.
    return to_ist(dt) <= cutoff


def window_to_utc(window_start: datetime, window_end: datetime) -> tuple[datetime, datetime]:
    return window_start.astimezone(UTC), window_end.astimezone(UTC)


def yfinance_date_range(listing_date_str: str, lookback_days: int = 30) -> tuple[str, str]:
    """yfinance uses [start, end) — passing T as `end` yields data through T-1."""
    T = parse_listing_date(listing_date_str)
    start = T - timedelta(days=lookback_days)
    end   = T
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

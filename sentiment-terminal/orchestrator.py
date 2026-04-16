"""
orchestrator.py  (v3 — batch-safe)
──────────────────────────────────
Single-IPO and batch entry points for the IPO Sentiment & Market Terminal.

CLI:
    python orchestrator.py --ipo "Zomato" --date "2021-07-23"
    python orchestrator.py --batch data/Initial_Public_Offering.xlsx
    python orchestrator.py --batch data/Initial_Public_Offering.xlsx --limit 5
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import Optional

import pandas as pd

# Load .env if present (no-op if dotenv not installed)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("orchestrator")

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

LOOKBACK_SENTIMENT = 30
LOOKBACK_MARKET    = 60

FAILURE_DIR = Path("storage/failures")


def run_pipeline(ipo_name: str, listing_date: str) -> dict:
    from workers import sentiment_worker, market_worker
    try:
        from workers import fred_worker
    except ImportError:
        fred_worker = None
    from utils.feature_aggregator import (
        compute_composite_score, to_feature_row, save_to_parquet,
        compute_visuals,
    )

    logger.info(f"═══ {ipo_name} | {listing_date} ═══")

    sentiment = sentiment_worker.run(
        ipo_name=ipo_name, listing_date=listing_date,
        news_api_key=NEWS_API_KEY, groq_api_key=GROQ_API_KEY,
        lookback_days=LOOKBACK_SENTIMENT,
    )
    logger.info(f"  Sentiment: {sentiment.article_count} arts | "
                f"src={sentiment.news_source} | "
                f"dominant={sentiment.dominant_sentiment} | "
                f"groq={sentiment.groq_score:+.2f} | "
                f"llama={getattr(sentiment, 'llama_score', 0.0):+.2f}")

    market = market_worker.run(listing_date=listing_date, lookback_days=LOOKBACK_MARKET)
    logger.info(f"  Market: Nifty T-1=₹{(market.nifty_price_t1 or 0):.0f} | "
                f"5d={(market.nifty_return_5d or 0)*100:+.2f}% | "
                f"VIX={(market.vix_t1 or 0):.1f} | mood={market.market_mood_score:+.2f}")

    # Macro snapshot from FRED
    macro_dict = {"available": False, "snapshot": {}, "macro_score": 0.0,
                  "macro_briefing": "", "regime": {}}
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
            if macro.available:
                logger.info(f"  Macro:  score={macro.macro_score:+.2f} | "
                            f"{macro.macro_briefing}")
            else:
                logger.info("  Macro:  unavailable (no FRED_API_KEY)")
        except Exception as e:
            logger.warning(f"  Macro:  FRED fetch failed: {e}")

    composite = compute_composite_score(
        sentiment, market,
        macro_score=macro_dict["macro_score"],
        macro_available=macro_dict["available"],
    )
    logger.info(f"  ★ Composite: {composite:+.4f}")

    # Compute visual series for persistence
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
    return row


def run_batch(
    master_path: str,
    limit: Optional[int] = None,
    sleep_between: float = 1.0,
    skip_existing: bool = True,
    name_col: str = "IPO_Name",
    date_col: str = "Date",
) -> dict:
    if str(master_path).lower().endswith(".csv"):
        df = pd.read_csv(master_path, low_memory=False)
    else:
        df = pd.read_excel(master_path, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]
    if name_col not in df.columns or date_col not in df.columns:
        raise ValueError(f"Spreadsheet must have columns '{name_col}' and '{date_col}'. "
                         f"Found: {list(df.columns)}")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, name_col])
    df = df.sort_values(date_col, ascending=False).reset_index(drop=True)
    if limit:
        df = df.head(limit)

    already: set[tuple[str, str]] = set()
    if skip_existing:
        store_path = Path("storage/features/ipo_features.parquet")
        if store_path.exists():
            existing = pd.read_parquet(store_path, columns=["ipo_name", "listing_date"])
            already = set(zip(existing["ipo_name"], existing["listing_date"]))
            logger.info(f"Skip-cache: {len(already)} IPOs already processed")

    run_id = uuid.uuid4().hex[:8]
    FAILURE_DIR.mkdir(parents=True, exist_ok=True)
    failure_path = FAILURE_DIR / f"{run_id}.jsonl"

    n_ok = n_fail = n_skip = 0
    t0 = time.time()
    for _, row in df.iterrows():
        name = str(row[name_col]).strip()
        date_str = row[date_col].strftime("%Y-%m-%d")
        if (name, date_str) in already:
            n_skip += 1
            continue

        try:
            run_pipeline(name, date_str)
            n_ok += 1
        except KeyboardInterrupt:
            logger.warning("Interrupted by user; persisting partial state.")
            break
        except Exception as e:
            n_fail += 1
            logger.error(f"  ✗ {name} @ {date_str} failed: {e}", exc_info=False)
            with failure_path.open("a") as f:
                f.write(json.dumps({
                    "ipo_name": name, "listing_date": date_str,
                    "error": repr(e), "ts": datetime.utcnow().isoformat(),
                }) + "\n")
        finally:
            time.sleep(sleep_between)

    elapsed = time.time() - t0
    summary = {
        "run_id": run_id, "total": len(df),
        "ok": n_ok, "failed": n_fail, "skipped": n_skip,
        "elapsed_sec": round(elapsed, 1),
        "failure_ledger": str(failure_path) if n_fail else None,
    }
    logger.info(f"═══ BATCH DONE: {summary} ═══")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IPO Sentiment Terminal")
    parser.add_argument("--ipo",   help='Single mode: company name e.g. "Zomato"')
    parser.add_argument("--date",  help="Single mode: YYYY-MM-DD listing date (IST)")
    parser.add_argument("--batch", help="Batch mode: path to IPO master .xlsx")
    parser.add_argument("--limit", type=int, default=None, help="Batch row cap")
    parser.add_argument("--sleep", type=float, default=1.0, help="Delay between IPOs (sec)")
    args = parser.parse_args()

    if args.batch:
        pprint(run_batch(args.batch, limit=args.limit, sleep_between=args.sleep))
    elif args.ipo and args.date:
        result = run_pipeline(args.ipo, args.date)
        pprint({k: v for k, v in result.items() if k != "groq_summary"})
        print(f"\nLLM Summary:\n{result.get('groq_summary', '')}")
    else:
        parser.error("Provide either --batch <xlsx> OR --ipo <name> --date <YYYY-MM-DD>")

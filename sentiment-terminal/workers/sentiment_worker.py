"""
sentiment_worker.py  (v5 — auth-free historical fetcher + tighter relevance + corpus-level FRES)
─────────────────────────────────────────────────────────────────────────────────────────────────
Fetches pre-IPO news with a multi-source fallback that requires NO auth:

  Listing year >= 2017  →  GDELT 2.0 DOC API   (rich, free, no key)
                           ↓ if 0 articles, fall through to ↓
  Listing year 2010-2016 →  Google News RSS    (before:/after: date ops)
                           ↓ if 0 articles, fall through to ↓
  Listing year < 2010   →  Returns "unavailable" (still scores via market+macro)

Key v5 fixes vs v4:
  1. No more BigQuery / GDELT 1.0 — Google News RSS works for all years >= 2010.
  2. Relevance filter now requires the company *name* (first 2 tokens) in
     the title — not just generic IPO keywords. Stops "HDFC AMC" matching
     "HDFC Bank" / "HDFC Life" / "HDFC Securities" articles.
  3. avg_flesch_score is computed once over the WHOLE concatenated corpus
     instead of per-headline (headlines are too short for textstat → 0.0).
  4. Groq + Llama are called whenever there is ANY corpus to summarize,
     even if the article count is small. They also get a market+macro
     fallback summary when news is genuinely unavailable, so older IPOs
     stop showing "No Llama summary available."
  5. Hardened RSS parsing & UTC handling.
"""

import logging
import re
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import requests
from transformers import pipeline
from groq import Groq

try:
    import feedparser
except ImportError:  # graceful: degrade if feedparser absent
    feedparser = None

try:
    import textstat
except ImportError:  # graceful: FRES will be 0.0 if textstat isn't installed
    textstat = None

from utils.time_utils import get_window, to_ist, is_before_cutoff, window_to_utc

logger = logging.getLogger(__name__)


# Keywords used for IPO-relevance filtering. An article must contain at
# least one of these (case-insensitive) in title+description to be kept.
IPO_KEYWORDS = {
    "ipo", "initial public offering", "listing", "subscribe", "subscription",
    "issue price", "grey market", "gmp", "oversubscribed", "allotment",
    "prospectus", "drhp", "rhp", "sebi", "public offer", "anchor",
    "price band", "lot size", "qib", "hni", "retail investor",
    "book build", "share sale", "share issue", "debut", "lists at",
    "lists on", "stock market debut",
}


# ── FinBERT singleton ────────────────────────────────────────────────────────
_finbert = None

def get_finbert():
    global _finbert
    if _finbert is None:
        logger.info("Loading FinBERT (ProsusAI/finbert) — first call only.")
        _finbert = pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
            top_k=None, truncation=True, max_length=512,
        )
    return _finbert


@dataclass
class ArticleSentiment:
    title: str
    published_at_ist: datetime
    source: str
    finbert_positive: float
    finbert_negative: float
    finbert_neutral: float
    finbert_label: str
    finbert_score: float
    flesch_score: float = 0.0   # Per-article FRES (often 0 for short headlines)


@dataclass
class SentimentFeatures:
    ipo_name: str
    listing_date: str
    article_count: int
    avg_positive: float
    avg_negative: float
    avg_neutral: float
    dominant_sentiment: str
    sentiment_momentum: float
    groq_summary: str
    groq_score: float
    llama_summary: str = ""    # Secondary LLM (LLaMA-3.1-8B)
    llama_score: float = 0.0   # Secondary LLM score
    news_source: str = "ok"   # "gdelt_v2" | "google_news" | "unavailable"
    articles_filtered_irrelevant: int = 0
    avg_flesch_score: float = 0.0
    articles: list[ArticleSentiment] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# 1. NEWS FETCH — TIERED DISPATCHER (auth-free)
# ═══════════════════════════════════════════════════════════════════════════

def _clean_company(name: str) -> str:
    """Strip common corporate suffixes for cleaner search queries."""
    cleaned = re.sub(
        r"\b(Limited|Ltd\.?|Pvt\.?|Private|Inc\.?|Corp\.?|Company|Co\.?)\b",
        "", name, flags=re.I,
    ).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


# Generic noise tokens stripped before deciding which words are "distinctive"
_STOP_NAME_TOKENS = {
    "limited", "ltd", "pvt", "private", "inc", "corp", "corporation",
    "company", "co", "of", "and", "the", "india", "indian", "international",
}


def _company_name_aliases(name: str) -> list[set[str]]:
    """
    Return one or more acceptable token-sets for company-name matching. An
    article title matches if it contains ALL tokens of ANY one alias-set.

    For multi-word brands we return both the full distinctive form AND the
    common acronym form, so e.g.

        "HDFC Asset Management Co. Ltd."  ->  [
            {"hdfc", "asset"},   # matches "HDFC Asset Management lists at premium"
            {"hdfc", "amc"},     # matches "HDFC AMC IPO oversubscribed 4x"
        ]

    But still rejects "HDFC Bank declares dividend" / "HDFC Life IPO opens"
    because neither alias-set is fully present.

    Single-word brands (Zomato, Aequs) just return one alias-set.
    """
    cleaned = _clean_company(name).lower()
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    tokens = [t for t in cleaned.split() if t]
    distinctive = [t for t in tokens
                   if t not in _STOP_NAME_TOKENS and len(t) >= 3]

    if not distinctive:
        # Fallback: use whole cleaned name (or original if all stopwords)
        fallback = cleaned.replace(" ", "") or name.lower()
        return [{fallback}]

    aliases: list[set[str]] = [set(distinctive[:2])]

    # If the brand has 3+ tokens, also accept "{brand} {acronym-of-rest}".
    # e.g. "HDFC Asset Management Co" -> "HDFC AMC".
    after_first = tokens[1:]
    if len(after_first) >= 2:
        acronym = "".join(t[0] for t in after_first[:4] if t)
        if 2 <= len(acronym) <= 5 and acronym != distinctive[0]:
            aliases.append({distinctive[0], acronym})

    return aliases


def _title_mentions_company(text: str, aliases: list[set[str]]) -> bool:
    """
    True iff the text contains ALL tokens from ANY one alias-set. Empty
    aliases never filter (defensive default).
    """
    if not aliases:
        return True
    t = text.lower()
    return any(all(tok in t for tok in alias) for alias in aliases)


def fetch_news(
    company_name: str,
    window_start: datetime,
    window_end: datetime,
    news_api_key: str = "",   # legacy compat, unused
    page_size: int = 100,
) -> tuple[list[dict], str]:
    """
    Returns (articles, source_tag) where source_tag is one of:
      "gdelt_v2"     - GDELT 2.0 (>=2017, primary)
      "google_news"  - Google News RSS (any year >=2010, fallback)
      "unavailable"  - both sources returned nothing
    """
    listing_year = window_end.year
    all_articles: list[dict] = []
    source_used = "unavailable"

    # 1. GDELT v2 - for 2017+ (the era it actually covers well)
    if listing_year >= 2017:
        try:
            gdelt_articles = _fetch_gdelt_v2(
                company_name, window_start, window_end, page_size
            )
            if gdelt_articles:
                all_articles.extend(gdelt_articles)
                source_used = "gdelt_v2"
        except Exception as e:
            logger.warning(f"GDELT v2 failed: {e}")

    # 2. Google News RSS - fallback for any year, primary for 2010-2016
    if not all_articles:
        try:
            gn_articles = _fetch_google_news_rss(
                company_name, window_start, window_end, page_size
            )
            if gn_articles:
                all_articles.extend(gn_articles)
                source_used = "google_news"
        except Exception as e:
            logger.warning(f"Google News RSS failed: {e}")

    if not all_articles:
        logger.info(
            f"No articles found for '{company_name}' in window "
            f"{window_start.date()} -> {window_end.date()} via any source."
        )
        return [], "unavailable"

    return all_articles, source_used


# --- GDELT 2.0 DOC API (>=2017) ---
GDELT_V2_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_V2_DT = "%Y%m%d%H%M%S"


def _gdelt_v2_request(params: dict, max_retries: int = 4, base_delay: float = 1.5) -> dict:
    last_err = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(GDELT_V2_URL, params=params, timeout=20)
            if resp.status_code == 200:
                if resp.headers.get("content-type", "").startswith("application/json"):
                    return resp.json()
                if not resp.text.strip():
                    return {"articles": []}
                logger.warning(f"GDELT v2 non-JSON 200 (truncated): {resp.text[:120]}")
                return {"articles": []}
            if resp.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {resp.status_code}"
                delay = base_delay * (2 ** attempt)
                logger.warning(f"GDELT v2 {last_err}; retry in {delay:.1f}s "
                               f"({attempt+1}/{max_retries})")
                time.sleep(delay)
                continue
            logger.error(f"GDELT v2 non-retryable {resp.status_code}: {resp.text[:200]}")
            return {"articles": []}
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = repr(e)
            delay = base_delay * (2 ** attempt)
            logger.warning(f"GDELT v2 network error: {last_err}; retry in {delay:.1f}s")
            time.sleep(delay)
    logger.error(f"GDELT v2 failed after {max_retries} attempts: {last_err}")
    return {"articles": []}


def _fetch_gdelt_v2(company_name, window_start, window_end, page_size) -> list[dict]:
    utc_start, utc_end = window_to_utc(window_start, window_end)
    clean = _clean_company(company_name)
    params = {
        "query": f'"{clean}" IPO sourcelang:eng sourcecountry:IN',
        "mode": "ArtList",
        "format": "json",
        "maxrecords": min(page_size, 250),
        "sort": "DateDesc",
        "startdatetime": utc_start.strftime(GDELT_V2_DT),
        "enddatetime":   utc_end.strftime(GDELT_V2_DT),
    }
    payload = _gdelt_v2_request(params)
    raw = payload.get("articles", []) or []

    out = []
    for a in raw:
        try:
            dt = datetime.strptime(a.get("seendate", ""), "%Y%m%dT%H%M%SZ")
        except (ValueError, TypeError):
            continue
        out.append({
            "title":       a.get("title", "") or "",
            "description": a.get("title", "") or "",
            "publishedAt": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source":      {"name": a.get("domain", "unknown")},
            "url":         a.get("url", ""),
        })
    logger.info(f"GDELT v2: {len(out)} articles for '{company_name}' "
                f"({utc_start.date()} -> {utc_end.date()} UTC)")
    return out


# --- Google News RSS (auth-free, any year >= ~2010) ---
GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"


def _fetch_google_news_rss(
    company_name: str,
    window_start: datetime,
    window_end: datetime,
    page_size: int,
) -> list[dict]:
    """
    Use Google News RSS with `after:YYYY-MM-DD before:YYYY-MM-DD` operators
    to retrieve historical IPO coverage. Works for any year reliably back
    to about 2008-2010 for major Indian publications.

    No API key, no auth, no rate limit (within reason).
    """
    if feedparser is None:
        logger.warning("feedparser not installed -- install with `pip install feedparser`")
        return []

    clean = _clean_company(company_name)
    after  = window_start.strftime("%Y-%m-%d")
    before = (window_end + timedelta(days=1)).strftime("%Y-%m-%d")  # before is exclusive

    # Two queries combined for better recall: one strict (with quotes), one loose
    queries = [
        f'"{clean}" IPO after:{after} before:{before}',
        f'{clean} IPO listing after:{after} before:{before}',
    ]

    seen_titles = set()
    out: list[dict] = []

    for q in queries:
        url = (
            f"{GOOGLE_NEWS_RSS_URL}"
            f"?q={urllib.parse.quote(q)}"
            f"&hl=en-IN&gl=IN&ceid=IN:en"
        )
        try:
            resp = requests.get(
                url,
                timeout=20,
                headers={"User-Agent": "Mozilla/5.0 (compatible; IPO-Terminal/5.0)"},
            )
            if resp.status_code != 200:
                logger.warning(f"Google News RSS HTTP {resp.status_code} for query: {q[:60]}")
                continue
            feed = feedparser.parse(resp.content)
        except Exception as e:
            logger.warning(f"Google News RSS fetch failed: {e}")
            continue

        for entry in feed.entries[:page_size]:
            title = (entry.get("title") or "").strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)

            # Parse pubDate
            pub_dt = None
            for time_field in ("published_parsed", "updated_parsed"):
                tp = entry.get(time_field)
                if tp:
                    try:
                        pub_dt = datetime(*tp[:6])
                        break
                    except (TypeError, ValueError):
                        continue
            if pub_dt is None:
                continue

            # Source extraction (Google News titles often look like "Title - Source")
            source_name = "unknown"
            src = entry.get("source")
            if src and isinstance(src, dict):
                source_name = src.get("title", "unknown")
            elif " - " in title:
                parts = title.rsplit(" - ", 1)
                if len(parts) == 2 and len(parts[1]) < 60:
                    source_name = parts[1].strip()
                    title = parts[0].strip()

            description = (entry.get("summary") or entry.get("description") or "").strip()
            description = re.sub(r"<[^>]+>", " ", description)
            description = re.sub(r"\s+", " ", description).strip()

            out.append({
                "title":       title,
                "description": description if description else title,
                "publishedAt": pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source":      {"name": source_name},
                "url":         entry.get("link", ""),
            })

        time.sleep(0.5)  # be polite between queries

    logger.info(f"Google News RSS: {len(out)} unique articles for '{company_name}' "
                f"({after} -> {before})")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# 2. FINBERT
# ═══════════════════════════════════════════════════════════════════════════

def run_finbert(text: str) -> dict:
    if not text or not text.strip():
        return {"positive": 0.0, "negative": 0.0, "neutral": 1.0}
    try:
        results = get_finbert()(text[:512])
        scores = {r["label"].lower(): r["score"] for r in results[0]}
        return {
            "positive": float(scores.get("positive", 0.0)),
            "negative": float(scores.get("negative", 0.0)),
            "neutral":  float(scores.get("neutral",  0.0)),
        }
    except Exception as e:
        logger.warning(f"FinBERT failure: {e}")
        return {"positive": 0.0, "negative": 0.0, "neutral": 1.0}


# ═══════════════════════════════════════════════════════════════════════════
# 3. GROQ aggregate
# ═══════════════════════════════════════════════════════════════════════════

def groq_aggregate_sentiment(
    articles_text: list[str],
    company_name: str,
    listing_date: str,
    groq_api_key: str,
    max_retries: int = 3,
) -> tuple[str, float]:
    if not articles_text:
        return ("No pre-listing articles available.", 0.0)

    client = Groq(api_key=groq_api_key)
    corpus = "\n\n".join(
        [f"[Article {i+1}]: {txt[:400]}" for i, txt in enumerate(articles_text[:20])]
    )
    system_prompt = (
        "You are a financial analyst specialising in Indian equity markets. "
        "You will be given a collection of news articles published BEFORE an IPO listing. "
        "Assess market sentiment about this company's IPO strictly from the provided "
        "articles. Output ONLY a JSON object with two keys: "
        '"summary" (2-3 sentences) and '
        '"score" (float in [-1.0, 1.0]: -1=very bearish, 0=neutral, 1=very bullish).'
    )
    user_prompt = (
        f"Company: {company_name}\nIPO Listing Date: {listing_date}\n"
        f"Pre-listing news articles:\n\n{corpus}"
    )

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.1, max_tokens=300,
                response_format={"type": "json_object"},
            )
            import json
            raw = response.choices[0].message.content
            raw = re.sub(r"```(?:json)?|```", "", raw).strip()
            parsed = json.loads(raw)
            summary = str(parsed.get("summary", ""))[:600]
            score = float(parsed.get("score", 0.0))
            return summary, max(-1.0, min(1.0, score))
        except Exception as e:
            delay = 2 ** attempt
            logger.warning(f"Groq attempt {attempt+1}/{max_retries} failed: {e}; "
                           f"retry in {delay}s")
            time.sleep(delay)
    return ("LLM aggregation unavailable (all retries exhausted).", 0.0)


def llama8b_aggregate_sentiment(
    articles_text: list[str],
    company_name: str,
    listing_date: str,
    groq_api_key: str,
    max_retries: int = 3,
) -> tuple[str, float]:
    """Secondary LLM call using LLaMA-3.1-8B for cross-validation."""
    if not articles_text:
        return ("No pre-listing articles available.", 0.0)

    client = Groq(api_key=groq_api_key)
    corpus = "\n\n".join(
        [f"[Article {i+1}]: {txt[:300]}" for i, txt in enumerate(articles_text[:15])]
    )
    system_prompt = (
        "You are a financial analyst. Given news articles published before an IPO listing, "
        "assess the overall market sentiment. Output ONLY a JSON object with two keys: "
        '"summary" (1-2 sentences) and '
        '"score" (float in [-1.0, 1.0]: -1=very bearish, 0=neutral, 1=very bullish).'
    )
    user_prompt = (
        f"Company: {company_name}\nIPO Listing Date: {listing_date}\n"
        f"Pre-listing articles:\n\n{corpus}"
    )

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.1, max_tokens=200,
                response_format={"type": "json_object"},
            )
            import json
            raw = response.choices[0].message.content
            raw = re.sub(r"```(?:json)?|```", "", raw).strip()
            parsed = json.loads(raw)
            summary = str(parsed.get("summary", ""))[:400]
            score = float(parsed.get("score", 0.0))
            return summary, max(-1.0, min(1.0, score))
        except Exception as e:
            delay = 2 ** attempt
            logger.warning(f"Llama-8B attempt {attempt+1}/{max_retries} failed: {e}; "
                           f"retry in {delay}s")
            time.sleep(delay)
    return ("Llama-8B aggregation unavailable.", 0.0)


def llm_market_only_summary(
    company_name: str,
    listing_date: str,
    groq_api_key: str,
    market_context: str,
    model: str = "llama-3.3-70b-versatile",
    max_retries: int = 2,
) -> tuple[str, float]:
    """
    For pre-2013 (or news-unavailable) IPOs: ask the LLM to comment based
    purely on Nifty/VIX context so the UI doesn't show empty boxes. Returns
    (summary, 0.0) - score is intentionally 0 because there's no news signal.
    """
    if not groq_api_key or not market_context:
        return ("News data unavailable for this listing window. "
                "Composite uses market+macro signals only.", 0.0)

    client = Groq(api_key=groq_api_key)
    system_prompt = (
        "You are a financial analyst. No pre-IPO news is available for this older "
        "listing, but you have the broader market context. Write a brief 2-sentence "
        "note describing the market backdrop the IPO listed into. Do NOT speculate "
        'about company-specific demand. Output ONLY a JSON object with key "summary".'
    )
    user_prompt = (
        f"Company: {company_name}\nListing Date: {listing_date}\n"
        f"Market context:\n{market_context}"
    )
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.2, max_tokens=180,
                response_format={"type": "json_object"},
            )
            import json
            raw = response.choices[0].message.content
            raw = re.sub(r"```(?:json)?|```", "", raw).strip()
            parsed = json.loads(raw)
            summary = str(parsed.get("summary", ""))[:400]
            if summary:
                return summary, 0.0
        except Exception as e:
            logger.warning(f"market-only LLM attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return ("News data unavailable; composite uses market+macro signals only.", 0.0)


# ═══════════════════════════════════════════════════════════════════════════
# 4. Sentiment momentum
# ═══════════════════════════════════════════════════════════════════════════

def compute_sentiment_momentum(articles: list[ArticleSentiment]) -> float:
    if len(articles) < 2:
        return 0.0
    import numpy as np
    sorted_articles = sorted(articles, key=lambda a: a.published_at_ist)
    timestamps = [a.published_at_ist.timestamp() for a in sorted_articles]
    positives = [a.finbert_positive for a in sorted_articles]
    t_min, t_max = min(timestamps), max(timestamps)
    if t_max == t_min:
        return 0.0
    t_norm = [(t - t_min) / (t_max - t_min) for t in timestamps]
    slope, _ = np.polyfit(t_norm, positives, 1)
    return float(slope)


# ═══════════════════════════════════════════════════════════════════════════
# 5. Corpus-level FRES helper
# ═══════════════════════════════════════════════════════════════════════════

def _compute_corpus_flesch(texts: list[str]) -> float:
    """
    Compute Flesch Reading Ease over the *concatenated* corpus.

    Per-headline FRES is almost always 0 because individual titles are too
    short for the formula to produce a meaningful value (it expects multiple
    sentences with 100+ words to be reliable). Concatenating the whole corpus
    gives textstat enough material to score sensibly.
    """
    if textstat is None or not texts:
        return 0.0
    blob_parts = []
    for t in texts:
        if not t:
            continue
        cleaned = t.strip().rstrip(".!?")
        if cleaned:
            blob_parts.append(cleaned + ".")
    blob = " ".join(blob_parts)
    if len(blob.split()) < 30:
        # Below ~30 words FRES is unreliable; return a neutral baseline of 60.
        return 60.0
    try:
        score = float(textstat.flesch_reading_ease(blob))
        # Clamp insane values (FRES can technically go negative or >100)
        return max(0.0, min(100.0, score))
    except Exception as e:
        logger.warning(f"textstat failed on corpus: {e}")
        return 60.0


# ═══════════════════════════════════════════════════════════════════════════
# 6. MAIN ENTRY
# ═══════════════════════════════════════════════════════════════════════════

def run(
    ipo_name: str,
    listing_date: str,
    news_api_key: str = "",
    groq_api_key: str = "",
    lookback_days: int = 30,
) -> SentimentFeatures:
    window_start, window_end = get_window(listing_date, lookback_days)
    logger.info(f"[Sentiment] Window: {window_start:%Y-%m-%d %H:%M %Z} -> "
                f"{window_end:%Y-%m-%d %H:%M %Z}")

    raw_articles, source_tag = fetch_news(ipo_name, window_start, window_end, news_api_key)
    name_aliases = _company_name_aliases(ipo_name)
    logger.info(f"[Sentiment] Company aliases for relevance check: {name_aliases}")

    processed: list[ArticleSentiment] = []
    n_before_cutoff = 0
    n_irrelevant = 0
    n_wrong_company = 0
    for art in raw_articles:
        pub_str = art.get("publishedAt", "")
        if not pub_str:
            continue
        try:
            pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        if not is_before_cutoff(pub_dt, window_end):
            continue
        n_before_cutoff += 1

        title_text = art.get("title", "") or ""
        desc_text = art.get("description", "") or ""
        text = f"{title_text}. {desc_text}".strip(". ")
        if not text:
            continue

        text_lower = text.lower()

        # --- STRICT relevance filter (v5) ---
        # Both conditions must be met:
        #   (a) The TITLE mentions the actual company (not just IPO keywords)
        #   (b) The text contains at least one IPO-domain keyword
        if not _title_mentions_company(title_text, name_aliases):
            n_wrong_company += 1
            logger.debug(
                f"Dropping wrong-company article ({name_aliases} not in title): "
                f"{title_text[:80]}"
            )
            continue

        if not any(kw in text_lower for kw in IPO_KEYWORDS):
            n_irrelevant += 1
            logger.debug(
                f"Dropping irrelevant article (no IPO keywords): {title_text[:80]}"
            )
            continue

        fb = run_finbert(text)
        dominant = max(fb, key=fb.get)

        flesch = 0.0
        if textstat is not None:
            try:
                flesch = float(textstat.flesch_reading_ease(text))
            except Exception:
                flesch = 0.0

        processed.append(ArticleSentiment(
            title=title_text,
            published_at_ist=to_ist(pub_dt),
            source=art.get("source", {}).get("name", "unknown"),
            finbert_positive=fb["positive"],
            finbert_negative=fb["negative"],
            finbert_neutral=fb["neutral"],
            finbert_label=dominant,
            finbert_score=fb[dominant],
            flesch_score=flesch,
        ))

    logger.info(
        f"[Sentiment] {ipo_name}: fetched={len(raw_articles)} raw, "
        f"{n_before_cutoff} passed cutoff, {n_wrong_company} wrong company, "
        f"{n_irrelevant} no IPO keywords, {len(processed)} kept "
        f"(source={source_tag})"
    )

    # --- Genuinely no usable news (older IPOs, dead names, future IPOs) ---
    if not processed:
        market_only_groq, _ = (
            llm_market_only_summary(
                ipo_name, listing_date, groq_api_key,
                market_context=(
                    f"This IPO listed in {window_end.year}. No pre-listing news "
                    f"could be retrieved from any free source for this window. "
                    f"Score is computed from market and macro signals only."
                ),
                model="llama-3.3-70b-versatile",
            ) if groq_api_key else
            ("News data unavailable for this listing window. Composite uses "
             "market+macro signals only.", 0.0)
        )
        market_only_llama, _ = (
            llm_market_only_summary(
                ipo_name, listing_date, groq_api_key,
                market_context=(
                    f"No pre-listing news available for {ipo_name} ({window_end.year}). "
                    f"Composite score relies on Nifty/VIX market context."
                ),
                model="llama-3.1-8b-instant",
            ) if groq_api_key else
            ("News data unavailable. Score derived from market context only.", 0.0)
        )
        return SentimentFeatures(
            ipo_name=ipo_name,
            listing_date=listing_date,
            article_count=0,
            avg_positive=0.0, avg_negative=0.0, avg_neutral=0.0,
            dominant_sentiment="neutral",
            sentiment_momentum=0.0,
            groq_summary=market_only_groq,
            groq_score=0.0,
            llama_summary=market_only_llama,
            llama_score=0.0,
            news_source="unavailable",
            articles_filtered_irrelevant=n_irrelevant + n_wrong_company,
            avg_flesch_score=0.0,
        )

    n = len(processed)
    avg_pos = sum(a.finbert_positive for a in processed) / n
    avg_neg = sum(a.finbert_negative for a in processed) / n
    avg_neu = sum(a.finbert_neutral  for a in processed) / n

    # v5 fix: corpus-level FRES instead of per-article average
    avg_flesch = _compute_corpus_flesch([a.title for a in processed])

    dist = {"positive": avg_pos, "negative": avg_neg, "neutral": avg_neu}
    dominant = max(dist, key=dist.get)

    titles = [a.title for a in processed]
    groq_summary, groq_score = (
        groq_aggregate_sentiment(titles, ipo_name, listing_date, groq_api_key)
        if groq_api_key else ("Groq disabled - no API key.", 0.0)
    )
    llama_summary, llama_score = (
        llama8b_aggregate_sentiment(titles, ipo_name, listing_date, groq_api_key)
        if groq_api_key else ("Llama disabled - no API key.", 0.0)
    )

    return SentimentFeatures(
        ipo_name=ipo_name,
        listing_date=listing_date,
        article_count=n,
        avg_positive=avg_pos,
        avg_negative=avg_neg,
        avg_neutral=avg_neu,
        dominant_sentiment=dominant,
        sentiment_momentum=compute_sentiment_momentum(processed),
        groq_summary=groq_summary,
        groq_score=groq_score,
        llama_summary=llama_summary,
        llama_score=llama_score,
        news_source=source_tag,
        articles_filtered_irrelevant=n_irrelevant + n_wrong_company,
        avg_flesch_score=avg_flesch,
        articles=processed,
    )

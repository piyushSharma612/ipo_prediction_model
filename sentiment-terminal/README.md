# IPO Sentiment & Market Context Terminal

> **v5** — auth-free historical news (Google News RSS replaces BigQuery),
> stricter relevance filter, corpus-level FRES, always-on LLM summaries,
> inline date picker for live analysis. See `CHANGES_v5.md` for details.

End-to-end pipeline + dashboard for Indian IPOs (2010 → present), with strict
T-1 IST cutoff to prevent look-ahead bias.

## Architecture

```
data/Initial_Public_Offering.xlsx          ← YOUR master spreadsheet
              │
              ▼
orchestrator.py ─── workers/sentiment_worker.py ──┐
              └─── workers/market_worker.py ──────┤
                                                  ▼
                              utils/feature_aggregator.py
                                                  │
                                                  ▼
                       storage/features/ipo_features.parquet
                                                  │
                                                  ▼
                                              api.py (FastAPI :8000)
                                                  │
                                                  ▼
                              frontend/ (React + Vite :5173)
```

## News-source tiering (handles 2010+ IPOs)

| Listing year | Source | Setup needed |
|---|---|---|
| ≥ 2017 | GDELT 2.0 DOC API | None (free, no key) |
| 2013-2016 | GDELT 1.0 GKG via BigQuery | Google Cloud project + auth (see below) |
| < 2013 | None — `news_source="unavailable"` | The score becomes market-only |

## Setup

### 1. Python backend

```bash
python -m venv venv
source venv/bin/activate                  # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env and add your GROQ_API_KEY (free at console.groq.com)
```

### 2. Drop your master spreadsheet into `data/`

Place your file at `data/Initial_Public_Offering.xlsx`. Required columns:

- `IPO_Name` (string)
- `Date` (parseable date — listing date)

Optional but used by the UI fundamentals panel:

- `Issue_Size(crores)`, `QIB`, `HNI`, `RII`, `Total`
- `Offer Price`, `List Price`, `Listing Gain`
- `CMP(BSE)`, `CMP(NSE)`, `Current Gains`

If your column names differ, edit `IPO_MASTER_PATH` handling in
`api.py:_load_ipo_index()`.

### 3. (Only if you have 2013-2016 IPOs) BigQuery for GDELT 1.0

```bash
# Install the SDK (already in requirements.txt)
pip install google-cloud-bigquery

# Install the gcloud CLI, then authenticate ONCE:
gcloud auth application-default login

# Verify:
python -c "from google.cloud import bigquery; print(bigquery.Client().project)"
```

The free tier gives 1 TB query data/month. Each IPO query scans ~50-200 MB
of the GDELT GKG, so you can run thousands of queries at zero cost.

If you don't want to set up BigQuery, just skip it — 2013-2016 IPOs will
emit `news_source="unavailable"` and use market-only scoring (same as pre-2013).

### 4. Frontend

```bash
cd frontend
npm install
npm run dev
```

## Running

### Process a single IPO (smoke test)

```bash
python orchestrator.py --ipo "Zomato" --date "2021-07-23"
```

Output: feature row printed + appended to `storage/features/ipo_features.parquet`.

### Process your entire spreadsheet (overnight job)

```bash
# Test with 5 first
python orchestrator.py --batch data/Initial_Public_Offering.xlsx --limit 5 --sleep 2

# Full run
python orchestrator.py --batch data/Initial_Public_Offering.xlsx --sleep 1.5
```

For ~500 IPOs expect 2-4 hours (FinBERT inference + Groq rate limits).
Failures are logged to `storage/failures/<runid>.jsonl` for re-processing.

### Start the API

```bash
uvicorn api:app --reload --port 8000
```

Test:
```bash
curl http://localhost:8000/api/health
curl "http://localhost:8000/api/sentiment/Zomato?listing_date=2021-07-23"
curl "http://localhost:8000/api/sentiment/Apple?listing_date=1980-12-12"   # → 404
```

### Start the frontend

```bash
cd frontend && npm run dev
```

Open http://localhost:5173

## Day-to-day workflow

1. **Once**: run the batch orchestrator overnight to fill `ipo_features.parquet`.
2. **Each session**: `uvicorn api:app --reload` + `npm run dev`. The API
   serves cached features instantly — no re-running the pipeline.
3. **New IPO**: `python orchestrator.py --ipo "NewCo" --date "2026-05-15"` —
   parquet appended, UI sees it on next request.
4. **For ML training**: read `storage/features/ipo_features.parquet` directly,
   drop `groq_summary` + `articles` columns, feed to XGBoost.

## First-run gotcha

FinBERT downloads ~440 MB from HuggingFace on first call. Pre-pull it:

```bash
python -c "from transformers import pipeline; pipeline('text-classification', model='ProsusAI/finbert')"
```

## Endpoints

| Endpoint | Returns |
|---|---|
| `GET /api/health` | Status + master-loaded flag |
| `GET /api/search?q=zoma&limit=10` | Dropdown autocomplete |
| `GET /api/sentiment/{ipo}?listing_date=YYYY-MM-DD` | Full feature payload (404 if not in master) |
| `GET /api/ipo/{ipo}/fundamentals?listing_date=...` | Subscription numbers from master |
| `GET /api/ml/feature_vector/{ipo}?listing_date=...` | Flat numeric vector for XGBoost |

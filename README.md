# Mobius Web Scraper

Standalone web scraper service for Mobius. Scrapes URLs (single page or tree), downloads documents to GCS, and provides sync review endpoint for Chat.

## Setup

```bash
cd mobius-skills/web-scraper
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
# Edit .env: REDIS_URL, GCS_BUCKET
```

## Run

- **API** (port 8002): `uvicorn app.main:app --host 0.0.0.0 --port 8002`
- **Worker**: `./mscrapew` (consumes from Redis)

Or use `mstart` from Mobius root (starts both if .venv exists).

## Endpoints

- `POST /scrape` – Async job: `{ url, mode, include_content?, include_summary? }` → `{ job_id }`
- `GET /scrape/{job_id}` – Poll status; returns `documents`, `pages` (raw content), `summary` (if requested)
- `GET /scrape/{job_id}/stream` – SSE stream of page content as it is scraped
- `POST /scrape/review` – Sync: `{ url, include_summary? }` → `{ text, summary? }`

## Callers

- **RAG frontend**: "Scrape from URL" card → POST /scrape, poll, "Add to RAG" via RAG import-from-gcs
- **Chat**: Detects URLs in message → POST /scrape/review → uses text as context

"""FastAPI app: POST /scrape, GET /scrape/{job_id}, GET /scrape/{job_id}/stream, POST /scrape/review."""
import json
import logging
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from app.config import (
    REDIS_URL,
    SCRAPER_REQUEST_KEY,
    SCRAPER_RESPONSE_KEY_PREFIX,
    SCRAPER_RESPONSE_TTL_SECONDS,
    SCRAPER_STREAM_CHANNEL_PREFIX,
)
from app.models import (
    ScrapeRequest,
    ScrapeReviewRequest,
    ScrapeReviewResponse,
    ScrapeJobResponse,
    ScrapeStatusResponse,
    ScrapedDocument,
    ScrapedPage,
)
from app.services.scraper import review_page
from app.services.summary import summarize_content

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Mobius Web Scraper", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _get_redis():
    import redis
    return redis.from_url(REDIS_URL, decode_responses=True)


@app.get("/health")
def health():
    """Health check: API is up only if Redis is reachable (needed for scrape queue)."""
    try:
        r = _get_redis()
        r.ping()
        return {"ok": True, "service": "mobius-web-scraper", "redis": True}
    except Exception as e:
        logger.warning("Health check failed (Redis): %s", e)
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {e}")


@app.post("/scrape", response_model=ScrapeJobResponse)
def create_scrape_job(req: ScrapeRequest):
    """Enqueue scrape job; returns job_id. Worker processes asynchronously."""
    job_id = str(uuid.uuid4())
    payload = {
        "job_id": job_id,
        "url": req.url,
        "mode": req.mode,
        "max_depth": req.max_depth,
        "max_pages": req.max_pages,
        "scope_mode": req.scope_mode,
        "document_types": req.document_types,
        "include_content": req.include_content,
        "include_summary": req.include_summary,
    }
    try:
        r = _get_redis()
        r.lpush(SCRAPER_REQUEST_KEY, json.dumps(payload))
        # Seed initial status so GET returns something before worker starts
        r.set(
            SCRAPER_RESPONSE_KEY_PREFIX + job_id,
            json.dumps({
                "job_id": job_id,
                "status": "pending",
                "documents": [],
                "pages": [],
                "summary": None,
                "error": None,
                "pages_scraped": 0,
            }),
            ex=SCRAPER_RESPONSE_TTL_SECONDS,
        )
    except Exception as e:
        logger.exception("Redis enqueue failed: %s", e)
        raise HTTPException(status_code=503, detail="Queue unavailable")
    return ScrapeJobResponse(job_id=job_id)


@app.get("/scrape/{job_id}", response_model=ScrapeStatusResponse)
def get_scrape_status(job_id: str):
    """Poll scrape job status and results."""
    try:
        r = _get_redis()
        raw = r.get(SCRAPER_RESPONSE_KEY_PREFIX + job_id)
    except Exception as e:
        logger.exception("Redis get failed: %s", e)
        raise HTTPException(status_code=503, detail="Storage unavailable")
    if raw is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    data = json.loads(raw)
    return ScrapeStatusResponse(
        job_id=data["job_id"],
        status=data["status"],
        documents=[ScrapedDocument(**d) for d in data.get("documents", [])],
        pages=[ScrapedPage(**p) for p in data.get("pages", [])],
        summary=data.get("summary"),
        error=data.get("error"),
        pages_scraped=data.get("pages_scraped", 0),
    )


def _stream_scrape_content(job_id: str):
    """Generator that yields SSE events for scraped page content."""
    import redis

    r = redis.from_url(REDIS_URL, decode_responses=True)
    channel = SCRAPER_STREAM_CHANNEL_PREFIX + job_id
    pubsub = r.pubsub()
    pubsub.subscribe(channel)

    # If job already completed, stream pages from stored result
    raw = r.get(SCRAPER_RESPONSE_KEY_PREFIX + job_id)
    if raw:
        data = json.loads(raw)
        if data.get("status") in ("completed", "failed"):
            for p in data.get("pages", []):
                yield f"data: {json.dumps({'url': p['url'], 'text': p['text']})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
            pubsub.unsubscribe(channel)
            return

    # Listen for real-time updates
    try:
        for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                payload = json.loads(message["data"])
                if payload.get("done"):
                    break
                yield f"data: {message['data']}\n\n"
            except (json.JSONDecodeError, TypeError):
                pass
    finally:
        pubsub.unsubscribe(channel)


@app.get("/scrape/{job_id}/stream")
def stream_scrape_content(job_id: str):
    """SSE stream of scraped page content (url, text) as it becomes available."""
    # Verify job exists
    r = _get_redis()
    raw = r.get(SCRAPER_RESPONSE_KEY_PREFIX + job_id)
    if raw is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    return StreamingResponse(
        _stream_scrape_content(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/scrape/review", response_model=ScrapeReviewResponse)
async def scrape_review(req: ScrapeReviewRequest):
    """Sync: fetch single page, extract text, optionally summarize."""
    try:
        text = await review_page(req.url)
    except Exception as e:
        logger.warning("Review failed for %s: %s", req.url, e)
        raise HTTPException(status_code=502, detail=f"Failed to fetch page: {e}")
    summary = summarize_content(text) if req.include_summary and text else None
    return ScrapeReviewResponse(text=text, url=req.url, summary=summary)

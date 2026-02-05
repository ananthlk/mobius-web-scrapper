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
    SCRAPER_EVENTS_KEY_PREFIX,
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


def _resolve_scrape_options(req: ScrapeRequest) -> dict:
    """Resolve request to worker payload with backward compat for include_content/include_summary."""
    mode = "simple" if req.mode == "regular" else req.mode
    content_mode = req.content_mode
    if req.include_content is not None:
        content_mode = "text" if req.include_content else "none"
    summarize = req.summarize
    if req.include_summary is not None:
        summarize = req.include_summary
    return {
        "mode": mode,
        "download_documents": req.download_documents,
        "content_mode": content_mode,
        "summarize": summarize,
    }


@app.post("/scrape", response_model=ScrapeJobResponse)
def create_scrape_job(req: ScrapeRequest):
    """Enqueue scrape job; returns job_id. Worker processes asynchronously."""
    job_id = str(uuid.uuid4())
    opts = _resolve_scrape_options(req)
    payload = {
        "job_id": job_id,
        "url": req.url,
        "mode": opts["mode"],
        "max_depth": req.max_depth,
        "max_pages": req.max_pages,
        "scope_mode": req.scope_mode,
        "document_types": req.document_types,
        "download_documents": opts["download_documents"],
        "content_mode": opts["content_mode"],
        "summarize": opts["summarize"],
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


def _normalize_doc(d: dict) -> dict:
    """Ensure document dict has only known keys for ScrapedDocument."""
    known = {"gcs_path", "filename", "content_type", "source_url", "size_bytes", "source_page_url"}
    return {k: v for k, v in d.items() if k in known}


def _normalize_page(p: dict) -> dict:
    """Ensure page dict has only known keys for ScrapedPage."""
    known = {"url", "text", "html", "final_url", "depth", "timestamp"}
    return {k: v for k, v in p.items() if k in known}


@app.get("/scrape/{job_id}", response_model=ScrapeStatusResponse)
def get_scrape_status(job_id: str, include_events: bool = False):
    """Poll scrape job status and results. Set include_events=1 for events list."""
    try:
        r = _get_redis()
        raw = r.get(SCRAPER_RESPONSE_KEY_PREFIX + job_id)
    except Exception as e:
        logger.exception("Redis get failed: %s", e)
        raise HTTPException(status_code=503, detail="Storage unavailable")
    if raw is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    data = json.loads(raw)
    events = None
    if include_events:
        events_key = SCRAPER_EVENTS_KEY_PREFIX + job_id
        events_raw = r.lrange(events_key, 0, -1)
        events = [json.loads(e) for e in events_raw] if events_raw else []
    return ScrapeStatusResponse(
        job_id=data["job_id"],
        status=data["status"],
        documents=[ScrapedDocument(**_normalize_doc(d)) for d in data.get("documents", [])],
        pages=[ScrapedPage(**_normalize_page(p)) for p in data.get("pages", [])],
        summary=data.get("summary"),
        error=data.get("error"),
        pages_scraped=data.get("pages_scraped", 0),
        events=events,
    )


def _stream_scrape_content(job_id: str):
    """Generator that yields SSE events as ScraperEvent JSON."""
    import redis

    r = redis.from_url(REDIS_URL, decode_responses=True)
    channel = SCRAPER_STREAM_CHANNEL_PREFIX + job_id
    events_key = SCRAPER_EVENTS_KEY_PREFIX + job_id
    pubsub = r.pubsub()
    pubsub.subscribe(channel)

    # If job already completed, replay from stored events or reconstruct
    raw = r.get(SCRAPER_RESPONSE_KEY_PREFIX + job_id)
    if raw:
        data = json.loads(raw)
        if data.get("status") in ("completed", "failed"):
            events_raw = r.lrange(events_key, 0, -1)
            if events_raw:
                for evt_str in events_raw:
                    yield f"data: {evt_str}\n\n"
            else:
                # Reconstruct ScraperEvents from stored result (backward compat)
                from datetime import datetime, timezone
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                for p in data.get("pages", []):
                    evt = {
                        "type": "page",
                        "metadata": {"timestamp": ts, "job_id": job_id, "source_url": p["url"], "final_url": p.get("final_url", p["url"]), "depth": p.get("depth")},
                        "payload": {"url": p["url"], "text": p.get("text"), "html": p.get("html")},
                    }
                    yield f"data: {json.dumps(evt)}\n\n"
                for d in data.get("documents", []):
                    evt = {
                        "type": "document",
                        "metadata": {"timestamp": ts, "job_id": job_id, "source_url": d.get("source_url"), "content_type": d.get("content_type"), "size_bytes": d.get("size_bytes")},
                        "payload": {"gcs_path": d["gcs_path"], "filename": d["filename"]},
                    }
                    yield f"data: {json.dumps(evt)}\n\n"
                if data.get("summary"):
                    evt = {"type": "summary", "metadata": {"timestamp": ts, "job_id": job_id}, "payload": {"summary": data["summary"]}}
                    yield f"data: {json.dumps(evt)}\n\n"
                evt = {"type": "done", "metadata": {"timestamp": ts, "job_id": job_id}, "payload": {"status": data["status"], "pages_scraped": data.get("pages_scraped", 0), "documents_count": len(data.get("documents", []))}}
                yield f"data: {json.dumps(evt)}\n\n"
            pubsub.unsubscribe(channel)
            return

    # Listen for real-time updates
    try:
        for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                payload = json.loads(message["data"])
                yield f"data: {message['data']}\n\n"
                if payload.get("type") == "done":
                    break
            except (json.JSONDecodeError, TypeError):
                pass
    finally:
        pubsub.unsubscribe(channel)


@app.get("/scrape/{job_id}/stream")
def stream_scrape_content(job_id: str):
    """SSE stream of ScraperEvent JSON (progress, page, document, summary, done) as they occur."""
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
    """Sync: fetch single page, extract text, optionally summarize. Respects robots.txt."""
    try:
        text = await review_page(req.url)
    except PermissionError as e:
        logger.info("Review blocked by robots.txt for %s: %s", req.url, e)
        raise HTTPException(status_code=403, detail=str(e) or "This URL is disallowed by robots.txt. We do not scrape it.")
    except Exception as e:
        if getattr(e, "response", None) is not None and getattr(e.response, "status_code", None) == 403:
            raise HTTPException(
                status_code=403,
                detail="This site does not allow automated access (403 Forbidden). Open the URL in your browser instead.",
            )
        logger.warning("Review failed for %s: %s", req.url, e)
        raise HTTPException(status_code=502, detail=f"Failed to fetch page: {e}")
    summary = summarize_content(text) if req.include_summary and text else None
    return ScrapeReviewResponse(text=text, url=req.url, summary=summary)

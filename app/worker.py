"""Worker: consume from Redis queue, run scraper, write results."""
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone

from app.config import (
    REDIS_URL,
    SCRAPER_REQUEST_KEY,
    SCRAPER_RESPONSE_KEY_PREFIX,
    SCRAPER_RESPONSE_TTL_SECONDS,
    SCRAPER_STREAM_CHANNEL_PREFIX,
    SCRAPER_EVENTS_KEY_PREFIX,
)
from app.services.scraper import scrape_regular, scrape_tree
from app.services.summary import summarize_content

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [SCRAPER WORKER] - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_redis():
    import redis
    return redis.from_url(REDIS_URL, decode_responses=True)


def _publish_event(r, channel: str, events_key: str, event: dict, ttl: int) -> None:
    """Publish event to Redis channel and append to events list for replay (same contract as mobius-web-scrapper)."""
    payload = json.dumps(event)
    r.publish(channel, payload)
    r.rpush(events_key, payload)
    r.expire(events_key, ttl)


def _serialize_for_redis(obj: list) -> list:
    """Ensure list of dicts is JSON-serializable (e.g. tree scan may add non-serializable fields)."""
    out = []
    for item in obj:
        if isinstance(item, dict):
            out.append({k: (v if isinstance(v, (str, int, float, bool, type(None))) else str(v)) for k, v in item.items()})
        else:
            out.append(item)
    return out


def _user_facing_error(e: Exception) -> str:
    """Return a clear message for the user; honor 403 and robots.txt intent."""
    resp = getattr(e, "response", None)
    if resp is not None and getattr(resp, "status_code", None) == 403:
        return "This site does not allow automated access (403 Forbidden). Open the URL in your browser instead."
    if isinstance(e, PermissionError) or "robots.txt" in str(e).lower():
        return str(e) if str(e).strip() else "This URL is disallowed by robots.txt. We do not scrape it."
    return str(e)


def _publish_response(
    job_id: str,
    status: str,
    documents: list,
    pages: list,
    summary: str | None = None,
    error: str | None = None,
    pages_scraped: int = 0,
):
    r = _get_redis()
    key = SCRAPER_RESPONSE_KEY_PREFIX + job_id
    payload = {
        "job_id": job_id,
        "status": status,
        "documents": _serialize_for_redis(documents),
        "pages": _serialize_for_redis(pages),
        "summary": summary,
        "error": error,
        "pages_scraped": pages_scraped,
    }
    try:
        r.set(key, json.dumps(payload), ex=SCRAPER_RESPONSE_TTL_SECONDS)
    except Exception as e:
        logger.exception("Failed to write scraper response to Redis: %s", e)
        raise


def _process_job(payload: dict) -> None:
    job_id = payload.get("job_id", "")
    url = payload.get("url", "")
    mode = payload.get("mode", "regular")
    include_content = payload.get("include_content", True)
    include_summary = payload.get("include_summary", False)

    _publish_response(job_id, "running", [], [], None, None, 0)

    r = _get_redis()
    channel = SCRAPER_STREAM_CHANNEL_PREFIX + job_id
    events_key = SCRAPER_EVENTS_KEY_PREFIX + job_id
    ttl = SCRAPER_RESPONSE_TTL_SECONDS

    def emit(event: dict) -> None:
        _publish_event(r, channel, events_key, event, ttl)

    def on_page(page_url: str, text: str) -> None:
        evt = {
            "type": "page",
            "metadata": {
                "timestamp": _iso_now(),
                "job_id": job_id,
                "source_url": page_url,
                "final_url": page_url,
                "depth": None,
            },
            "payload": {"url": page_url, "text": text},
        }
        emit(evt)

    try:
        emit({
            "type": "progress",
            "metadata": {"timestamp": _iso_now(), "job_id": job_id},
            "payload": {"phase": "starting", "message": f"Starting {mode} scan of {url}"},
        })
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        if mode == "regular":
            documents, pages, n = loop.run_until_complete(
                scrape_regular(
                    url,
                    job_id,
                    document_types=payload.get("document_types"),
                    include_content=include_content,
                    on_page_scraped=on_page,
                )
            )
        else:
            documents, pages, n = loop.run_until_complete(
                scrape_tree(
                    url,
                    job_id,
                    max_depth=payload.get("max_depth"),
                    max_pages=payload.get("max_pages"),
                    scope_mode=payload.get("scope_mode"),
                    path_prefix=payload.get("path_prefix"),
                    document_types=payload.get("document_types"),
                    include_content=include_content,
                    on_page_scraped=on_page,
                )
            )

        loop.close()
        # Regular mode does not call on_page_scraped during scrape; emit page events from result
        if mode == "regular":
            for p in pages:
                evt = {
                    "type": "page",
                    "metadata": {
                        "timestamp": _iso_now(),
                        "job_id": job_id,
                        "source_url": p.get("url"),
                        "final_url": p.get("url"),
                        "depth": p.get("depth"),
                    },
                    "payload": {"url": p.get("url"), "text": p.get("text"), "html": p.get("html")},
                }
                emit(evt)
        # Emit document events so frontend shows downloaded docs (skills scraper has no on_document callback)
        for doc in documents:
            evt = {
                "type": "document",
                "metadata": {
                    "timestamp": _iso_now(),
                    "job_id": job_id,
                    "source_url": doc.get("source_url"),
                    "source_page_url": doc.get("source_page_url"),
                    "content_type": doc.get("content_type"),
                    "size_bytes": doc.get("size_bytes"),
                },
                "payload": {"gcs_path": doc["gcs_path"], "filename": doc["filename"]},
            }
            emit(evt)
        emit({
            "type": "done",
            "metadata": {"timestamp": _iso_now(), "job_id": job_id},
            "payload": {"status": "completed", "pages_scraped": n, "documents_count": len(documents)},
        })

        summary = None
        if include_summary and pages:
            aggregated = "\n\n---\n\n".join(
                f"From {p['url']}:\n{p['text']}" for p in pages
            )
            summary = summarize_content(aggregated)

        _publish_response(job_id, "completed", documents, pages, summary, None, n)
        logger.info("Job %s completed: %d docs, %d pages, summary=%s", job_id, len(documents), n, bool(summary))
    except Exception as e:
        logger.exception("Job %s failed: %s", job_id, e)
        msg = _user_facing_error(e)
        try:
            r = _get_redis()
            channel = SCRAPER_STREAM_CHANNEL_PREFIX + job_id
            events_key = SCRAPER_EVENTS_KEY_PREFIX + job_id
            evt = {
                "type": "done",
                "metadata": {"timestamp": _iso_now(), "job_id": job_id},
                "payload": {"status": "failed", "error": msg},
            }
            _publish_event(r, channel, events_key, evt, SCRAPER_RESPONSE_TTL_SECONDS)
        except Exception:
            pass
        _publish_response(job_id, "failed", [], [], None, msg, 0)


def run_worker():
    try:
        r = _get_redis()
        r.ping()
    except Exception as e:
        logger.error(
            "Redis not available at %s: %s. Start Redis: brew services start redis (or redis-server)",
            REDIS_URL,
            e,
        )
        sys.exit(1)
    logger.info("Starting scraper worker, consuming from %s", SCRAPER_REQUEST_KEY)
    while True:
        try:
            result = r.brpop(SCRAPER_REQUEST_KEY, timeout=5)
            if result is None:
                continue
            _, raw = result
            payload = json.loads(raw)
            _process_job(payload)
        except KeyboardInterrupt:
            logger.info("Worker stopped")
            sys.exit(0)
        except Exception as e:
            logger.exception("Consumer error: %s", e)


if __name__ == "__main__":
    run_worker()

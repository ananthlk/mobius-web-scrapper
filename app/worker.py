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
from app.services.fetcher import extract_text
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
    """Publish event to Redis channel and append to events list for replay."""
    payload = json.dumps(event)
    r.publish(channel, payload)
    r.rpush(events_key, payload)
    r.expire(events_key, ttl)


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
    summary: str | None,
    error: str | None,
    pages_scraped: int,
):
    r = _get_redis()
    key = SCRAPER_RESPONSE_KEY_PREFIX + job_id
    payload = {
        "job_id": job_id,
        "status": status,
        "documents": documents,
        "pages": pages,
        "summary": summary,
        "error": error,
        "pages_scraped": pages_scraped,
    }
    r.set(key, json.dumps(payload), ex=SCRAPER_RESPONSE_TTL_SECONDS)


def _process_job(payload: dict) -> None:
    job_id = payload.get("job_id", "")
    url = payload.get("url", "")
    mode = payload.get("mode", "simple")
    content_mode = payload.get("content_mode", "text")
    download_documents = payload.get("download_documents", True)
    summarize_flag = payload.get("summarize", False)

    _publish_response(job_id, "running", [], [], None, None, 0)

    r = _get_redis()
    channel = SCRAPER_STREAM_CHANNEL_PREFIX + job_id
    events_key = SCRAPER_EVENTS_KEY_PREFIX + job_id
    ttl = SCRAPER_RESPONSE_TTL_SECONDS
    documents_so_far: list[dict] = []
    pages_so_far: list[dict] = []

    def emit(event: dict) -> None:
        _publish_event(r, channel, events_key, event, ttl)

    def _push_running_state() -> None:
        """Write current documents/pages to Redis so frontend (poll or SSE) sees interim state.
        Pages are sent as stubs (url, depth, timestamp) only to avoid huge payloads when
        content_mode is 'both' (text+html); full content is streamed via SSE page events."""
        page_stubs = [
            {"url": p.get("url"), "depth": p.get("depth"), "timestamp": p.get("timestamp")}
            for p in pages_so_far
        ]
        _publish_response(
            job_id, "running", list(documents_so_far), page_stubs, None, None, len(pages_so_far)
        )

    def on_progress(phase: str, message: str, url_or_none: str | None, count: int | None) -> None:
        evt = {
            "type": "progress",
            "metadata": {"timestamp": _iso_now(), "job_id": job_id},
            "payload": {"phase": phase, "message": message},
        }
        if url_or_none:
            evt["payload"]["url"] = url_or_none
        if count is not None:
            evt["payload"]["count"] = count
        emit(evt)

    def on_page(page_url: str, entry: dict) -> None:
        evt = {
            "type": "page",
            "metadata": {
                "timestamp": entry.get("timestamp", _iso_now()),
                "job_id": job_id,
                "source_url": page_url,
                "final_url": entry.get("final_url", page_url),
                "depth": entry.get("depth"),
            },
            "payload": {"url": page_url, **{k: v for k, v in entry.items() if k in ("text", "html")}},
        }
        emit(evt)
        page_record = {"url": page_url or entry.get("final_url", ""), **{k: v for k, v in entry.items() if k in ("text", "html", "final_url", "depth", "timestamp")}}
        pages_so_far.append(page_record)
        _push_running_state()

    def on_document(doc: dict, meta: dict) -> None:
        evt = {
            "type": "document",
            "metadata": {
                "timestamp": _iso_now(),
                "job_id": job_id,
                "source_url": doc.get("source_url"),
                "source_page_url": meta.get("source_page_url") or doc.get("source_page_url"),
                "content_type": doc.get("content_type"),
                "size_bytes": doc.get("size_bytes"),
            },
            "payload": {"gcs_path": doc["gcs_path"], "filename": doc["filename"]},
        }
        emit(evt)
        documents_so_far.append(doc)
        _push_running_state()

    try:
        emit({"type": "progress", "metadata": {"timestamp": _iso_now(), "job_id": job_id}, "payload": {"phase": "starting", "message": f"Starting {mode} scan of {url}"}})
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        if mode == "simple":
            documents, pages, n = loop.run_until_complete(
                scrape_regular(
                    url,
                    job_id,
                    document_types=payload.get("document_types"),
                    content_mode=content_mode,
                    download_documents=download_documents,
                    on_page_scraped=on_page,
                    on_document=on_document,
                    on_progress=on_progress,
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
                    document_types=payload.get("document_types"),
                    content_mode=content_mode,
                    download_documents=download_documents,
                    on_page_scraped=on_page,
                    on_document=on_document,
                    on_progress=on_progress,
                )
            )

        loop.close()

        summary = None
        if summarize_flag and pages:
            emit({"type": "progress", "metadata": {"timestamp": _iso_now(), "job_id": job_id}, "payload": {"phase": "summarizing", "message": "Generating summary..."}})
            parts = []
            for p in pages:
                txt = p.get("text")
                if not txt and p.get("html"):
                    txt = extract_text(p["html"])
                if txt and txt.strip():
                    parts.append(f"From {p['url']}:\n{txt}")
            if parts:
                aggregated = "\n\n---\n\n".join(parts)
                summary = summarize_content(aggregated)
            if summary:
                emit({"type": "summary", "metadata": {"timestamp": _iso_now(), "job_id": job_id}, "payload": {"summary": summary}})

        emit({"type": "done", "metadata": {"timestamp": _iso_now(), "job_id": job_id}, "payload": {"status": "completed", "pages_scraped": n, "documents_count": len(documents)}})
        _publish_response(job_id, "completed", documents, pages, summary, None, n)
        logger.info("Job %s completed: %d docs, %d pages, summary=%s", job_id, len(documents), n, bool(summary))
    except Exception as e:
        logger.exception("Job %s failed: %s", job_id, e)
        msg = _user_facing_error(e)
        emit({"type": "done", "metadata": {"timestamp": _iso_now(), "job_id": job_id}, "payload": {"status": "failed", "error": msg}})
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
            logger.debug("Waiting for job on %s...", SCRAPER_REQUEST_KEY)
            result = r.brpop(SCRAPER_REQUEST_KEY, timeout=5)
            if result is None:
                continue
            _, raw = result
            logger.info("Received job payload: %s", raw[:200] if len(raw) > 200 else raw)
            payload = json.loads(raw)
            logger.info("Processing job %s for URL %s", payload.get("job_id"), payload.get("url"))
            _process_job(payload)
        except KeyboardInterrupt:
            logger.info("Worker stopped")
            sys.exit(0)
        except Exception as e:
            logger.exception("Consumer error: %s", e)


if __name__ == "__main__":
    run_worker()

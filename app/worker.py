"""Worker: consume from Redis queue, run scraper, write results."""
import asyncio
import json
import logging
import sys

from app.config import (
    REDIS_URL,
    SCRAPER_REQUEST_KEY,
    SCRAPER_RESPONSE_KEY_PREFIX,
    SCRAPER_RESPONSE_TTL_SECONDS,
    SCRAPER_STREAM_CHANNEL_PREFIX,
)
from app.services.scraper import scrape_regular, scrape_tree
from app.services.summary import summarize_content

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [SCRAPER WORKER] - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _get_redis():
    import redis
    return redis.from_url(REDIS_URL, decode_responses=True)


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
    mode = payload.get("mode", "regular")
    include_content = payload.get("include_content", True)
    include_summary = payload.get("include_summary", False)

    _publish_response(job_id, "running", [], [], None, None, 0)

    channel = SCRAPER_STREAM_CHANNEL_PREFIX + job_id

    def on_page(url: str, text: str) -> None:
        r = _get_redis()
        r.publish(channel, json.dumps({"url": url, "text": text}))

    try:
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
                    document_types=payload.get("document_types"),
                    include_content=include_content,
                    on_page_scraped=on_page,
                )
            )

        loop.close()
        # Signal stream complete
        _get_redis().publish(channel, json.dumps({"done": True}))

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
        _publish_response(job_id, "failed", [], [], None, str(e), 0)


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

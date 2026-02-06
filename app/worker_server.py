"""
Minimal HTTP server for Cloud Run: runs the scraper worker in a background thread
and serves /health so Cloud Run keeps the instance alive.
Restarts the consumer thread if it exits (e.g. transient Redis failure).
Use: uvicorn app.worker_server:app --host 0.0.0.0 --port ${PORT}
"""
import logging
import threading
import time

from fastapi import FastAPI

from app.worker import run_worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [scraper-worker] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Mobius Scraper Worker", version="0.1.0")

_worker_thread: threading.Thread | None = None
_stop_restart = False
_RESTART_DELAY_SEC = 5
_MAX_RESTART_DELAY_SEC = 60


def _run_worker_loop():
    """Run worker; on exit, log and return so caller can restart."""
    try:
        run_worker()
    except Exception as e:
        logger.exception("Worker loop exited: %s", e)


@app.on_event("startup")
def start_worker():
    global _worker_thread, _stop_restart
    _stop_restart = False
    delay = _RESTART_DELAY_SEC

    def _run():
        nonlocal delay
        while not _stop_restart:
            t = threading.Thread(target=_run_worker_loop, daemon=True)
            t.start()
            logger.info("Scraper worker started in background thread")
            t.join()
            if _stop_restart:
                break
            logger.warning("Worker thread exited; restarting in %s s (max %s s)", delay, _MAX_RESTART_DELAY_SEC)
            time.sleep(delay)
            delay = min(delay * 2, _MAX_RESTART_DELAY_SEC)

    runner = threading.Thread(target=_run, daemon=True)
    runner.start()


@app.get("/health")
def health():
    return {"status": "ok", "service": "mobius-scraper-worker"}

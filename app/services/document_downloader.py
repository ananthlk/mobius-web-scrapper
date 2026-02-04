"""Download documents from URLs and upload to GCS."""
import logging
import re
from urllib.parse import urlparse, unquote

import httpx
from google.cloud import storage

from app.config import GCS_BUCKET, DOCUMENT_TYPES

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mobius-WebScraper/1.0 (+https://github.com/mobius)",
}


def _sanitize_filename(filename: str) -> str:
    """Make filename safe for GCS."""
    # Remove path components
    base = filename.split("/")[-1].split("\\")[-1]
    # Remove/replace unsafe chars
    safe = re.sub(r'[^\w\-_.]', '_', base)
    return safe or "document"


def _filename_from_url(url: str, content_disposition: str | None) -> str:
    """Derive filename from URL or Content-Disposition."""
    if content_disposition:
        # Content-Disposition: attachment; filename="doc.pdf"
        m = re.search(r'filename\*?=(?:UTF-8\'[\'])?"?([^";\n]+)"?', content_disposition, re.I)
        if m:
            return _sanitize_filename(unquote(m.group(1).strip()))
    parsed = urlparse(url)
    path = unquote(parsed.path)
    if path and "/" in path:
        name = path.rsplit("/", 1)[-1]
        if name and "." in name:
            return _sanitize_filename(name)
    return "document"


async def download_and_upload(
    url: str,
    job_id: str,
    document_types: list[str] | None = None,
    source_page_url: str | None = None,
) -> dict | None:
    """
    Download document from URL and upload to GCS.
    Returns { gcs_path, filename, content_type, source_url, size_bytes, source_page_url? } or None.
    """
    ext_set = set((document_types or DOCUMENT_TYPES))
    parsed = urlparse(url)
    path_lower = parsed.path.lower()
    ext = path_lower.split(".")[-1] if "." in path_lower else ""

    if ext not in ext_set:
        return None

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=60.0,
        headers=DEFAULT_HEADERS,
    ) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to download %s: %s", url, e)
            return None

        content_type = resp.headers.get("content-type", "").split(";")[0].strip()
        content_disp = resp.headers.get("content-disposition")
        filename = _filename_from_url(url, content_disp)
        if not filename.endswith(f".{ext}"):
            filename = f"{filename}.{ext}" if "." not in filename else filename

        blob_path = f"web-scraper/{job_id}/{filename}"
        try:
            client_storage = storage.Client()
            bucket = client_storage.bucket(GCS_BUCKET)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(
                resp.content,
                content_type=content_type or "application/octet-stream",
            )
            gcs_path = f"gs://{GCS_BUCKET}/{blob_path}"
            size_bytes = len(resp.content)
            logger.info("Uploaded %s to %s (%d bytes)", filename, gcs_path, size_bytes)
            result: dict = {
                "gcs_path": gcs_path,
                "filename": filename,
                "content_type": content_type or None,
                "source_url": url,
                "size_bytes": size_bytes,
            }
            if source_page_url:
                result["source_page_url"] = source_page_url
            return result
        except Exception as e:
            logger.error("Failed to upload to GCS %s: %s", blob_path, e)
            return None

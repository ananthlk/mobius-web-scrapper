"""Core scrape logic: regular and tree scan."""
import logging
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import urlparse

from app.config import TREE_MAX_DEPTH, TREE_MAX_PAGES, SCOPE_MODE, DOCUMENT_TYPES
from app.services.fetcher import fetch_page, extract_links, extract_page_links, extract_text, extract_html
from app.services.document_downloader import download_and_upload

logger = logging.getLogger(__name__)


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _same_domain(url: str, base_netloc: str) -> bool:
    parsed = urlparse(url)
    return parsed.netloc.lower() == base_netloc.lower()


def _same_origin(url: str, base_url: str) -> bool:
    p1 = urlparse(url)
    p2 = urlparse(base_url)
    return (
        p1.scheme == p2.scheme
        and p1.netloc.lower() == p2.netloc.lower()
        and (p1.port or 80) == (p2.port or 80)
    )


def _build_page_entry(url: str, content_mode: str, html: str, depth: int | None = None) -> dict:
    """Build page entry dict with text/html based on content_mode, plus metadata."""
    entry: dict = {"url": url, "final_url": url, "depth": depth, "timestamp": _iso_now()}
    if content_mode in ("text", "both"):
        text = extract_text(html)
        if text.strip():
            entry["text"] = text
    if content_mode in ("html", "both"):
        entry["html"] = extract_html(html)
    return entry if ("text" in entry or "html" in entry) else {}


async def scrape_regular(
    url: str,
    job_id: str,
    document_types: list[str] | None = None,
    content_mode: str = "text",
    download_documents: bool = True,
    on_page_scraped: Callable[[str, dict], None] | None = None,
    on_document: Callable[[dict, dict], None] | None = None,
    on_progress: Callable[[str, str, str | None, int | None], None] | None = None,
) -> tuple[list[dict], list[dict], int]:
    """
    Simple scan: single page, extract content and optionally download documents.
    Returns (documents, pages, pages_scraped).
    """
    doc_types = document_types or DOCUMENT_TYPES
    ext_set = set(doc_types)

    if on_progress:
        on_progress("fetching", f"Fetching {url}", url, None)
    html, final_url = await fetch_page(url)
    documents: list[dict] = []
    pages: list[dict] = []

    if content_mode != "none":
        entry = _build_page_entry(final_url, content_mode, html, depth=0)
        if entry:
            pages.append(entry)
            if on_page_scraped:
                on_page_scraped(final_url, entry)

    if download_documents:
        doc_links = extract_links(html, final_url, ext_set)
        for i, link in enumerate(doc_links):
            if on_progress:
                on_progress("downloading", f"Downloading document {i + 1}/{len(doc_links)}", link, i + 1)
            result = await download_and_upload(link, job_id, doc_types, source_page_url=final_url)
            if result:
                documents.append(result)
                if on_document:
                    on_document(result, {"source_page_url": final_url})

    return documents, pages, 1


async def scrape_tree(
    url: str,
    job_id: str,
    max_depth: int | None = None,
    max_pages: int | None = None,
    scope_mode: str | None = None,
    document_types: list[str] | None = None,
    content_mode: str = "text",
    download_documents: bool = True,
    on_page_scraped: Callable[[str, dict], None] | None = None,
    on_document: Callable[[dict, dict], None] | None = None,
    on_progress: Callable[[str, str, str | None, int | None], None] | None = None,
) -> tuple[list[dict], list[dict], int]:
    """
    Tree scan: BFS from seed URL, collect document links and page content, optionally download.
    Returns (documents, pages, pages_scraped).
    """
    depth_limit = max_depth or TREE_MAX_DEPTH
    limit = max_pages or TREE_MAX_PAGES
    scope = scope_mode or SCOPE_MODE
    doc_types = document_types or DOCUMENT_TYPES
    ext_set = set(doc_types)

    parsed_seed = urlparse(url)
    base_netloc = parsed_seed.netloc.lower()

    to_visit: list[tuple[str, int]] = [(url, 0)]
    visited: set[str] = set()
    all_doc_links: list[tuple[str, str]] = []  # (link, source_page_url)
    seen_doc_links: set[str] = set()
    pages_content: list[dict] = []
    pages_scraped = 0

    while to_visit and pages_scraped < limit:
        current, d = to_visit.pop(0)
        if current in visited:
            continue
        visited.add(current)

        if d > depth_limit:
            continue

        try:
            if on_progress:
                on_progress("fetching", f"Fetching page {pages_scraped + 1}: {current}", current, pages_scraped)
            html, final_url = await fetch_page(current)
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", current, e)
            continue

        pages_scraped += 1
        if content_mode != "none":
            entry = _build_page_entry(final_url, content_mode, html, depth=d)
            if entry:
                pages_content.append(entry)
                if on_page_scraped:
                    on_page_scraped(final_url, entry)

        if download_documents:
            doc_links = extract_links(html, final_url, ext_set)
            for link in doc_links:
                if link not in seen_doc_links:
                    seen_doc_links.add(link)
                    all_doc_links.append((link, final_url))

        # Collect page links for next level
        if d < depth_limit:
            page_links = extract_page_links(html, final_url)
            for link in page_links:
                if scope == "same_domain" and not _same_domain(link, base_netloc):
                    continue
                if scope == "same_origin" and not _same_origin(link, url):
                    continue
                if link not in visited and (link, d + 1) not in [(u, _) for u, _ in to_visit]:
                    to_visit.append((link, d + 1))

    documents: list[dict] = []
    if download_documents and all_doc_links:
        for i, (link, source_page_url) in enumerate(all_doc_links):
            if on_progress:
                on_progress("downloading", f"Downloading document {i + 1}/{len(all_doc_links)}", link, i + 1)
            result = await download_and_upload(link, job_id, doc_types, source_page_url=source_page_url)
            if result:
                documents.append(result)
                if on_document:
                    on_document(result, {"source_page_url": source_page_url})

    return documents, pages_content, pages_scraped


async def review_page(url: str) -> str:
    """
    Sync review: fetch single page, extract text only (no download).
    Used by Chat for "review this webpage".
    """
    html, _ = await fetch_page(url)
    return extract_text(html)

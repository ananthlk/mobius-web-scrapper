"""Core scrape logic: regular and tree scan."""
import asyncio
import logging
from typing import Callable
from urllib.parse import urlparse

from app.config import TREE_MAX_DEPTH, TREE_MAX_PAGES, SCOPE_MODE, DOCUMENT_TYPES
from app.services.fetcher import (
    fetch_page,
    extract_links,
    extract_page_links,
    extract_text,
    can_fetch_url,
    DEFAULT_USER_AGENT,
)
from app.services.document_downloader import download_and_upload

logger = logging.getLogger(__name__)


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


def _path_matches_prefix(path: str, prefix: str) -> bool:
    """Return True if path is under prefix (or exact match). prefix e.g. /providers."""
    if not prefix or not prefix.strip():
        return True
    p = (path or "/").lower()
    pre = prefix.strip().lower().rstrip("/")
    if not pre.startswith("/"):
        pre = "/" + pre
    return p == pre or p.startswith(pre + "/")


async def scrape_regular(
    url: str,
    job_id: str,
    document_types: list[str] | None = None,
    include_content: bool = True,
    on_page_scraped: Callable[[str, str], None] | None = None,
) -> tuple[list[dict], list[dict], int]:
    """
    Regular scan: single page, extract text and download documents.
    Respects robots.txt: skips fetch/download if disallowed.
    Returns (documents, pages, pages_scraped).
    """
    doc_types = document_types or DOCUMENT_TYPES
    ext_set = set(doc_types)
    robots_cache: dict = {}

    if not await can_fetch_url(url, DEFAULT_USER_AGENT, robots_cache):
        logger.info("Skipping %s: disallowed by robots.txt", url)
        raise PermissionError("This URL is disallowed by robots.txt. We do not scrape it.")
    html, final_url = await fetch_page(url)
    text = extract_text(html) if include_content else ""
    doc_links = extract_links(html, final_url, ext_set)
    documents: list[dict] = []
    pages: list[dict] = []
    if include_content and text.strip():
        pages.append({"url": final_url, "text": text})

    for link in doc_links:
        if not await can_fetch_url(link, DEFAULT_USER_AGENT, robots_cache):
            logger.debug("Skipping document %s: disallowed by robots.txt", link)
            continue
        result = await download_and_upload(link, job_id, doc_types)
        if result:
            documents.append(result)

    return documents, pages, 1


async def scrape_tree(
    url: str,
    job_id: str,
    max_depth: int | None = None,
    max_pages: int | None = None,
    scope_mode: str | None = None,
    path_prefix: str | None = None,
    document_types: list[str] | None = None,
    include_content: bool = True,
    on_page_scraped: Callable[[str, str], None] | None = None,
) -> tuple[list[dict], list[dict], int]:
    """
    Tree scan: BFS from seed URL, collect document links and page content, download.
    Returns (documents, pages, pages_scraped).
    """
    depth = max_depth or TREE_MAX_DEPTH
    limit = max_pages or TREE_MAX_PAGES
    scope = scope_mode or SCOPE_MODE
    doc_types = document_types or DOCUMENT_TYPES
    ext_set = set(doc_types)
    path_prefix_norm = None
    if path_prefix and str(path_prefix).strip():
        p = str(path_prefix).strip()
        path_prefix_norm = p if p.startswith("/") else "/" + p
        logger.info("Tree scan path_prefix filter active: %r", path_prefix_norm)

    parsed_seed = urlparse(url)
    base_netloc = parsed_seed.netloc.lower()
    robots_cache: dict = {}
    if not await can_fetch_url(url, DEFAULT_USER_AGENT, robots_cache):
        logger.info("Seed URL %s disallowed by robots.txt", url)
        raise PermissionError("This URL is disallowed by robots.txt. We do not scrape it.")

    to_visit: list[tuple[str, int]] = [(url, 0)]
    visited: set[str] = set()
    all_doc_links: set[str] = set()
    pages_content: list[dict] = []
    pages_scraped = 0

    while to_visit and pages_scraped < limit:
        current, d = to_visit.pop(0)
        if current in visited:
            continue
        visited.add(current)

        if d > depth:
            continue

        # Skip document URLs (pdf, etc.) — we don't fetch them as HTML; they're downloaded via doc_links
        parsed_current = urlparse(current)
        path_lower = (parsed_current.path or "").lower()
        ext = path_lower.split(".")[-1] if "." in path_lower else ""
        if ext in ext_set:
            logger.debug("Skipping document URL (not a page): %s", current[:80])
            continue

        # Tree mode: skip pages disallowed by robots.txt but proceed with other pages
        if not await can_fetch_url(current, DEFAULT_USER_AGENT, robots_cache):
            logger.debug("Skipping %s: disallowed by robots.txt", current)
            continue
        try:
            html, final_url = await fetch_page(current)
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", current, e)
            continue

        pages_scraped += 1
        if include_content:
            text = extract_text(html)
            if text.strip():
                pages_content.append({"url": final_url, "text": text})
                if on_page_scraped:
                    on_page_scraped(final_url, text)

        # Collect document links
        doc_links = extract_links(html, final_url, ext_set)
        for link in doc_links:
            all_doc_links.add(link)

        # Collect page links for next level (exclude document URLs — they're downloaded, not fetched as HTML)
        if d < depth:
            page_links = extract_page_links(html, final_url)
            for link in page_links:
                if scope == "same_domain" and not _same_domain(link, base_netloc):
                    continue
                if scope == "same_origin" and not _same_origin(link, url):
                    continue
                parsed_link = urlparse(link)
                link_path = parsed_link.path or "/"
                if path_prefix_norm and not _path_matches_prefix(link_path, path_prefix_norm):
                    continue
                path_lower = link_path.lower()
                ext = path_lower.split(".")[-1] if "." in path_lower else ""
                if ext in ext_set:
                    continue
                if link not in visited and (link, d + 1) not in [(u, _) for u, _ in to_visit]:
                    to_visit.append((link, d + 1))

    # Download all documents
    documents: list[dict] = []
    for link in all_doc_links:
        if not await can_fetch_url(link, DEFAULT_USER_AGENT, robots_cache):
            logger.debug("Skipping document %s: disallowed by robots.txt", link)
            continue
        result = await download_and_upload(link, job_id, doc_types)
        if result:
            documents.append(result)

    return documents, pages_content, pages_scraped


async def review_page(url: str) -> str:
    """
    Sync review: fetch single page, extract text only (no download).
    Used by Chat for "review this webpage". Respects robots.txt.
    """
    robots_cache: dict = {}
    if not await can_fetch_url(url, DEFAULT_USER_AGENT, robots_cache):
        raise PermissionError("This URL is disallowed by robots.txt. We do not scrape it.")
    html, _ = await fetch_page(url)
    return extract_text(html)

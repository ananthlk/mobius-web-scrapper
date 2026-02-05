"""HTTP fetch and link extraction."""
import logging
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Identify as a bot; sites that return 403 are treated as "do not scrape" (see fetch_page).
DEFAULT_USER_AGENT = "Mobius-WebScraper/1.0 (+https://github.com/mobius)"
DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _origin(url: str) -> str:
    """Return scheme + netloc for robots.txt URL (e.g. https://example.com)."""
    p = urlparse(url)
    scheme = p.scheme or "https"
    netloc = p.netloc or ""
    return f"{scheme}://{netloc}"


async def fetch_robots_txt(origin: str, timeout: float = 10.0) -> str:
    """
    Fetch robots.txt for the given origin (e.g. https://example.com).
    Returns body as string; empty string on 404, timeout, or error (treated as allow-all).
    """
    robots_url = f"{origin.rstrip('/')}/robots.txt"
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        ) as client:
            resp = await client.get(robots_url)
            if resp.status_code == 404:
                return ""
            resp.raise_for_status()
            return resp.text or ""
    except Exception as e:
        logger.debug("Could not fetch robots.txt from %s: %s", robots_url, e)
        return ""


def _parse_robots(robots_txt: str) -> RobotFileParser:
    """Parse robots.txt content and return a RobotFileParser. Empty content = allow all."""
    rp = RobotFileParser()
    if (robots_txt or "").strip():
        rp.parse(robots_txt.strip().splitlines())
    return rp


async def can_fetch_url(
    url: str,
    user_agent: str,
    robots_cache: dict[str, RobotFileParser],
    timeout: float = 10.0,
) -> bool:
    """
    Return True if robots.txt allows user_agent to fetch url.
    robots_cache is keyed by origin (scheme + netloc); we populate it per origin.
    If robots.txt is missing or unparseable, we allow (common practice).
    """
    origin = _origin(url)
    if origin not in robots_cache:
        raw = await fetch_robots_txt(origin, timeout=timeout)
        robots_cache[origin] = _parse_robots(raw)
    return robots_cache[origin].can_fetch(user_agent, url)


async def fetch_page(url: str, timeout: float = 30.0) -> tuple[str, str]:
    """
    Fetch a URL and return (html_content, final_url).
    Raises httpx.HTTPError on failure (e.g. 403 = site blocks bots; we do not circumvent).
    """
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout,
        headers=DEFAULT_HEADERS,
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text, str(resp.url)


def extract_links(html: str, base_url: str, document_extensions: set[str]) -> list[str]:
    """
    Extract document links from HTML that match given extensions.
    Returns list of absolute URLs.
    """
    soup = BeautifulSoup(html, "html.parser")
    parsed_base = urlparse(base_url)
    base_scheme = parsed_base.scheme
    base_netloc = parsed_base.netloc
    seen: set[str] = set()
    result: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        try:
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            path = parsed.path.lower()
            ext = path.split(".")[-1] if "." in path else ""
            if ext in document_extensions:
                if absolute not in seen:
                    seen.add(absolute)
                    result.append(absolute)
        except Exception as e:
            logger.debug("Skipping link %r: %s", href, e)

    return result


def extract_page_links(html: str, base_url: str) -> list[str]:
    """
    Extract all same-page links (for tree scan) - same domain only.
    Returns list of absolute URLs.
    """
    soup = BeautifulSoup(html, "html.parser")
    parsed_base = urlparse(base_url)
    base_netloc = parsed_base.netloc.lower()
    seen: set[str] = set()
    result: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        try:
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            if parsed.scheme not in ("http", "https"):
                continue
            if parsed.netloc.lower() != base_netloc:
                continue
            # Normalize: strip fragment
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed.query:
                normalized += "?" + parsed.query
            if normalized not in seen:
                seen.add(normalized)
                result.append(absolute)
        except Exception as e:
            logger.debug("Skipping link %r: %s", href, e)

    return result


def extract_text(html: str) -> str:
    """Extract main text content from HTML (strip tags, normalize whitespace)."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n\n".join(lines)


def extract_html(html: str, clean: bool = True) -> str:
    """
    Extract body HTML from page. When clean=True, strip scripts/styles/nav/footer
    for cleaner content HTML. Returns string of body HTML.
    """
    soup = BeautifulSoup(html, "html.parser")
    if clean:
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
    body = soup.find("body")
    if body:
        return str(body)
    return str(soup)

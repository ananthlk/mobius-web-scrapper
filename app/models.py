"""Pydantic models for scrape requests and responses."""
from pydantic import BaseModel
from typing import Any, Literal, Optional, Union


class ScrapeRequest(BaseModel):
    """Request for async scrape job."""
    url: str
    mode: Literal["simple", "tree", "regular"] = "simple"  # "regular" deprecated, maps to "simple"
    max_depth: Optional[int] = None
    max_pages: Optional[int] = None
    scope_mode: Optional[Literal["same_domain", "same_origin", "cross_domain"]] = None
    document_types: Optional[list[str]] = None
    download_documents: bool = True  # Download PDFs, DOC, etc. to GCS
    content_mode: Literal["none", "text", "html", "both"] = "text"  # What to extract from pages
    summarize: bool = False  # LLM summary (requires OPENAI_API_KEY or Vertex)
    # Deprecated: use content_mode and summarize
    include_content: Optional[bool] = None
    include_summary: Optional[bool] = None


class ScrapeReviewRequest(BaseModel):
    """Request for sync page review (Chat use case)."""
    url: str
    include_summary: bool = False  # Add LLM summary when true


class ScrapeReviewResponse(BaseModel):
    """Response from sync page review."""
    text: str
    url: str
    summary: Optional[str] = None  # LLM summary when include_summary=true


class ScrapedDocument(BaseModel):
    """A document downloaded from a scrape."""
    gcs_path: str
    filename: str
    content_type: Optional[str] = None
    source_url: Optional[str] = None
    size_bytes: Optional[int] = None
    source_page_url: Optional[str] = None


class ScrapedPage(BaseModel):
    """Extracted content from a scraped HTML page."""
    url: str
    text: Optional[str] = None  # When content_mode in (text, both)
    html: Optional[str] = None  # When content_mode in (html, both)
    final_url: Optional[str] = None
    depth: Optional[int] = None
    timestamp: Optional[str] = None


# --- ScraperEvent: unified event envelope for SSE stream ---

class ScraperEventMetadata(BaseModel):
    """Metadata common to all event types."""
    timestamp: str  # ISO8601
    job_id: str
    source_url: Optional[str] = None
    depth: Optional[int] = None


class PageEventMetadata(ScraperEventMetadata):
    final_url: Optional[str] = None
    content_length: Optional[int] = None


class DocumentEventMetadata(ScraperEventMetadata):
    source_url: Optional[str] = None  # Document URL
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None
    source_page_url: Optional[str] = None


class ScraperEvent(BaseModel):
    type: Literal["progress", "page", "document", "summary", "done"]
    metadata: Union[ScraperEventMetadata, PageEventMetadata, DocumentEventMetadata]
    payload: Optional[dict[str, Any]] = None


class ScrapeJobResponse(BaseModel):
    """Response when submitting async scrape job."""
    job_id: str


class ScrapeStatusResponse(BaseModel):
    """Status of a scrape job."""
    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    documents: list[ScrapedDocument] = []
    pages: list[ScrapedPage] = []  # Raw content from HTML pages
    summary: Optional[str] = None  # LLM summary when summarize=true
    error: Optional[str] = None
    pages_scraped: int = 0
    events: Optional[list[dict]] = None  # ScraperEvent list when include_events=1

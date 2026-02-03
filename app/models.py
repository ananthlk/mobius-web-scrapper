"""Pydantic models for scrape requests and responses."""
from pydantic import BaseModel
from typing import Literal, Optional


class ScrapeRequest(BaseModel):
    """Request for async scrape job."""
    url: str
    mode: Literal["regular", "tree"] = "regular"
    max_depth: Optional[int] = None
    max_pages: Optional[int] = None
    scope_mode: Optional[Literal["same_domain", "same_origin", "cross_domain"]] = None
    document_types: Optional[list[str]] = None
    include_content: bool = True  # Include extracted page text in result
    include_summary: bool = False  # Add LLM summary (requires OPENAI_API_KEY or Vertex)


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


class ScrapedPage(BaseModel):
    """Extracted text from a scraped HTML page."""
    url: str
    text: str


class ScrapeJobResponse(BaseModel):
    """Response when submitting async scrape job."""
    job_id: str


class ScrapeStatusResponse(BaseModel):
    """Status of a scrape job."""
    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    documents: list[ScrapedDocument] = []
    pages: list[ScrapedPage] = []  # Raw content from HTML pages
    summary: Optional[str] = None  # LLM summary when include_summary=true
    error: Optional[str] = None
    pages_scraped: int = 0

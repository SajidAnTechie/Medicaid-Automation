"""
Navigator Agent — Pydantic Models
----------------------------------
Input / output schemas for the Navigator Agent ("The Researcher").

Designed for **AWS AgentCore** stateless execution:
  • `portal_url` is the **only required input**.
  • All context (state, portal type, datasets) is derived at runtime
    from the URL itself — no hidden or implicit dependencies.
  • Output is a fully self-contained JSON structure consumable by
    any downstream agent or AgentCore orchestrator.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator


class FileType(str, Enum):
    PDF = "pdf"
    EXCEL_XLS = "xls"
    EXCEL_XLSX = "xlsx"
    CSV = "csv"
    ZIP = "zip"
    UNKNOWN = "unknown"


class DatasetCategory(str, Enum):
    """Common Medicaid fee schedule categories found across state portals."""
    PHYSICIAN = "physician"
    DENTAL = "dental"
    PHARMACY = "pharmacy"
    DMEPOS = "dmepos"
    OUTPATIENT = "outpatient"
    INPATIENT = "inpatient"
    BEHAVIORAL_HEALTH = "behavioral_health"
    LAB = "laboratory"
    VISION = "vision"
    HOME_HEALTH = "home_health"
    GENERAL = "general"
    UNKNOWN = "unknown"


class DiscoveredDataset(BaseModel):
    """A single dataset link discovered on the portal."""
    url: str = Field(..., description="Direct download URL of the dataset")
    title: str = Field(..., description="Title or link text of the dataset")
    file_type: FileType = Field(..., description="Detected file type")
    page_source_url: str = Field(...,
                                 description="The page URL where this link was found")
    context_text: str = Field(
        "", description="Surrounding text/context near the link")
    parent_section: str = Field(
        "", description="Section or folder name containing this link")
    last_modified: Optional[str] = Field(
        None, description="Last modified date if available on the page")


class RankedDataset(BaseModel):
    """A dataset ranked by relevance to Medicaid fee schedules."""
    url: str = Field(..., description="Direct download URL")
    title: str = Field(..., description="Title of the dataset")
    file_type: FileType = Field(..., description="Detected file type")
    category: DatasetCategory = Field(...,
                                      description="Detected fee schedule category")
    relevance_score: float = Field(..., ge=0.0,
                                   le=1.0, description="Relevance score (0-1)")
    relevance_reason: str = Field(...,
                                  description="Why this dataset is considered relevant")
    page_source_url: str = Field(...,
                                 description="The page URL where this link was found")
    is_current: bool = Field(
        True, description="Whether this appears to be the current/active version")
    estimated_date: Optional[str] = Field(
        None, description="Estimated effective date if detectable")


class NavigatorInput(BaseModel):
    """
    Input schema for the Navigator Agent.

    Designed for **AgentCore stateless execution**: the only required
    field is `portal_url`.  All other parameters have sensible defaults.
    No implicit context (state name, credentials, session) is expected.

    The agent returns **all** relevant datasets it discovers — there is
    no top-k limit.  Crawl depth is managed internally by the crawl tool.

    AgentCore payload mapping:
        payload["portal_url"]  →  portal_url   (required)
        payload["category"]    →  dataset_category
    """

    portal_url: str = Field(
        ...,
        description="Root URL of the state Medicaid fee-schedule portal (only required input).",
        examples=[
            "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html",
            "https://ahca.myflorida.com/medicaid/cost-reimbursement-and-auditing",
        ],
    )
    dataset_category: str = Field(
        "all",
        description="Target dataset category or 'all' for everything.",
    )

    # ── Validators ────────────────────────────────────────────────────────

    @field_validator("portal_url")
    @classmethod
    def validate_portal_url(cls, v: str) -> str:
        """
        Ensure portal_url is a well-formed HTTP(S) URL.

        Raises:
            ValueError: If the URL scheme is not http/https,
                        the hostname is missing, or the string is empty.
        """
        v = v.strip()
        if not v:
            raise ValueError("portal_url must not be empty")
        parsed = urlparse(v)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(
                f"portal_url must start with http:// or https:// — got '{v}'"
            )
        if not parsed.netloc:
            raise ValueError(f"portal_url has no hostname — got '{v}'")
        return v


class NavigatorOutput(BaseModel):
    """
    Structured output from the Navigator Agent.

    Every invocation produces this schema — whether the crawl succeeded
    or failed.  Downstream consumers should check `success` first.
    `state_name` / `state_code` are **output-only** fields inferred by
    the agent from the portal content (not from input).
    """

    success: bool = Field(
        True, description="False when the crawl or analysis failed entirely.")
    portal_url: str = Field(
        ..., description="The portal URL that was crawled.")
    state_name: str = Field(
        "", description="State name inferred from portal content (output-only).")
    state_code: str = Field(
        "", description="Two-letter state code inferred from portal (output-only).")
    total_links_discovered: int = Field(
        0, description="Total downloadable links found during crawl.")
    relevant_datasets: list[RankedDataset] = Field(default_factory=list)
    crawled_pages: list[str] = Field(default_factory=list)
    portal_type: str = Field(
        "unknown", description="Detected portal type: sharepoint, drupal, custom, etc.")
    errors: list[str] = Field(default_factory=list)
    crawl_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat())

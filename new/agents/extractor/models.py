"""
Extractor Agent Models
----------------------
Pydantic models for Extractor Agent input/output and extracted data structures.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Enums ─────────────────────────────────────────────────────────────────────


class FileType(str, Enum):
    """Supported file types for extraction."""
    PDF = "pdf"
    XLS = "xls"
    XLSX = "xlsx"
    CSV = "csv"
    ZIP = "zip"
    UNKNOWN = "unknown"


class DatasetCategory(str, Enum):
    """Medicaid service categories."""
    PHYSICIAN = "physician"
    DENTAL = "dental"
    PHARMACY = "pharmacy"
    DMEPOS = "dmepos"
    OUTPATIENT = "outpatient"
    INPATIENT = "inpatient"
    BEHAVIORAL_HEALTH = "behavioral_health"
    LABORATORY = "laboratory"
    VISION = "vision"
    HOME_HEALTH = "home_health"
    GENERAL = "general"
    UNKNOWN = "unknown"


# ── Input Model ───────────────────────────────────────────────────────────────


class ExtractorInput(BaseModel):
    """
    Input for the Extractor Agent.

    Typically receives a dataset discovered by the Navigator Agent.
    """

    # Required fields
    url: str = Field(
        ...,
        description="Direct download URL of the dataset file"
    )

    # Optional metadata from Navigator
    state_name: str = Field(
        default="",
        description="State name (e.g., 'Alaska')"
    )

    state_code: str = Field(
        default="",
        description="Two-letter state code (e.g., 'AK')"
    )

    file_type: FileType = Field(
        default=FileType.UNKNOWN,
        description="Expected file type"
    )

    category: DatasetCategory = Field(
        default=DatasetCategory.UNKNOWN,
        description="Expected dataset category"
    )

    title: str = Field(
        default="",
        description="Descriptive title of the dataset"
    )

    # Extraction configuration
    extract_all_sheets: bool = Field(
        default=True,
        description="For Excel files: extract all sheets or just the first"
    )

    ocr_enabled: bool = Field(
        default=False,
        description="Enable OCR for scanned PDFs (slower but more accurate)"
    )


# ── Extracted Table ───────────────────────────────────────────────────────────


class ExtractedTable(BaseModel):
    """
    A single extracted table from a file.

    For Excel files, one table per sheet.
    For CSV files, one table.
    For PDF files, one or more tables per page.
    """

    sheet_name: str = Field(
        ...,
        description="Sheet name (Excel) or 'Page N' (PDF) or filename (CSV)"
    )

    headers: list[str] = Field(
        ...,
        description="Column headers detected in the table"
    )

    data: list[dict[str, Any]] = Field(
        ...,
        description="Rows as list of dictionaries (column -> value)"
    )

    row_count: int = Field(
        ...,
        description="Number of data rows (excluding header)"
    )

    detected_header_row: int = Field(
        default=0,
        description="Zero-indexed row number where headers were found"
    )

    footer_notes: str | None = Field(
        default=None,
        description="Any footer text or disclaimers found"
    )

    # Column mapping (Phase 2: Analysis)
    column_mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Raw column name -> canonical column name mapping"
    )

    mapping_confidence: float = Field(
        default=0.0,
        description="Confidence score (0.0-1.0) for the column mapping"
    )


# ── Output Model ──────────────────────────────────────────────────────────────


class ExtractorOutput(BaseModel):
    """
    Output from the Extractor Agent.

    Contains extracted tables, metadata, column mappings, and quality metrics.
    """

    # Success indicator
    success: bool = Field(
        default=True,
        description="Whether extraction completed successfully"
    )

    # Source metadata
    source_url: str = Field(
        ...,
        description="The URL that was processed"
    )

    state_name: str = Field(
        default="",
        description="State name"
    )

    state_code: str = Field(
        default="",
        description="Two-letter state code"
    )

    file_type: FileType = Field(
        ...,
        description="Detected file type"
    )

    category: DatasetCategory = Field(
        default=DatasetCategory.UNKNOWN,
        description="Dataset category"
    )

    # Extracted data
    extracted_tables: list[ExtractedTable] = Field(
        default_factory=list,
        description="All tables extracted from the file"
    )

    # File metadata
    file_size_bytes: int = Field(
        default=0,
        description="Downloaded file size"
    )

    download_timestamp: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z",
        description="ISO-8601 timestamp when file was downloaded"
    )

    file_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata from file (effective dates, version, etc.)"
    )

    # Analysis results (Phase 2)
    schema_drift_detected: bool = Field(
        default=False,
        description="Whether schema differs from expected canonical schema"
    )

    schema_drift_details: list[str] = Field(
        default_factory=list,
        description="Details about detected schema changes"
    )

    data_quality_issues: list[str] = Field(
        default_factory=list,
        description="Data quality warnings (missing codes, invalid rates, etc.)"
    )

    # Errors
    errors: list[str] = Field(
        default_factory=list,
        description="Any errors encountered during extraction/analysis"
    )

    # Processing stats
    total_rows_extracted: int = Field(
        default=0,
        description="Total number of data rows across all tables"
    )

    processing_time_seconds: float = Field(
        default=0.0,
        description="Time taken to extract and analyze"
    )


# ── Canonical Schema Definition ──────────────────────────────────────────────


class CanonicalSchema(BaseModel):
    """
    The expected canonical schema for Medicaid fee schedule data.

    All raw columns should be mapped to these standard fields.
    Based on the Extractor Agent system prompt.
    """

    # Core fields (required)
    procedure_code: str = Field(
        ...,
        description="CPT, HCPCS, CDT, or NDC code"
    )

    description: str = Field(
        ...,
        description="Procedure/service description"
    )

    reimbursement_rate: float = Field(
        ...,
        description="Reimbursement amount in dollars"
    )

    # Optional fields
    modifier: str | None = Field(
        default=None,
        description="Procedure modifier code"
    )

    effective_date: str | None = Field(
        default=None,
        description="Date when rate became effective (YYYY-MM-DD format)"
    )

    end_date: str | None = Field(
        default=None,
        description="Date when rate expires/expired (YYYY-MM-DD format)"
    )

    unit_type: str | None = Field(
        default=None,
        description="Unit of service (per procedure, per day, per unit, etc.)"
    )

    place_of_service: str | None = Field(
        default=None,
        description="Where service is provided (office, hospital, etc.)"
    )

    provider_type: str | None = Field(
        default=None,
        description="Type of provider eligible for this rate"
    )

    notes: str | None = Field(
        default=None,
        description="Additional notes or restrictions"
    )

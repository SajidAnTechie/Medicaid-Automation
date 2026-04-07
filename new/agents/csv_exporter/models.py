"""
Pydantic models for CSV Exporter Agent.
"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class CSVExporterInput(BaseModel):
    """Input for CSV Exporter Agent."""
    
    source_url: str = Field(..., description="URL of the original file")
    file_type: str = Field(..., description="File extension (xlsx, csv, etc.)")
    sheet_name: str = Field(..., description="Sheet name for Excel files")
    column_mapping: Dict[str, str] = Field(..., description="Dict mapping original columns to canonical names")
    state_name: str = Field(..., description="State name")
    state_code: str = Field(default="", description="State code (e.g., AK)")
    category: str = Field(..., description="Category (physician, dental, etc.)")
    download_timestamp: str = Field(..., description="Timestamp of extraction")
    output_path: str = Field(..., description="Path to save the CSV file")
    cached_file_path: Optional[str] = Field(default=None, description="Optional path to already downloaded file")


class CSVExporterOutput(BaseModel):
    """Output from CSV Exporter Agent."""
    
    success: bool = Field(..., description="Whether export was successful")
    rows_exported: int = Field(default=0, description="Number of rows exported")
    columns: List[str] = Field(default_factory=list, description="List of column names in output CSV")
    output_path: str = Field(default="", description="Path to the exported CSV file")
    error: Optional[str] = Field(default=None, description="Error message if export failed")
    metadata: Dict = Field(default_factory=dict, description="Additional metadata about the export")

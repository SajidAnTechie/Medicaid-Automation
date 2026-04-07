"""agents.extractor
-----------------
Strands-powered Extractor Agent ("The Parser") that downloads Medicaid
fee schedule files discovered by Navigator, parses multiple formats
(Excel, CSV, PDF, ZIP), and maps columns to canonical schema.
"""

from .agent import create_extractor_agent, run_extractor, save_extracted_data_to_csv
from .models import (
    DatasetCategory,
    ExtractorInput,
    ExtractorOutput,
    ExtractedTable,
    FileType,
)

__all__ = [
    "create_extractor_agent",
    "run_extractor",
    "save_extracted_data_to_csv",
    "ExtractorInput",
    "ExtractorOutput",
    "ExtractedTable",
    "FileType",
    "DatasetCategory",
]

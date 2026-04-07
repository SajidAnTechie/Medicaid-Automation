"""
Extractor Agent — "The Parser & Analyst"
----------------------------------------
Strands-agents powered agent that:
  Phase 1: Downloads and parses Medicaid fee schedule files (Excel, CSV, PDF, ZIP)
  Phase 2: Maps raw columns to canonical schema and validates data quality
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from strands import Agent
from strands.models.bedrock import BedrockModel

from .models import (
    ExtractorInput,
    ExtractorOutput,
    ExtractedTable,
    FileType,
)
from .tools import download_file, parse_file, map_columns, export_raw_csv

logger = logging.getLogger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────────


EXTRACTOR_SYSTEM_PROMPT = """
You are the **Extractor Agent** (alias: "The Data Miner") in the Sentinel-State pipeline.
Your mission is to download files, extract structured data from ANY tabular content,
and output clean, raw structured records for downstream processing.

## CONTEXT
You receive file URLs from the Navigator Agent. Your job is to:
1. Download and parse each file (Excel, CSV, PDF)
2. Extract metadata and identify ALL tables in the file
3. Analyze which table is most relevant for fee schedule data
4. Transform and clean the data into a standardized structured format

**IMPORTANT:** Do NOT assume specific required columns exist. Extract ALL data found
in whatever structure it appears. The goal is to produce a raw structured file 
that preserves all information from the source.

═══════════════════════════════════════════════════════════════════════════════
## WORKFLOW (Execute each step and sub-step in strict order)
═══════════════════════════════════════════════════════════════════════════════

### STEP 1: DOWNLOAD FILE
────────────────────────────────────────────────────────────────────────────────
**1.1 Validate URL**
   - Confirm URL is accessible (HTTP 200)
   - Check Content-Type header matches expected file type
   - Handle redirects (follow up to 5 redirects)

**1.2 Download File Content**
   - Stream download for large files (>10MB)
   - Set timeout: 60 seconds for standard files, 120 seconds for large files
   - Verify file is not corrupted (check file signature/magic bytes)

**1.3 Determine File Type**
   - Primary: Use file extension (.xlsx, .xls, .csv, .pdf, .zip)
   - Fallback: Inspect magic bytes if extension is ambiguous
   - For ZIP: Extract contents and identify processable files

**1.4 Record Download Metadata**
   - File size (bytes)
   - Download timestamp (ISO-8601 UTC)
   - Source URL
   - Content-Type from response headers

### STEP 2: EXTRACT FILE METADATA (Chunked Processing for Large Files)
────────────────────────────────────────────────────────────────────────────────
**2.1 Scan File Data in Chunks (TOKEN MANAGEMENT)**
   
   **For Excel files:**
   - Read sheet names first (without loading full data)
   - For each sheet: Read first 50 rows as sample
   - Process one sheet at a time to avoid memory/token issues
   - Note: Header row may NOT be row 1 (check for title rows, merged cells)
   
   **For CSV files:**
   - Read first 100 rows as sample
   - Auto-detect delimiter (comma, tab, pipe, semicolon)
   - Auto-detect encoding (UTF-8, Latin-1, Windows-1252)
   - Handle multi-line quoted fields
   
   **For PDF files:**
   - Extract total page count first
   - Process 5-10 pages at a time for table extraction/OCR
   - Prioritize pages with visible table structures
   - Handle both text-based and scanned (image) PDFs

**2.2 Extract Information of the File**

   **2.2.1 File Name Analysis**
   Parse filename to extract hints:
   - State identifier (e.g., "AK", "Alaska", "California")
   - Date/year indicators (e.g., "FY2025", "CY2024", "2025", "Jan2025")
   - Document type hints (e.g., "fee", "schedule", "rates", "physician")
   - Version indicators (e.g., "v2", "revised", "final", "draft")
   - Category hints (e.g., "professional", "dental", "pharmacy")
   
   **2.2.2 File Data Summary**
   Generate comprehensive statistics:
   - Total sheets/pages in file
   - Total tables detected across all sheets/pages
   - Estimated total data rows (excluding headers/footers)
   - Column count per table
   - Data density (% of non-empty cells)
   - Detected data patterns (numeric columns, date columns, code columns)
   
   **2.2.3 All Tables Found in File**
   For EACH table/sheet discovered, capture:
   ```
   - table_id: Unique identifier (sheet name, "Table_1", "Page3_Table1")
   - location: Where found (sheet name, page number, cell range A1:Z100)
   - row_count: Total rows including header
   - column_count: Number of columns
   - raw_headers: Original column headers exactly as they appear
   - header_row_index: Which row contains headers (0-based)
   - data_start_row: First row of actual data (0-based)
   - sample_rows: First 5 data rows (as arrays)
   - has_merged_cells: Boolean
   - has_multi_row_header: Boolean (header spans multiple rows)
   - notes: Any special observations (empty columns, repeating patterns)
   ```

**2.3 Store Extracted Metadata (Structured JSON)**
   Compile all metadata into structured format:
   ```json
   {
       "file_name": "<original filename>",
       "file_type": "<xlsx|xls|csv|pdf>",
       "file_size_bytes": <int>,
       "download_timestamp": "<ISO-8601 UTC>",
       "source_url": "<URL>",
       "file_summary": {
           "total_sheets": <int>,
           "total_tables_found": <int>,
           "estimated_total_rows": <int>,
           "file_name_hints": {
               "state": "<detected state or null>",
               "year": "<detected year or null>",
               "document_type": "<detected type or null>"
           }
       },
       "tables": [
           {
               "table_id": "<identifier>",
               "location": "<location reference>",
               "row_count": <int>,
               "column_count": <int>,
               "raw_headers": ["col1", "col2", ...],
               "header_row_index": <int>,
               "data_start_row": <int>,
               "sample_rows": [[...], [...], ...],
               "has_merged_cells": <bool>,
               "has_multi_row_header": <bool>
           }
       ]
   }
   ```


"""

# ── Factory ───────────────────────────────────────────────────────────────────


def create_extractor_agent(
    model_id: str = "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
    region: str = "eu-north-1",
) -> Agent:
    """
    Factory that builds a Strands Extractor Agent wired to Amazon Bedrock.

    Args:
        model_id: Bedrock model identifier.
                  Default: 'eu.anthropic.claude-3-5-sonnet-20241022-v2:0'
        region:   AWS region for the Bedrock runtime.
                  Default: 'eu-north-1'

    Returns:
        Agent: A ready-to-invoke Strands Agent instance.
    """

    bedrock_model = BedrockModel(
        model_id=model_id,
        region_name=region,
        temperature=0.1,
        max_tokens=16384,  # Larger for table data
    )

    return Agent(
        model=bedrock_model,
        system_prompt=EXTRACTOR_SYSTEM_PROMPT,
        tools=[download_file, parse_file, map_columns, export_raw_csv],
    )


# ── Runner ────────────────────────────────────────────────────────────────────


def _format_warnings_as_strings(warnings: list) -> list[str]:
    """
    Convert warning dictionaries to strings for Pydantic validation.
    
    Args:
        warnings: List of warning dictionaries with row, column, issue fields
    
    Returns:
        List of formatted warning strings
    """
    result = []
    for warning in warnings:
        if isinstance(warning, dict):
            row = warning.get("row", "?")
            column = warning.get("column", "?")
            issue = warning.get("issue", "Unknown issue")
            result.append(f"Row {row}, Column '{column}': {issue}")
        elif isinstance(warning, str):
            result.append(warning)
        else:
            result.append(str(warning))
    return result


def run_extractor(input_data: ExtractorInput) -> ExtractorOutput:
    """
    Execute the Extractor Agent end-to-end for a single fee schedule file.

    Workflow:
      1. download_file  — downloads from URL to temp storage
      2. parse_file     — extracts tables from Excel/CSV/PDF/ZIP
      3. map_columns    — LLM maps raw columns to canonical schema
      4. validate       — checks data quality
      5. JSON response  — returns structured ExtractorOutput

    Args:
        input_data: ExtractorInput with URL and optional metadata

    Returns:
        ExtractorOutput with extracted tables, column mappings, and quality metrics
    """
    start_time = time.time()
    agent = create_extractor_agent()

    user_prompt = f"""Extract and analyze the Medicaid fee schedule file following the workflow.

**Source Information:**
- URL: {input_data.url}
- State: {input_data.state_name} ({input_data.state_code})
- Expected Type: {input_data.file_type.value}
- Category: {input_data.category.value}
- Title: {input_data.title}

**Execute the 4-step workflow in strict order:**

═══════════════════════════════════════════════════════════════════════════════
STEP 1: DOWNLOAD FILE
═══════════════════════════════════════════════════════════════════════════════
Call `download_file` tool with:
- url: "{input_data.url}"

You will receive:
- Downloaded file path
- File size in bytes
- Download timestamp
- Content type

═══════════════════════════════════════════════════════════════════════════════
STEP 2: EXTRACT FILE METADATA (Chunked Processing)
═══════════════════════════════════════════════════════════════════════════════
Call `parse_file` tool with the downloaded file path.

**IMPORTANT**: The tool returns SAMPLE DATA ONLY (5 rows) to avoid token limits.

You will receive for each table/sheet:
- table_id (sheet name or page reference)
- location (where found in file)
- row_count (TOTAL rows in original file)
- column_count
- raw_headers (original column names exactly as they appear)
- header_row_index (which row has headers, 0-based)
- data_start_row (first data row, 0-based)
- sample_rows (only 5 rows as sample)
- has_merged_cells
- has_multi_row_header

═══════════════════════════════════════════════════════════════════════════════
STEP 3: ANALYZE METADATA & SELECT TABLE
═══════════════════════════════════════════════════════════════════════════════
Review the extracted metadata and:

3.1 For each table, analyze for fee schedule patterns:
   - Code-like columns (CPT, HCPCS, CDT, NDC patterns)
   - Numeric/currency columns (rates, fees, amounts)
   - Description columns (text fields)
   - Date columns (effective dates)

3.2 Calculate relevance score (0.0-1.0):
   - +0.3 if has code-like columns
   - +0.3 if has currency/rate columns
   - +0.2 if has description columns
   - +0.1 if sheet name suggests fee schedule
   - +0.1 if reasonable structure

3.3 Select table to process:
   - If only one table → Select it
   - If multiple → Select highest scoring table
   - ALWAYS process at least one table (never reject)


═══════════════════════════════════════════════════════════════════════════════
FINAL OUTPUT: Return ONLY this JSON structure (no markdown, no fences)
═══════════════════════════════════════════════════════════════════════════════

{{
    "status": "success",
    "source_file": "<filename from URL>",
    "source_url": "{input_data.url}",
    "file_type": "<xlsx|xls|csv|pdf>",
    "extraction_metadata": {{
        "download_timestamp": "<ISO-8601 UTC format>",
        "file_size_bytes": <integer>,
        "tables_found": <total tables discovered>,
        "table_selected": "<selected table_id>",
        "header_row": <0-based header row index>,
        "data_start_row": <0-based data start row>,
        "total_rows_processed": <TOTAL rows in original file, not just sample>
    }},
    "schema": {{
        "columns": ["canonical_col1", "canonical_col2", ...],
        "original_headers": ["Original Col 1", "Original Col 2", ...],
        "data_types": {{"canonical_col1": "string", "canonical_col2": "number", ...}}
    }},
    "header_mapping": [
        {{"original": "Original Col 1", "canonical": "canonical_col1", "index": 0}},
        {{"original": "Original Col 2", "canonical": "canonical_col2", "index": 1}}
    ],
    "data_summary": {{
        "total_records": <TOTAL rows in original file>,
        "records_extracted": 5,
        "records_skipped": 0
    }},
    "records": [
        {{"canonical_col1": "value1", "canonical_col2": 123.45}},
        {{"canonical_col1": "value2", "canonical_col2": 150.00}},
        (only 5 sample records using canonical column names)
    ],
    "extraction_notes": [
        "Header found at row 2 (row 0-1 were title rows)",
        "Selected table 'Sheet1' with highest relevance score 0.9",
        "Sample of 5 rows shown, total dataset has <N> rows"
    ],
    "warnings": [
        {{"row": 3, "column": "rate", "issue": "Non-numeric value 'N/A' converted to null"}},
        {{"row": 5, "column": "procedure_code", "issue": "Header may be in earlier rows"}}
    ],
    "errors": []
}}

**CRITICAL REQUIREMENTS:**
1. Return ONLY the JSON object (no ```json fences, no explanatory text)
2. Use canonical column names in "records" array
3. Include only 5 sample records (not all data)
4. Set total_records to actual row count from file
5. Convert warnings to dict format with row, column, issue keys
6. Ensure all JSON is valid (proper escaping, no trailing commas)
"""

    try:
        result = agent(user_prompt)
    except Exception as exc:  # noqa: BLE001
        logger.error("Agent execution failed: %s", exc)
        return ExtractorOutput(
            success=False,
            source_url=input_data.url,
            state_name=input_data.state_name,
            state_code=input_data.state_code,
            file_type=input_data.file_type,
            errors=[f"Agent execution error: {exc}"],
            processing_time_seconds=time.time() - start_time,
        )

    try:
        response_text = str(result)
        
        # Log the raw response for debugging
        print("=" * 80)
        print("RAW AGENT RESPONSE:")
        print(response_text)
        print("=" * 80)
        
        logger.debug("Raw agent response length: %d", len(response_text))

        parsed = _extract_json_from_response(response_text)

        # Handle the new output format from the updated system prompt
        # Build ExtractedTable objects
        tables: list[ExtractedTable] = []
        total_rows = 0
        
        # Check if using new format (schema, header_mapping, records)
        if "schema" in parsed and "header_mapping" in parsed and "records" in parsed:
            # New format from updated system prompt
            schema = parsed["schema"]
            header_mapping = parsed.get("header_mapping", [])
            records = parsed.get("records", [])
            
            # Build column mapping dict from header_mapping array
            column_mapping = {}
            for mapping in header_mapping:
                column_mapping[mapping["original"]] = mapping["canonical"]
            
            table = ExtractedTable(
                sheet_name=parsed.get("extraction_metadata", {}).get("table_selected", "Sheet1"),
                headers=schema.get("original_headers", []),
                data=records[:5],  # Only first 5 records
                row_count=parsed.get("data_summary", {}).get("total_records", len(records)),
                detected_header_row=parsed.get("extraction_metadata", {}).get("header_row", 0),
                footer_notes=None,
                column_mapping=column_mapping,
                mapping_confidence=0.9,  # Default high confidence
            )
            tables.append(table)
            total_rows = table.row_count
            
        # Fallback to old format
        elif "extracted_tables" in parsed:
            for tbl in parsed.get("extracted_tables", []):
                table = ExtractedTable(
                    sheet_name=tbl["sheet_name"],
                    headers=tbl["headers"],
                    data=tbl["data"],
                    row_count=tbl["row_count"],
                    detected_header_row=tbl.get("detected_header_row", 0),
                    footer_notes=tbl.get("footer_notes"),
                    column_mapping=tbl.get("column_mapping", {}),
                    mapping_confidence=tbl.get("mapping_confidence", 0.0),
                )
                tables.append(table)
                total_rows += table.row_count

        return ExtractorOutput(
            success=True if parsed.get("status") == "success" else False,
            source_url=input_data.url,
            state_name=input_data.state_name,
            state_code=input_data.state_code,
            file_type=FileType(parsed.get("file_type", input_data.file_type.value)),
            category=input_data.category,
            extracted_tables=tables,
            file_size_bytes=parsed.get("extraction_metadata", {}).get("file_size_bytes", 0),
            file_metadata=parsed.get("extraction_metadata", {}),
            schema_drift_detected=False,
            schema_drift_details=[],
            data_quality_issues=_format_warnings_as_strings(parsed.get("warnings", [])),
            errors=parsed.get("errors", []),
            total_rows_extracted=total_rows,
            processing_time_seconds=time.time() - start_time,
        )
    
    except Exception as exc:  # noqa: BLE001
        logger.error("Error parsing extractor output: %s", exc)
        return ExtractorOutput(
            success=False,
            source_url=input_data.url,
            state_name=input_data.state_name,
            state_code=input_data.state_code,
            file_type=input_data.file_type,
            errors=[f"Output parsing error: {exc}"],
            processing_time_seconds=time.time() - start_time,
        )


# ── CSV Export Helper ─────────────────────────────────────────────────────────


def save_extracted_data_to_csv(output: ExtractorOutput, output_dir: str = "output") -> list[str]:
    """
    Save extracted table data to CSV files.
    
    Args:
        output: ExtractorOutput containing extracted tables
        output_dir: Directory to save CSV files (default: "output")
    
    Returns:
        List of CSV file paths created
    """
    import csv
    from pathlib import Path
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    csv_files = []
    
    # Generate base filename from state and URL
    state_key = output.state_code.lower() if output.state_code else output.state_name.lower().replace(' ', '_')
    if not state_key:
        state_key = "unknown"
    
    for table_idx, table in enumerate(output.extracted_tables):
        # Create CSV filename
        sheet_name_clean = table.sheet_name.replace(' ', '_').replace('/', '_')[:50]
        csv_filename = f"{state_key}_extracted_{sheet_name_clean}.csv"
        csv_path = output_path / csv_filename
        
        # Get canonical column names from column_mapping
        if table.column_mapping:
            # Use canonical names as headers
            canonical_headers = list(table.column_mapping.values())
            original_headers = list(table.column_mapping.keys())
        else:
            # Fallback to original headers
            canonical_headers = table.headers
            original_headers = table.headers
        
        # Write CSV file
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=canonical_headers)
                writer.writeheader()
                
                # Write the sample data we have
                for row in table.data:
                    # Map original headers to canonical headers
                    canonical_row = {}
                    for orig_header, canonical_header in table.column_mapping.items():
                        canonical_row[canonical_header] = row.get(orig_header)
                    writer.writerow(canonical_row)
            
            logger.info(f"Saved CSV: {csv_path} ({len(table.data)} sample rows)")
            csv_files.append(str(csv_path))
            
            # Also create a metadata file
            metadata_filename = f"{state_key}_extracted_{sheet_name_clean}_metadata.json"
            metadata_path = output_path / metadata_filename
            
            metadata = {
                "source_url": output.source_url,
                "state_name": output.state_name,
                "state_code": output.state_code,
                "sheet_name": table.sheet_name,
                "total_rows_in_source": table.row_count,
                "sample_rows_in_csv": len(table.data),
                "column_mapping": table.column_mapping,
                "original_headers": original_headers,
                "canonical_headers": canonical_headers,
                "detected_header_row": table.detected_header_row,
                "mapping_confidence": table.mapping_confidence,
                "extraction_timestamp": output.file_metadata.get("download_timestamp"),
                "note": f"This CSV contains {len(table.data)} sample rows. Full dataset has {table.row_count} rows."
            }
            
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Saved metadata: {metadata_path}")
            
        except Exception as e:
            logger.error(f"Failed to save CSV {csv_path}: {e}")
    
    return csv_files

# ── JSON extraction helper ────────────────────────────────────────────────────


def _extract_json_from_response(text: str) -> dict[str, Any]:
    """
    Extract JSON from agent response (same logic as Navigator).
    """
    import re
    
    # 1. Direct parse
    try:
        return json.loads(text)  # type: ignore[return-value]
    except json.JSONDecodeError:
        pass

    # 2. Strip markdown code block
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))  # type: ignore[return-value]
        except json.JSONDecodeError:
            pass

    # 3. Brace-matching fallback
    depth = 0
    start: int | None = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(text[start: i + 1])  # type: ignore[return-value]
                except json.JSONDecodeError:
                    start = None

    raise ValueError("No valid JSON object found in agent response")

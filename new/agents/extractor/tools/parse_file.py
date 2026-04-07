"""
parse_file tool
---------------
Parses downloaded files (Excel, CSV, PDF, ZIP) into structured table data.

IMPORTANT: To avoid token limits and JSON serialization issues:
- Returns only SAMPLE data (5 rows per table)
- Cleans all special characters
- Converts all values to strings
- Tracks total row count separately
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
from strands import tool

logger = logging.getLogger(__name__)


@tool
def parse_file(
    file_path: str,
    extract_all_sheets: bool = True,
    ocr_enabled: bool = False,
) -> dict[str, Any]:
    """
    Parse a downloaded file and extract comprehensive metadata about its structure.
    
    Returns detailed information about:
    - File name and type
    - All tables/sheets found in the file
    - Headers and data structure for each table
    - Sample data (5 rows per table)
    
    This supports the 4-step extraction workflow:
    1. Download file (already done)
    2. Extract file metadata (this tool)
    3. Analyze metadata (LLM does this)
    4. Process selected table (LLM does this)
    
    Args:
        file_path: Absolute path to the downloaded file
        extract_all_sheets: For Excel files, extract all sheets or just first
        ocr_enabled: Enable OCR for scanned PDFs (slower but more accurate)
    
    Returns:
        Dictionary with comprehensive file metadata:
        {
            "success": bool,
            "file_name": str,
            "file_type": str,
            "file_size_bytes": int,
            "file_summary": {
                "total_sheets": int,
                "total_tables_found": int,
                "estimated_total_rows": int
            },
            "tables": [
                {
                    "table_id": str,
                    "location": str,
                    "row_count": int,
                    "column_count": int,
                    "raw_headers": [str],
                    "header_row_index": int,
                    "data_start_row": int,
                    "sample_rows": [dict],
                    "has_merged_cells": bool,
                    "has_multi_row_header": bool
                }
            ]
        }
    """
    try:
        logger.info(f"Parsing file: {file_path}")
        
        # Extract file name from path
        file_name = Path(file_path).name
        file_size = os.path.getsize(file_path)
        
        # Detect file type
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in [".xlsx", ".xls"]:
            result = _parse_excel(file_path, extract_all_sheets)
        elif file_ext == ".csv":
            result = _parse_csv(file_path)
        elif file_ext == ".pdf":
            result = _parse_pdf(file_path, ocr_enabled)
        elif file_ext == ".zip":
            result = _parse_zip(file_path, extract_all_sheets, ocr_enabled)
        else:
            return {
                "success": False,
                "error": f"Unsupported file type: {file_ext}",
            }
        
        # Add file-level metadata to result
        if result.get("success"):
            result["file_name"] = file_name
            result["file_size_bytes"] = file_size
            
            # Calculate file summary
            tables = result.get("tables", [])
            total_rows = sum(t.get("row_count", 0) for t in tables)
            
            result["file_summary"] = {
                "total_sheets": len(tables),
                "total_tables_found": len(tables),
                "estimated_total_rows": total_rows,
            }
        
        return result
    
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return {
            "success": False,
            "error": f"Parse error: {e}",
        }


# ── Excel parsing ─────────────────────────────────────────────────────────────


def _parse_excel(file_path: str, extract_all_sheets: bool) -> dict[str, Any]:
    """
    Parse Excel file and return comprehensive metadata.
    
    Returns metadata matching the extraction flow:
    - File name analysis
    - File data summary
    - All tables found in file with full structure info
    """
    tables = []
    MAX_SAMPLE_ROWS = 5  # Only return 5 sample rows to LLM
    
    # Read all sheets
    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names
    
    if not extract_all_sheets:
        sheet_names = sheet_names[:1]  # Only first sheet
    
    for sheet_name in sheet_names:
        try:
            # Read sheet WITHOUT header to analyze raw structure
            df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            
            # SMART HEADER DETECTION
            header_row = _detect_header_row(df_raw)
            
            logger.info(f"Detected header row at index {header_row} for sheet '{sheet_name}'")
            
            # Re-read with correct header row
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
            
            # Clean column names - remove special characters
            raw_headers = [str(col).strip().replace('\n', ' ').replace('\r', '') for col in df.columns]
            df.columns = raw_headers
            
            # Remove completely empty rows
            df = df.dropna(how="all")
            
            # Get total row count
            total_rows = len(df)
            data_start_row = header_row + 1  # Data starts after header
            
            # Take only first 5 rows as sample
            df_sample = df.head(MAX_SAMPLE_ROWS)
            
            # Convert to simple list of dicts - clean all values
            sample_data = []
            for _, row in df_sample.iterrows():
                row_dict = {}
                for col in df_sample.columns:
                    value = row[col]
                    # Convert to clean string or None
                    if pd.isna(value):
                        row_dict[col] = None
                    else:
                        # Convert everything to string to avoid type issues
                        clean_val = str(value).strip()
                        if clean_val in ['', 'None', 'nan', 'NaN']:
                            row_dict[col] = None
                        else:
                            # Limit length to avoid huge values
                            row_dict[col] = clean_val[:200]
                sample_data.append(row_dict)
            
            # Build comprehensive table metadata
            table_metadata = {
                "table_id": str(sheet_name)[:100],  # Unique identifier
                "location": f"Sheet: {sheet_name}",  # Where found in file
                "row_count": total_rows,  # TOTAL rows in original (not just sample)
                "column_count": len(raw_headers),
                "raw_headers": raw_headers[:50],  # Original headers exactly as they appear
                "header_row_index": header_row,  # 0-based index where headers found
                "data_start_row": data_start_row,  # 0-based index where data starts
                "sample_rows": sample_data,  # First 5 rows as sample
                "has_merged_cells": False,  # TODO: Detect merged cells if needed
                "has_multi_row_header": False,  # TODO: Detect multi-row headers if needed
            }
            
            tables.append(table_metadata)
            
            logger.info(f"Extracted sheet '{sheet_name}': {total_rows} rows, {len(raw_headers)} columns (returning {len(sample_data)} sample rows)")
        
        except Exception as e:
            logger.warning(f"Failed to parse sheet '{sheet_name}': {e}")
    
    return {
        "success": True,
        "tables": tables,
        "file_type": "xlsx" if file_path.endswith(".xlsx") else "xls",
        "metadata": {},
    }


def _detect_header_row(df_raw: pd.DataFrame) -> int:
    """
    Intelligently detect which row contains the actual headers.
    
    Rules:
    1. Check first 15 rows
    2. Look for row with most non-empty text values
    3. Avoid rows with:
       - Single merged cell (likely a title)
       - All numeric values (likely data, not headers)
       - Very few non-null values
    4. Prefer rows where values look like column names (contain letters, reasonable length)
    
    Args:
        df_raw: DataFrame read without header
    
    Returns:
        0-based index of detected header row
    """
    max_rows_to_check = min(15, len(df_raw))
    best_header_row = 0
    best_score = -1
    
    for row_idx in range(max_rows_to_check):
        row_data = df_raw.iloc[row_idx]
        
        # Count non-null values
        non_null_count = row_data.notna().sum()
        
        # Skip if too few values (likely not a header)
        if non_null_count < len(df_raw.columns) * 0.3:
            continue
        
        # Count text values (headers should be mostly text)
        text_count = 0
        single_value_row = False
        all_numeric = True
        
        for val in row_data:
            if pd.notna(val):
                val_str = str(val).strip()
                
                # Check if this looks like header text
                if len(val_str) > 0:
                    # Headers typically have letters, not just numbers
                    if any(c.isalpha() for c in val_str):
                        text_count += 1
                        all_numeric = False
                    
                    # Check for single merged cell (title row)
                    if non_null_count == 1 and len(val_str) > 30:
                        single_value_row = True
                        break
        
        # Skip single-value rows (likely titles)
        if single_value_row:
            continue
        
        # Skip all-numeric rows (likely data)
        if all_numeric and non_null_count > 0:
            continue
        
        # Calculate score: prefer rows with more text values
        score = text_count
        
        # Bonus for having most of the columns filled
        if non_null_count > len(df_raw.columns) * 0.7:
            score += 10
        
        # Update best if this row scores higher
        if score > best_score:
            best_score = score
            best_header_row = row_idx
    
    return best_header_row


# ── CSV parsing ───────────────────────────────────────────────────────────────


def _parse_csv(file_path: str) -> dict[str, Any]:
    """Parse CSV file and return comprehensive metadata."""
    tables = []
    MAX_SAMPLE_ROWS = 5  # Only return 5 sample rows
    
    try:
        # Try common delimiters
        for delimiter in [",", "\t", "|", ";"]:
            try:
                df = pd.read_csv(file_path, delimiter=delimiter, encoding="utf-8", nrows=10)
                if len(df.columns) > 1:  # Successful parse
                    break
            except:
                continue
        else:
            # Try with encoding detection
            df = pd.read_csv(file_path, encoding="latin-1", nrows=10)
        
        # Count total rows
        with open(file_path, 'r') as f:
            total_rows = sum(1 for _ in f) - 1  # Subtract header
        
        # Clean column names
        raw_headers = [str(col).strip().replace('\n', ' ').replace('\r', '') for col in df.columns]
        df.columns = raw_headers
        
        # Take only first 5 rows
        df = df.head(MAX_SAMPLE_ROWS).dropna(how="all")
        
        # Convert to clean strings
        sample_data = []
        for _, row in df.iterrows():
            row_dict = {}
            for col in df.columns:
                value = row[col]
                if pd.isna(value):
                    row_dict[col] = None
                else:
                    clean_val = str(value).strip()
                    row_dict[col] = clean_val[:200] if clean_val not in ['', 'None', 'nan'] else None
            sample_data.append(row_dict)
        
        # Build comprehensive table metadata
        table_metadata = {
            "table_id": Path(file_path).stem[:100],
            "location": "CSV File",
            "row_count": total_rows,
            "column_count": len(raw_headers),
            "raw_headers": raw_headers[:50],
            "header_row_index": 0,
            "data_start_row": 1,
            "sample_rows": sample_data,
            "has_merged_cells": False,
            "has_multi_row_header": False,
        }
        
        tables.append(table_metadata)
        
        logger.info(f"Extracted CSV: {total_rows} rows, {len(raw_headers)} columns (returning {len(sample_data)} samples)")
        
        return {
            "success": True,
            "tables": tables,
            "file_type": "csv",
            "metadata": {},
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": f"CSV parse error: {e}",
        }


# ── PDF parsing ───────────────────────────────────────────────────────────────


def _parse_pdf(file_path: str, ocr_enabled: bool) -> dict[str, Any]:
    """Parse PDF file and return comprehensive metadata."""
    try:
        import pdfplumber
    except ImportError:
        return {
            "success": False,
            "error": "pdfplumber not installed. Run: pip install pdfplumber",
        }
    
    tables = []
    MAX_SAMPLE_ROWS = 5  # Only return 5 sample rows per table
    MAX_TABLES = 3  # Only return first 3 tables
    
    try:
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            logger.info(f"Processing PDF with {total_pages} pages")
            
            tables_found = 0
            
            for page_num, page in enumerate(pdf.pages, start=1):
                if tables_found >= MAX_TABLES:
                    break
                
                # Extract tables from page
                page_tables = page.extract_tables()
                
                for table_num, table_data in enumerate(page_tables, start=1):
                    if tables_found >= MAX_TABLES:
                        break
                        
                    if not table_data or len(table_data) < 2:
                        continue
                    
                    # First row as headers - clean them
                    raw_headers = [str(h or f"Column_{i}").strip().replace('\n', ' ')[:100] 
                                  for i, h in enumerate(table_data[0])]
                    rows = table_data[1:]
                    
                    # Limit rows
                    total_rows = len(rows)
                    sample_rows = rows[:MAX_SAMPLE_ROWS]
                    
                    # Convert to clean dicts
                    sample_data = []
                    for row in sample_rows:
                        row_dict = {}
                        for i, header in enumerate(raw_headers):
                            value = row[i] if i < len(row) else None
                            if value is not None:
                                clean_val = str(value).strip()
                                row_dict[header] = clean_val[:200] if clean_val not in ['', 'None'] else None
                            else:
                                row_dict[header] = None
                        sample_data.append(row_dict)
                    
                    table_id = f"Page_{page_num}"
                    if len(page_tables) > 1:
                        table_id += f"_Table_{table_num}"
                    
                    location = f"Page {page_num}"
                    if len(page_tables) > 1:
                        location += f", Table {table_num}"
                    
                    # Build comprehensive table metadata
                    table_metadata = {
                        "table_id": table_id,
                        "location": location,
                        "row_count": total_rows,
                        "column_count": len(raw_headers),
                        "raw_headers": raw_headers[:50],
                        "header_row_index": 0,
                        "data_start_row": 1,
                        "sample_rows": sample_data,
                        "has_merged_cells": False,
                        "has_multi_row_header": False,
                    }
                    
                    tables.append(table_metadata)
                    
                    tables_found += 1
                    logger.info(f"Extracted {location}: {total_rows} rows (returning {len(sample_data)} samples)")
        
        logger.info(f"Extracted {len(tables)} tables from PDF (showing first {MAX_TABLES})")
        
        return {
            "success": True,
            "tables": tables,
            "file_type": "pdf",
            "metadata": {"total_pages": total_pages, "tables_shown": len(tables)},
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": f"PDF parse error: {e}",
        }


# ── ZIP parsing ───────────────────────────────────────────────────────────────


def _parse_zip(file_path: str, extract_all_sheets: bool, ocr_enabled: bool) -> dict[str, Any]:
    """Extract and parse files from ZIP archive."""
    import zipfile
    import tempfile
    
    all_tables = []
    
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            # Extract to temp directory
            temp_dir = tempfile.mkdtemp()
            zip_ref.extractall(temp_dir)
            
            # Process each extracted file
            for root, dirs, files in os.walk(temp_dir):
                for filename in files:
                    file_path_inner = os.path.join(root, filename)
                    ext = Path(filename).suffix.lower()
                    
                    if ext in [".xlsx", ".xls"]:
                        result = _parse_excel(file_path_inner, extract_all_sheets)
                    elif ext == ".csv":
                        result = _parse_csv(file_path_inner)
                    elif ext == ".pdf":
                        result = _parse_pdf(file_path_inner, ocr_enabled)
                    else:
                        continue  # Skip unsupported files
                    
                    if result.get("success"):
                        # Prefix sheet names with filename
                        for table in result.get("tables", []):
                            table["sheet_name"] = f"{filename} - {table['sheet_name']}"
                            all_tables.append(table)
        
        return {
            "success": True,
            "tables": all_tables,
            "file_type": "zip",
            "metadata": {},
        }
    
    except Exception as e:
        return {
            "success": False,
            "error": f"ZIP parse error: {e}",
        }

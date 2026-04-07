"""
generate_raw_csv tool
---------------------
Generates raw CSV output from extracted Medicaid fee schedule data.
This tool is designed to be used by the Extractor Agent.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from strands import tool

logger = logging.getLogger(__name__)


@tool
def generate_raw_csv(
    file_path: str,
    sheet_name: str,
    header_row_index: int,
    state_code: str,
    output_dir: str = "output",
) -> dict[str, Any]:
    """
    Generate a raw CSV file from the source Excel/CSV file with all rows.
    
    This tool reads the full source file and generates a complete CSV
    with all data rows, using the detected header row index.
    
    Args:
        file_path: Path to the downloaded source file
        sheet_name: Name of the sheet to extract (for Excel files)
        header_row_index: 0-based index of the header row
        state_code: State code for naming the output file
        output_dir: Directory to save the output CSV (default: "output")
    
    Returns:
        Dictionary containing:
          - success: Boolean indicating success
          - csv_path: Path to the generated CSV file
          - row_count: Number of data rows written
          - column_count: Number of columns
          - file_size_bytes: Size of the generated CSV file
          - error: Error message if failed
    """
    try:
        import pandas as pd
        
        logger.info(f"Generating raw CSV from: {file_path}")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Detect file type
        file_ext = Path(file_path).suffix.lower()
        
        # Read the file
        if file_ext in [".xlsx", ".xls"]:
            # Read Excel file with detected header row
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row_index)
        elif file_ext == ".csv":
            # Read CSV file
            df = pd.read_csv(file_path, header=header_row_index)
        else:
            return {
                "success": False,
                "error": f"Unsupported file type: {file_ext}",
            }
        
        # Clean column names
        df.columns = [str(col).strip().replace('\n', ' ').replace('\r', '') for col in df.columns]
        
        # Remove completely empty rows
        df = df.dropna(how="all")
        
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sheet_name_clean = sheet_name.replace(' ', '_').replace('/', '_')[:50]
        csv_filename = f"{state_code}_raw_{sheet_name_clean}_{timestamp}.csv"
        csv_path = output_path / csv_filename
        
        # Convert all values to strings and clean
        for col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else "")
        
        # Write to CSV
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        # Get file size
        file_size = os.path.getsize(csv_path)
        
        logger.info(f"Generated raw CSV: {csv_path}")
        logger.info(f"  Rows: {len(df):,}")
        logger.info(f"  Columns: {len(df.columns)}")
        logger.info(f"  Size: {file_size:,} bytes")
        
        return {
            "success": True,
            "csv_path": str(csv_path),
            "row_count": len(df),
            "column_count": len(df.columns),
            "file_size_bytes": file_size,
            "columns": list(df.columns),
        }
    
    except Exception as e:
        logger.error(f"Failed to generate raw CSV: {e}")
        return {
            "success": False,
            "error": f"CSV generation error: {e}",
        }


@tool
def generate_canonical_csv(
    file_path: str,
    sheet_name: str,
    header_row_index: int,
    column_mapping: dict[str, str],
    state_code: str,
    output_dir: str = "output",
) -> dict[str, Any]:
    """
    Generate a CSV file with canonical column names from the source file.
    
    This tool reads the full source file and generates a CSV with:
    - All data rows from the source
    - Column names mapped to canonical format
    - Clean data values
    
    Args:
        file_path: Path to the downloaded source file
        sheet_name: Name of the sheet to extract (for Excel files)
        header_row_index: 0-based index of the header row
        column_mapping: Dictionary mapping original headers to canonical names
        state_code: State code for naming the output file
        output_dir: Directory to save the output CSV (default: "output")
    
    Returns:
        Dictionary containing:
          - success: Boolean indicating success
          - csv_path: Path to the generated CSV file
          - metadata_path: Path to the metadata JSON file
          - row_count: Number of data rows written
          - column_count: Number of columns
          - file_size_bytes: Size of the generated CSV file
          - error: Error message if failed
    """
    try:
        import pandas as pd
        import json
        
        logger.info(f"Generating canonical CSV from: {file_path}")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Detect file type
        file_ext = Path(file_path).suffix.lower()
        
        # Read the file
        if file_ext in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row_index)
        elif file_ext == ".csv":
            df = pd.read_csv(file_path, header=header_row_index)
        else:
            return {
                "success": False,
                "error": f"Unsupported file type: {file_ext}",
            }
        
        # Clean column names
        df.columns = [str(col).strip().replace('\n', ' ').replace('\r', '') for col in df.columns]
        
        # Remove completely empty rows
        df = df.dropna(how="all")
        
        # Apply column mapping (rename columns to canonical names)
        df = df.rename(columns=column_mapping)
        
        # Clean data values
        for col in df.columns:
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)
            # Convert empty strings back to None
            df[col] = df[col].replace('', None)
        
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sheet_name_clean = sheet_name.replace(' ', '_').replace('/', '_')[:50]
        csv_filename = f"{state_code}_canonical_{sheet_name_clean}_{timestamp}.csv"
        csv_path = output_path / csv_filename
        
        # Write to CSV
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        # Get file size
        file_size = os.path.getsize(csv_path)
        
        # Generate metadata file
        metadata_filename = f"{state_code}_canonical_{sheet_name_clean}_{timestamp}_metadata.json"
        metadata_path = output_path / metadata_filename
        
        metadata = {
            "source_file": file_path,
            "sheet_name": sheet_name,
            "state_code": state_code,
            "header_row_index": header_row_index,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "column_mapping": column_mapping,
            "canonical_columns": list(df.columns),
            "original_columns": list(column_mapping.keys()),
            "generation_timestamp": datetime.now().isoformat(),
            "csv_file": str(csv_path),
            "file_size_bytes": file_size,
        }
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Generated canonical CSV: {csv_path}")
        logger.info(f"  Rows: {len(df):,}")
        logger.info(f"  Columns: {len(df.columns)}")
        logger.info(f"  Size: {file_size:,} bytes")
        logger.info(f"Generated metadata: {metadata_path}")
        
        return {
            "success": True,
            "csv_path": str(csv_path),
            "metadata_path": str(metadata_path),
            "row_count": len(df),
            "column_count": len(df.columns),
            "file_size_bytes": file_size,
            "canonical_columns": list(df.columns),
        }
    
    except Exception as e:
        logger.error(f"Failed to generate canonical CSV: {e}")
        return {
            "success": False,
            "error": f"CSV generation error: {e}",
        }

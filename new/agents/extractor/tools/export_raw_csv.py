"""
Export raw CSV data using column mappings from extraction results.
"""

import pandas as pd
import requests
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


def export_raw_csv(
    source_url: str,
    file_type: str,
    sheet_name: str,
    column_mapping: Dict[str, str],
    state_name: str,
    state_code: str,
    category: str,
    download_timestamp: str,
    output_path: str,
    cached_file_path: Optional[str] = None
) -> Dict:
    """
    Export raw data from original file using column mappings.
    
    Args:
        source_url: URL of the original file
        file_type: File extension (xlsx, csv, etc.)
        sheet_name: Sheet name for Excel files
        column_mapping: Dict mapping original columns to canonical names
        state_name: State name
        state_code: State code
        category: Category (physician, dental, etc.)
        download_timestamp: Timestamp of extraction
        output_path: Path to save the CSV file
        cached_file_path: Optional path to already downloaded file
        
    Returns:
        Dict with success status and row count
    """
    try:
        logger.info(f"Exporting raw CSV data to {output_path}")
        
        # Download or use cached file
        if cached_file_path and Path(cached_file_path).exists():
            original_file = cached_file_path
            logger.info(f"Using cached file: {original_file}")
        else:
            # Download file
            temp_file = Path(output_path).parent / f"temp_download.{file_type}"
            logger.info(f"Downloading from {source_url}...")
            response = requests.get(source_url, timeout=60)
            response.raise_for_status()
            
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            original_file = str(temp_file)
            logger.info(f"Downloaded to {original_file}")
        
        # Read the original file
        if file_type == 'xlsx':
            df = pd.read_excel(original_file, sheet_name=sheet_name)
        elif file_type == 'csv':
            df = pd.read_csv(original_file)
        else:
            return {
                "success": False,
                "error": f"Unsupported file type: {file_type}"
            }
        
        logger.info(f"Loaded {len(df)} rows from original file")
        
        # Create mapped dataframe
        mapped_df = pd.DataFrame()
        
        # Map columns to canonical names
        for original_col, canonical_col in column_mapping.items():
            if original_col in df.columns:
                mapped_df[canonical_col] = df[original_col]
            else:
                logger.warning(f"Column '{original_col}' not found in original data")
        
        # Add metadata columns at the beginning
        mapped_df.insert(0, 'state_name', state_name)
        mapped_df.insert(1, 'state_code', state_code)
        mapped_df.insert(2, 'source_url', source_url)
        mapped_df.insert(3, 'category', category)
        mapped_df.insert(4, 'extraction_date', download_timestamp)
        
        # Save to CSV
        mapped_df.to_csv(output_path, index=False)
        
        logger.info(f"✅ Exported {len(mapped_df):,} rows to {output_path}")
        logger.info(f"Columns: {list(mapped_df.columns)}")
        
        return {
            "success": True,
            "rows_exported": len(mapped_df),
            "columns": list(mapped_df.columns),
            "output_path": output_path
        }
        
    except Exception as e:
        logger.error(f"Error exporting raw CSV: {e}")
        return {
            "success": False,
            "error": str(e)
        }

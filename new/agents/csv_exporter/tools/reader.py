"""
Read and map data tool.
"""

import pandas as pd
from typing import Dict
import logging

logger = logging.getLogger(__name__)


def read_and_map_data(
    file_path: str,
    file_type: str,
    sheet_name: str,
    column_mapping: Dict[str, str],
    state_name: str,
    state_code: str,
    source_url: str,
    category: str,
    extraction_date: str
) -> dict:
    """
    Read original file and preserve ALL columns.
    
    **CRITICAL**: This function preserves ALL original columns.
    No columns are filtered or renamed. Column mapping is ignored
    to ensure complete data preservation for downstream analysis.
    
    Args:
        file_path: Path to the file
        file_type: File extension (xlsx, csv)
        sheet_name: Sheet name for Excel files
        column_mapping: Dict mapping (NOT USED - kept for compatibility)
        state_name: State name
        state_code: State code
        source_url: Source URL
        category: Category
        extraction_date: Extraction timestamp
        
    Returns:
        dict: {"dataframe": pd.DataFrame, "rows": int, "columns": list, 
               "original_columns": list, "error": str or None}
    """
    try:
        # Read original file
        if file_type == 'xlsx':
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            logger.info(f"Read Excel sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
        elif file_type == 'csv':
            df = pd.read_csv(file_path)
            logger.info(f"Read CSV file: {len(df)} rows, {len(df.columns)} columns")
        else:
            return {
                "dataframe": None,
                "rows": 0,
                "columns": [],
                "original_columns": [],
                "error": f"Unsupported file type: {file_type}"
            }
        
        # Store original column names for reference
        original_columns = list(df.columns)
        
        # Create final dataframe with ALL original columns
        final_df = pd.DataFrame()
        
        # Add ALL original columns from the source file
        # This preserves all data for future analysis
        for col in df.columns:
            final_df[col] = df[col]

        logger.info(f"Preserved all {len(df.columns)} original columns")
        logger.info(f"Total columns in output: {len(final_df.columns)}")
        logger.info(f"Original columns: {', '.join(original_columns[:10])}{'...' if len(original_columns) > 10 else ''}")
        
        return {
            "dataframe": final_df,
            "rows": len(final_df),
            "columns": list(final_df.columns),
            "original_columns": original_columns,
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Error reading/mapping data: {e}", exc_info=True)
        return {
            "dataframe": None,
            "rows": 0,
            "columns": [],
            "original_columns": [],
            "error": str(e)
        }

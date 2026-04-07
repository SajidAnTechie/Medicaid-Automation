"""
CSV export tool.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def export_to_csv(dataframe: pd.DataFrame, output_path: str) -> dict:
    """
    Export dataframe to CSV file.
    
    Args:
        dataframe: DataFrame to export
        output_path: Path to save CSV file
        
    Returns:
        dict: {"success": bool, "path": str, "rows": int, "error": str or None}
    """
    try:
        # Save to CSV
        dataframe.to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"✅ Exported {len(dataframe):,} rows to {output_path}")
        
        return {
            "success": True,
            "path": output_path,
            "rows": len(dataframe),
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        return {
            "success": False,
            "path": output_path,
            "rows": 0,
            "error": str(e)
        }

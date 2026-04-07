"""
Download or cache file tool.
"""

import requests
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def download_or_cache_file(source_url: str, file_type: str, output_dir: str, cached_file_path: str = None) -> dict:
    """
    Download file from URL or use cached version.
    
    Args:
        source_url: URL to download from
        file_type: File extension (xlsx, csv)
        output_dir: Directory to save downloaded file
        cached_file_path: Optional path to cached file
        
    Returns:
        dict: {"file_path": str, "cached": bool, "error": str or None}
    """
    try:
        # Check if cached file exists
        if cached_file_path and Path(cached_file_path).exists():
            logger.info(f"Using cached file: {cached_file_path}")
            return {
                "file_path": cached_file_path,
                "cached": True,
                "error": None
            }
        
        # Download file
        temp_file = Path(output_dir) / f"temp_download.{file_type}"
        logger.info(f"Downloading from {source_url}...")
        
        response = requests.get(source_url, timeout=60, stream=True)
        response.raise_for_status()
        
        with open(temp_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded to {temp_file}")
        return {
            "file_path": str(temp_file),
            "cached": False,
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return {
            "file_path": None,
            "cached": False,
            "error": str(e)
        }

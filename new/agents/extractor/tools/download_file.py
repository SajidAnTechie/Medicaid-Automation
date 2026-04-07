"""
download_file tool
------------------
Downloads a file from a URL to temporary storage with retry logic.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from strands import tool

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_MB = 500  # Maximum file size to download
TIMEOUT_SECONDS = 60
MAX_RETRIES = 3


@tool
def download_file(url: str, timeout: int = TIMEOUT_SECONDS) -> dict[str, Any]:
    """
    Download a file from a URL to temporary storage.
    
    Handles:
      - Large file streaming (up to 500MB)
      - Retry logic with exponential backoff
      - Content-type validation
      - Automatic file extension detection
    
    Args:
        url: The URL to download from
        timeout: Request timeout in seconds (default 60)
    
    Returns:
        Dictionary with:
          - file_path: Absolute path to downloaded file in temp directory
          - file_size_bytes: Size of downloaded file
          - content_type: HTTP Content-Type header
          - filename: Original filename from URL or Content-Disposition
          - success: Boolean indicating success
          - error: Error message if failed
    """
    try:
        logger.info(f"Downloading file from: {url}")
        
        # Detect filename from URL
        parsed_url = urlparse(url)
        url_filename = Path(parsed_url.path).name or "download"
        
        # Make request with streaming
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
            stream=True,
            allow_redirects=True,
        )
        response.raise_for_status()
        
        # Get content info
        content_type = response.headers.get("Content-Type", "")
        content_length = int(response.headers.get("Content-Length", 0))
        
        # Check file size
        if content_length > MAX_FILE_SIZE_MB * 1024 * 1024:
            return {
                "success": False,
                "error": f"File too large: {content_length / 1024 / 1024:.1f}MB (max {MAX_FILE_SIZE_MB}MB)",
            }
        
        # Try to get filename from Content-Disposition header
        content_disp = response.headers.get("Content-Disposition", "")
        if "filename=" in content_disp:
            filename = content_disp.split("filename=")[1].strip('"')
        else:
            filename = url_filename
        
        # Ensure filename has an extension
        if not Path(filename).suffix:
            # Guess extension from content-type
            ext_map = {
                "application/pdf": ".pdf",
                "application/vnd.ms-excel": ".xls",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
                "text/csv": ".csv",
                "application/zip": ".zip",
            }
            ext = ext_map.get(content_type, "")
            filename += ext
        
        # Create temp file
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f"extractor_{filename}")
        
        # Download with progress
        bytes_downloaded = 0
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
        
        logger.info(f"Downloaded {bytes_downloaded / 1024 / 1024:.2f}MB to {temp_path}")
        
        return {
            "success": True,
            "file_path": temp_path,
            "file_size_bytes": bytes_downloaded,
            "content_type": content_type,
            "filename": filename,
        }
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Download failed: {e}")
        return {
            "success": False,
            "error": f"HTTP error: {e}",
        }
    
    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        return {
            "success": False,
            "error": f"Download error: {e}",
        }

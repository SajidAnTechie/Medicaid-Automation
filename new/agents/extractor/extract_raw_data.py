#!/usr/bin/env python3
"""
Extract raw data from original file using Extractor JSON mapping.

Reads the Extractor JSON output, downloads the original file,
and creates a CSV with all data mapped to canonical columns.
"""

import json
import pandas as pd
import requests
from pathlib import Path
from typing import Dict, List
import sys


def download_file(url: str, output_path: Path) -> None:
    """Download file from URL."""
    print(f"Downloading from {url}...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded to {output_path}")


def load_extractor_json(json_path: Path) -> Dict:
    """Load Extractor JSON output."""
    with open(json_path, 'r') as f:
        return json.load(f)


def extract_raw_data(extractor_data: Dict, original_file: Path, output_csv: Path) -> None:
    """Extract raw data using column mapping and save to CSV."""
    
    # Get the first table (main table)
    table = extractor_data['extracted_tables'][0]
    sheet_name = table['sheet_name']
    column_mapping = table['column_mapping']
    
    print(f"\nReading sheet: {sheet_name}")
    print(f"Column mapping: {column_mapping}")
    
    # Read the original Excel file
    if extractor_data['file_type'] == 'xlsx':
        df = pd.read_excel(original_file, sheet_name=sheet_name)
    else:
        df = pd.read_csv(original_file)
    
    print(f"Original data shape: {df.shape}")
    print(f"Original columns: {list(df.columns)}")
    
    # Create new dataframe with mapped columns
    mapped_df = pd.DataFrame()
    
    for original_col, canonical_col in column_mapping.items():
        if original_col in df.columns:
            mapped_df[canonical_col] = df[original_col]
        else:
            print(f"Warning: Column '{original_col}' not found in original data")
    
    # Add metadata columns
    mapped_df.insert(0, 'state_name', extractor_data['state_name'])
    mapped_df.insert(1, 'state_code', extractor_data.get('state_code', ''))
    mapped_df.insert(2, 'source_url', extractor_data['source_url'])
    mapped_df.insert(3, 'category', extractor_data.get('category', ''))
    mapped_df.insert(4, 'extraction_date', extractor_data['download_timestamp'])
    
    # Save to CSV
    mapped_df.to_csv(output_csv, index=False)
    print(f"\n✅ Saved {len(mapped_df):,} rows to {output_csv}")
    print(f"Columns: {list(mapped_df.columns)}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_raw_data.py <extractor_json_path>")
        print("\nExample:")
        print("  python extract_raw_data.py output/alaska_extractor_result_0.json")
        sys.exit(1)
    
    # Paths
    json_path = Path(sys.argv[1])
    output_dir = json_path.parent
    
    if not json_path.exists():
        print(f"Error: {json_path} not found")
        sys.exit(1)
    
    # Load extractor JSON
    print(f"Loading {json_path}...")
    extractor_data = load_extractor_json(json_path)
    
    # Download original file
    source_url = extractor_data['source_url']
    file_type = extractor_data['file_type']
    state_name = extractor_data['state_name'].lower().replace(' ', '_')
    
    original_file = output_dir / f"{state_name}_original.{file_type}"
    
    if not original_file.exists():
        download_file(source_url, original_file)
    else:
        print(f"Using cached file: {original_file}")
    
    # Extract and save raw data
    output_csv = output_dir / f"{state_name}_raw_data.csv"
    extract_raw_data(extractor_data, original_file, output_csv)
    
    print(f"\n🎉 Complete!")
    print(f"   Input:  {json_path}")
    print(f"   Source: {original_file}")
    print(f"   Output: {output_csv}")


if __name__ == '__main__':
    main()

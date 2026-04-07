#!/usr/bin/env python3
"""
Extractor Agent CLI Runner
--------------------------
Runs the Extractor Agent to download and parse Medicaid fee schedule files.

Can be used standalone or chained after Navigator output.

Usage:
    # From Navigator output JSON
    python -m agents.extractor.run --navigator-output output/alaska_navigator_result.json

    # From a single URL
    python -m agents.extractor.run --url "https://example.com/schedule.xlsx" --state alaska

    # Process top N datasets from Navigator
    python -m agents.extractor.run --navigator-output output/florida_navigator_result.json --top 3
"""

from __future__ import annotations
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from .models import DatasetCategory, ExtractorInput, FileType
from .agent import run_extractor, save_extracted_data_to_csv
from pathlib import Path
import sys
import logging
import json
import argparse
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file if present

app = BedrockAgentCoreApp()
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ── Output directory ──────────────────────────────────────────────────────────

# OUTPUT_DIR = Path(__file__).parent.parent.parent / "output"
# try:
#     OUTPUT_DIR.mkdir(exist_ok=True)
# except PermissionError:
#     # Inside AgentCore container the parent dir may be root-owned;
#     # fall back to a writable temp location.
#     OUTPUT_DIR = Path("/tmp/extractor_output")
#     OUTPUT_DIR.mkdir(exist_ok=True)
#     logger.warning("Using fallback output dir: %s", OUTPUT_DIR)

# Ensure the project root (new/) is on sys.path so absolute imports work
# regardless of how this script is invoked.


# ── Helper Functions ──────────────────────────────────────────────────────────


def load_navigator_output(file_path: str) -> dict[str, any]:
    """Load Navigator output JSON file."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        logger.info(f"Loaded Navigator output from: {file_path}")
        return data
    except Exception as e:
        logger.error(f"Failed to load Navigator output: {e}")
        sys.exit(1)


def file_type_from_string(file_type_str: str) -> FileType:
    """Convert string to FileType enum."""
    try:
        return FileType(file_type_str.lower())
    except ValueError:
        return FileType.UNKNOWN


def category_from_string(category_str: str) -> DatasetCategory:
    """Convert string to DatasetCategory enum."""
    try:
        return DatasetCategory(category_str.lower())
    except ValueError:
        return DatasetCategory.UNKNOWN


# def save_output(output_data: dict[str, any], state_key: str, dataset_index: int = 0) -> Path:
#     """Save extractor output to JSON file."""
#     filename = f"{state_key}_extractor_result_{dataset_index}.json"
#     output_path = OUTPUT_DIR / filename

#     with open(output_path, "w") as f:
#         json.dump(output_data, f, indent=2)

#     return output_path


def print_results(result: dict[str, any], dataset_num: int = 1):
    """Pretty print extractor results."""
    print()
    print("=" * 80)
    print(
        f"{'✅' if result['success'] else '❌'} RESULTS: Dataset #{dataset_num}")
    print(f"URL: {result['source_url']}")
    print("=" * 80)
    print()
    print(
        f"State        : {result.get('state_name', 'Unknown')} ({result.get('state_code', '')})")
    print(f"File Type    : {result['file_type']}")
    print(f"Category     : {result['category']}")
    print(f"File Size    : {result['file_size_bytes'] / 1024 / 1024:.2f} MB")
    print(f"Tables       : {len(result['extracted_tables'])}")
    print(f"Total Rows   : {result['total_rows_extracted']}")
    print(f"Processing   : {result['processing_time_seconds']:.2f}s")
    print()

    # Table details
    if result['extracted_tables']:
        print("📊 Extracted Tables:")
        print("-" * 80)
        for i, table in enumerate(result['extracted_tables'], 1):
            confidence_emoji = "🟢" if table['mapping_confidence'] >= 0.8 else "🟡" if table['mapping_confidence'] >= 0.6 else "🔴"
            print(f"\n{i}. {table['sheet_name']}")
            print(
                f"   Rows: {table['row_count']:,} | Columns: {len(table['headers'])}")
            print(
                f"   Mapping Confidence: {confidence_emoji} {table['mapping_confidence']:.0%}")
            print(f"   Headers: {', '.join(table['headers'][:5])}" + (
                "..." if len(table['headers']) > 5 else ""))
            if table.get('column_mapping'):
                print(
                    f"   Mapped Fields: {', '.join(table['column_mapping'].values())}")

    # Data quality issues
    if result.get('data_quality_issues'):
        print()
        print(
            f"⚠️  Data Quality Issues ({len(result['data_quality_issues'])}):")
        for issue in result['data_quality_issues'][:5]:
            print(f"   - {issue}")
        if len(result['data_quality_issues']) > 5:
            print(f"   ... and {len(result['data_quality_issues']) - 5} more")

    # Schema drift
    if result.get('schema_drift_detected'):
        print()
        print("🔄 Schema Drift Detected:")
        for detail in result.get('schema_drift_details', []):
            print(f"   - {detail}")

    # Errors
    if result.get('errors'):
        print()
        print(f"❌ Errors ({len(result['errors'])}):")
        for error in result['errors']:
            print(f"   - {error}")

    print()


# ── Main Runner ───────────────────────────────────────────────────────────────

@app.entrypoint
def run_from_navigator_output(payload: dict[str, any]):
    """Run Extractor on datasets from Navigator output."""
    # AgentCore may deliver payload as a dict (already parsed) or a JSON string.
    if isinstance(payload, str):
        payload = json.loads(payload)

    nav_data = payload.get('prompt', payload)

    if isinstance(nav_data, str):
        try:
            nav_data = json.loads(nav_data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Navigator output JSON: {e}")
            sys.exit(1)

    if not nav_data.get('success', True):
        logger.error("Navigator output indicates failure")
        sys.exit(1)

    datasets = nav_data.get('relevant_datasets', [])
    if not datasets:
        logger.error("No relevant datasets found in Navigator output")
        sys.exit(1)

    state_name = nav_data.get('state_name', 'Unknown')
    state_code = nav_data.get('state_code', '')
    state_key = state_code.lower() if state_code else state_name.lower().replace(' ', '_')

    # Process top N datasets
    datasets_to_process = datasets[:1]
    logger.info(
        f"Processing top {len(datasets_to_process)} dataset(s) from Navigator output")

    results = []

    for i, dataset in enumerate(datasets_to_process, 1):
        print()
        print("=" * 80)
        print(f"🔄 Processing Dataset {i}/{len(datasets_to_process)}")
        print("=" * 80)
        print(f"Title: {dataset.get('title', 'Untitled')}")
        print(f"URL: {dataset['url']}")
        print(f"Relevance Score: {dataset.get('relevance_score', 0):.2f}")
        print(f"Category: {dataset.get('category', 'unknown')}")
        print()

        # Create ExtractorInput
        extractor_input = ExtractorInput(
            url=dataset['url'],
            state_name=state_name,
            state_code=state_code,
            file_type='xlsx',
            category=category_from_string(dataset.get('category', 'unknown')),
            title=dataset.get('title', ''),
            extract_all_sheets=True,
            ocr_enabled=False,
        )

        # Run extractor
        result = run_extractor(extractor_input)
        return result.model_dump()

    # Summary
    print()
    print("=" * 80)
    print("📊 EXTRACTION SUMMARY")
    print("=" * 80)
    total_rows = sum(r.total_rows_extracted for r in results)
    total_tables = sum(len(r.extracted_tables) for r in results)
    successful = sum(1 for r in results if r.success)

    print(f"Datasets Processed: {len(results)}")
    print(f"Successful: {successful}/{len(results)}")
    print(f"Total Tables: {total_tables}")
    print(f"Total Rows Extracted: {total_rows:,}")
    print()


# def run_from_url(
#     url: str,
#     state_name: str = "",
#     state_code: str = "",
#     file_type: str = "unknown",
#     category: str = "unknown",
#     title: str = "",
# ):
#     """Run Extractor on a single URL."""
#     print()
#     print("=" * 80)
#     print("🔄 Extracting Single Dataset")
#     print("=" * 80)
#     print(f"URL: {url}")
#     print()

#     # Create ExtractorInput
#     extractor_input = ExtractorInput(
#         url=url,
#         state_name=state_name,
#         state_code=state_code,
#         file_type=file_type_from_string(file_type),
#         category=category_from_string(category),
#         title=title,
#         extract_all_sheets=True,
#         ocr_enabled=False,
#     )

#     # Run extractor
#     result = run_extractor(extractor_input)

#     # Save output
#     state_key = state_code.lower() if state_code else state_name.lower().replace(
#         ' ', '_') or 'unknown'
#     output_path = save_output(result.model_dump(), state_key, 0)

#     # Save CSV files
#     from .tools.export_raw_csv import export_raw_csv

#     if result.success and result.extracted_tables:
#         table = result.extracted_tables[0]
#         csv_output = OUTPUT_DIR / f"{state_key}_raw_data.csv"

#         print(f"📊 Exporting raw CSV data...")
#         export_result = export_raw_csv(
#             source_url=result.source_url,
#             file_type=result.file_type,
#             sheet_name=table.sheet_name,
#             column_mapping=table.column_mapping,
#             state_name=result.state_name,
#             state_code=result.state_code,
#             category=result.category,
#             download_timestamp=result.download_timestamp,
#             output_path=str(csv_output)
#         )

#         if export_result["success"]:
#             print(
#                 f"✅ Exported {export_result['rows_exported']:,} rows to {csv_output}")
#         else:
#             print(f"❌ CSV export failed: {export_result.get('error')}")
#     else:
#         print("⚠️ No tables found or extraction was not successful, skipping CSV export")

#     # Print results
#     print_results(result.model_dump(), 1)
#     print(f"💾 Full JSON saved to: {output_path}")


# ── CLI Entry Point ───────────────────────────────────────────────────────────


# def main():
    # """CLI entry point."""
    # parser = argparse.ArgumentParser(
    #     description="Extractor Agent - Download and parse Medicaid fee schedule files",
    #     formatter_class=argparse.RawDescriptionHelpFormatter,
    # )

    # # Input source options
    # input_group = parser.add_mutually_exclusive_group(required=True)
    # input_group.add_argument(
    #     "--navigator-output",
    #     type=str,
    #     help="Path to Navigator output JSON file",
    # )
    # input_group.add_argument(
    #     "--url",
    #     type=str,
    #     help="Direct URL to a fee schedule file",
    # )

    # # Navigator-specific options
    # parser.add_argument(
    #     "--top",
    #     type=int,
    #     default=1,
    #     help="Number of top datasets to process from Navigator output (default: 1)",
    # )

    # # URL-specific options
    # parser.add_argument(
    #     "--state",
    #     type=str,
    #     default="",
    #     help="State name (for --url mode)",
    # )
    # parser.add_argument(
    #     "--state-code",
    #     type=str,
    #     default="",
    #     help="Two-letter state code (for --url mode)",
    # )
    # parser.add_argument(
    #     "--file-type",
    #     type=str,
    #     default="unknown",
    #     choices=["pdf", "xls", "xlsx", "csv", "zip", "unknown"],
    #     help="File type (for --url mode)",
    # )
    # parser.add_argument(
    #     "--category",
    #     type=str,
    #     default="unknown",
    #     choices=[
    #         "physician", "dental", "pharmacy", "dmepos",
    #         "outpatient", "inpatient", "behavioral_health",
    #         "laboratory", "vision", "home_health", "general", "unknown"
    #     ],
    #     help="Dataset category (for --url mode)",
    # )
    # parser.add_argument(
    #     "--title",
    #     type=str,
    #     default="",
    #     help="Dataset title (for --url mode)",
    # )

    # # Logging
    # parser.add_argument(
    #     "--debug",
    #     action="store_true",
    #     help="Enable debug logging",
    # )

    # args = parser.parse_args()

    # # Set log level
    # if args.debug:
    #     logging.getLogger().setLevel(logging.DEBUG)
    #     logging.getLogger("strands").setLevel(logging.DEBUG)
    #     logging.getLogger("agents.extractor").setLevel(logging.DEBUG)
    # else:
    #     # Set to INFO to see important logs
    #     logging.getLogger().setLevel(logging.INFO)
    #     logging.getLogger("agents.extractor").setLevel(logging.INFO)

    # # Run appropriate mode
    # if args.navigator_output:
    #     run_from_navigator_output(args.navigator_output, args.top)
    # elif args.url:
    #     run_from_url(
    #         url=args.url,
    #         state_name=args.state,
    #         state_code=args.state_code,
    #         file_type=args.file_type,
    #         category=args.category,
    #         title=args.title,
    #     )


if __name__ == "__main__":
    app.run()

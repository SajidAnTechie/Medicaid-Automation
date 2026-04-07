#!/usr/bin/env python3
"""
Complete Pipeline Example: Navigator → Extractor
-------------------------------------------------
Demonstrates chaining the Navigator and Extractor agents.

Usage:
    python pipeline_example.py alaska
    python pipeline_example.py florida --top 3
"""

from __future__ import annotations
from new.agents.analysis import agentcore_analysis as analysis_agent
from agents.csv_exporter import run_csv_exporter, CSVExporterInput
from agents.extractor.models import DatasetCategory, FileType
from agents.extractor import ExtractorInput, run_extractor
from agents.navigator import NavigatorInput, run_navigator
from pathlib import Path
import sys
import logging
import json
import argparse
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file if present


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ── State Portal Configuration ────────────────────────────────────────────────

STATE_PORTALS = {
    "alaska": {
        "state_name": "Alaska",
        "state_code": "AK",
        "portal_url": "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html",
    },
    "florida": {
        "state_name": "Florida",
        "state_code": "FL",
        "portal_url": "https://ahca.myflorida.com/medicaid/cost-reimbursement-and-auditing",
    },
    "california": {
        "state_name": "California",
        "state_code": "CA",
        "portal_url": "https://www.dhcs.ca.gov/provgovpart/Pages/Rate-Information.aspx",
    },
}

# ── Output Directory ──────────────────────────────────────────────────────────

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Pipeline Functions ────────────────────────────────────────────────────────


def run_pipeline(state_key: str, top_n: int = 1, max_crawl_depth: int = 1):
    """
    Run the complete Navigator → Extractor pipeline for a state.

    Args:
        state_key: State identifier (e.g., 'alaska', 'florida')
        top_n: Number of top datasets to extract
        max_crawl_depth: Maximum crawl depth for Navigator
    """
    if state_key not in STATE_PORTALS:
        logger.error(
            f"Unknown state: {state_key}. Available: {', '.join(STATE_PORTALS.keys())}")
        sys.exit(1)

    state_config = STATE_PORTALS[state_key]

    print()
    print("=" * 80)
    print(f"🚀 MEDICAID FEE SCHEDULE PIPELINE: {state_config['state_name']}")
    print("=" * 80)
    print()

    # ── STEP 1: Navigator Agent ──────────────────────────────────────────────

    print("📍 STEP 1: Navigator Agent - Discovering Datasets")
    print("-" * 80)
    print(f"Portal: {state_config['portal_url']}")
    print(f"Max Depth: {max_crawl_depth}")
    print()

    nav_input = NavigatorInput(
        portal_url=state_config["portal_url"],
        state_name=state_config["state_name"],
        state_code=state_config["state_code"],
        dataset_category="all",
        max_depth=max_crawl_depth,
        top_k=15,  # Navigator discovers many URLs, we'll pick top N for extraction
    )

    nav_result = run_navigator(nav_input)

    # Save Navigator output
    nav_output_path = OUTPUT_DIR / f"{state_key}_navigator_result.json"
    with open(nav_output_path, "w") as f:
        json.dump(nav_result.model_dump(mode="json"), f, indent=2)

    print()
    print(f"✅ Navigator Complete!")
    print(f"   Discovered: {nav_result.total_links_discovered} links")
    print(f"   Relevant: {len(nav_result.relevant_datasets)} datasets")
    print(f"   Saved to: {nav_output_path}")
    print()

    if not nav_result.relevant_datasets:
        logger.error("No relevant datasets found. Exiting.")
        sys.exit(1)

    # ── STEP 2: Extractor Agent ──────────────────────────────────────────────

    print("=" * 80)
    print(
        f"🔧 STEP 2: Extractor Agent - Processing Top {min(top_n, len(nav_result.relevant_datasets))} Dataset(s)")
    print("-" * 80)
    print()

    # By default, only process the #1 ranked dataset (highest relevance score)
    # Use --top N to process multiple datasets
    datasets_to_process = nav_result.relevant_datasets[:top_n]
    extraction_results = []

    for i, dataset in enumerate(datasets_to_process, 1):
        print(f"📦 Dataset {i}/{len(datasets_to_process)}")
        print(f"   Title: {dataset.title}")
        print(f"   Score: {dataset.relevance_score:.2f}")
        print(f"   Type: {dataset.file_type.value}")
        print(f"   URL: {dataset.url}")
        print()

        # Create ExtractorInput
        ext_input = ExtractorInput(
            url=dataset.url,
            state_name=nav_result.state_name,
            state_code=nav_result.state_code,
            file_type=FileType(dataset.file_type.value),
            category=DatasetCategory(dataset.category.value),
            title=dataset.title,
            extract_all_sheets=True,
            ocr_enabled=False,
        )

        # Run Extractor
        ext_result = run_extractor(ext_input)

        # Save Extractor output
        ext_output_path = OUTPUT_DIR / \
            f"{state_key}_extractor_result_{i - 1}.json"
        with open(ext_output_path, "w") as f:
            json.dump(ext_result.model_dump(mode="json"), f, indent=2)

        # Print summary
        status = "✅" if ext_result.success else "❌"
        print(
            f"   {status} Extraction: {'Success' if ext_result.success else 'Failed'}")
        print(f"   Tables: {len(ext_result.extracted_tables)}")
        print(f"   Total Rows: {ext_result.total_rows_extracted:,}")
        print(f"   Processing Time: {ext_result.processing_time_seconds:.2f}s")
        print(f"   Saved to: {ext_output_path}")

        if ext_result.data_quality_issues:
            print(
                f"   ⚠️  Quality Issues: {len(ext_result.data_quality_issues)}")

        # Export raw CSV using CSV Exporter Agent
        if ext_result.success and ext_result.extracted_tables:
            print(f"\n📊 Exporting raw CSV with CSV Exporter Agent...")

            table = ext_result.extracted_tables[0]
            csv_output = OUTPUT_DIR / \
                f"{state_key.lower().replace(' ', '_')}_raw_data_{i}.csv"

            csv_input = CSVExporterInput(
                source_url=ext_result.source_url,
                file_type=ext_result.file_type,
                sheet_name=table.sheet_name,
                column_mapping=table.column_mapping,
                state_name=ext_result.state_name,
                state_code=ext_result.state_code,
                category=ext_result.category,
                download_timestamp=ext_result.download_timestamp,
                output_path=str(csv_output)
            )

            csv_result = run_csv_exporter(csv_input)

            if csv_result.success:
                print(f"✅ Exported {csv_result.rows_exported:,} rows")

                # Get the output path (could be S3 URI or local file path)
                csv_output_path = csv_result.output_path
                print(f"   Output: {csv_output_path}")

                # Run Analysis Agent
                print(
                    f"\n🔍 Running Analysis Agent to create canonical header mapping...")
                print(f"   Reading from: {csv_output_path}")
                try:
                    analysis_result = analysis_agent.run(
                        # Use the output_path from csv_exporter (S3 URI or file://)
                        csv_url=csv_output_path,
                        metadata_json=ext_result.model_dump(mode="json"),
                    )

                    # Save analysis result
                    analysis_output_path = OUTPUT_DIR / \
                        f"{state_key}_analysis_result_{i}.json"
                    with open(analysis_output_path, "w") as f:
                        json.dump(analysis_result, f, indent=2)

                    print(f"✅ Analysis Complete!")
                    print(
                        f"   Canonical headers mapped: {len(analysis_result['canonical_headers'])}")
                    print(
                        f"   Rows transformed: {len(analysis_result.get('transformed_data', []))}")
                    print(
                        f"   Unmapped headers: {len(analysis_result['unmapped_headers'])}")

                    # Check if cleaned CSV was created
                    if analysis_result.get('csv_output_path'):
                        print(
                            f"   Cleaned CSV: {analysis_result['csv_output_path']}")
                    if analysis_result.get('s3_upload'):
                        s3_cleaned = analysis_result['s3_upload']
                        print(f"   S3 Cleaned URI: {s3_cleaned['s3_uri']}")

                    print(f"   Analysis saved to: {analysis_output_path}")

                    if analysis_result['canonical_headers']:
                        print(f"\n   Sample mappings:")
                        for mapping in analysis_result['canonical_headers'][:3]:
                            print(f"      {mapping['raw_header']} -> {mapping['canonical_name']} "
                                  f"(confidence: {mapping['confidence']})")
                        if len(analysis_result['canonical_headers']) > 3:
                            print(
                                f"      ... and {len(analysis_result['canonical_headers']) - 3} more")

                except Exception as e:
                    print(f"❌ Analysis failed: {e}")
                    logger.exception("Analysis agent error")
            else:
                print(f"❌ CSV export failed: {csv_result.error}")

        extraction_results.append(ext_result)

    # ── STEP 3: Summary ───────────────────────────────────────────────────────

    print("=" * 80)
    print("📊 PIPELINE SUMMARY")
    print("=" * 80)
    print()

    # Navigator stats
    print(f"🔍 Navigator:")
    print(f"   Portal Crawled: {state_config['state_name']}")
    print(f"   Pages Visited: {len(nav_result.crawled_pages)}")
    print(f"   Links Found: {nav_result.total_links_discovered}")
    print(f"   Relevant Datasets: {len(nav_result.relevant_datasets)}")
    print()

    # Extractor stats
    successful_extractions = sum(1 for r in extraction_results if r.success)
    total_tables = sum(len(r.extracted_tables) for r in extraction_results)
    total_rows = sum(r.total_rows_extracted for r in extraction_results)
    avg_processing_time = sum(r.processing_time_seconds for r in extraction_results) / \
        len(extraction_results) if extraction_results else 0

    print(f"🔧 Extractor:")
    print(f"   Datasets Processed: {len(extraction_results)}")
    print(f"   Successful: {successful_extractions}/{len(extraction_results)}")
    print(f"   Total Tables: {total_tables}")
    print(f"   Total Rows: {total_rows:,}")
    print(f"   Avg Processing Time: {avg_processing_time:.2f}s")
    print()

    # Data quality
    all_quality_issues = []
    for r in extraction_results:
        all_quality_issues.extend(r.data_quality_issues)

    if all_quality_issues:
        print(f"⚠️  Data Quality:")
        print(f"   Total Issues: {len(all_quality_issues)}")
        print(f"   Sample Issues:")
        for issue in all_quality_issues[:3]:
            print(f"      - {issue}")
        if len(all_quality_issues) > 3:
            print(f"      ... and {len(all_quality_issues) - 3} more")
        print()

    # Next steps
    print("🎯 Next Steps:")
    print(f"   1. Review extracted data in: {OUTPUT_DIR}/")
    print(f"   2. Address any data quality issues")
    print(f"   3. Run Archivist agent to load data to database")
    print()

    print("=" * 80)
    print(f"✅ PIPELINE COMPLETE: {state_config['state_name']}")
    print("=" * 80)
    print()


# ── CLI Entry Point ───────────────────────────────────────────────────────────


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Complete Medicaid Fee Schedule Ingestion Pipeline (Navigator → Extractor)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Available States:
{chr(10).join(f"  - {key}: {val['state_name']}" for key, val in STATE_PORTALS.items())}

Examples:
  python pipeline_example.py alaska
  python pipeline_example.py florida --top 3
  python pipeline_example.py california --max-depth 3
        """,
    )

    parser.add_argument(
        "state",
        type=str,
        choices=list(STATE_PORTALS.keys()),
        help="State to process",
    )

    parser.add_argument(
        "--top",
        type=int,
        default=1,
        help="Number of top datasets to extract (default: 1)",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="Maximum crawl depth for Navigator (default: 2)",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Set log level
    if args.debug:
        logging.getLogger("strands").setLevel(logging.DEBUG)
        logging.getLogger("agents").setLevel(logging.DEBUG)

    # Run pipeline
    run_pipeline(
        state_key=args.state,
        top_n=args.top,
        max_crawl_depth=args.max_depth,
    )


if __name__ == "__main__":
    main()

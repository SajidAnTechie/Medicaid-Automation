"""
Sentinel-State MVP Pipeline
----------------------------
Simple linear runner:
  Hardcoded URLs → Extractor → Analyst → JSON Schema Validation → output files

Usage:
    python pipeline.py                     # process all URLs in config/urls.py
    python pipeline.py --state FL          # process only Florida entries
    python pipeline.py --dry-run           # print config, do not download
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from config.urls import URL_REGISTRY
import agents.extractor as extractor_agent
import agents.analyst as analyst_agent
import validator as schema_validator

OUTPUT_DIR = Path("output")


def run(state_filter: str | None = None, dry_run: bool = False) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Filter entries if --state flag provided
    entries = URL_REGISTRY
    if state_filter:
        entries = [e for e in entries if e["state_code"].upper() == state_filter.upper()]
        if not entries:
            print(f"No entries found for state '{state_filter}'. Check config/urls.py.")
            sys.exit(1)

    if dry_run:
        print("=== DRY RUN — URLs that would be processed ===")
        for e in entries:
            print(f"  {e['state_code']:4s} | {e['dataset_type']:12s} | {e['file_type']:6s} | {e['url']}")
        return

    all_valid: list[dict] = []
    all_rejected: list[dict] = []

    for entry in entries:
        label = f"{entry['state_code']} / {entry['dataset_type']}"
        print(f"\n{'─' * 60}")
        print(f"  Processing: {label}")
        print(f"  URL:        {entry['url']}")
        print(f"  Format:     {entry['file_type']}")

        # ── Step 1: Extractor ─────────────────────────────────────
        try:
            extracted = extractor_agent.run(
                state_code=entry["state_code"],
                dataset_type=entry["dataset_type"],
                url=entry["url"],
                file_type=entry["file_type"],
            )
        except Exception as exc:
            print(f"  [ERROR] Extractor failed: {exc}")
            all_rejected.append({
                "state_code": entry["state_code"],
                "dataset_type": entry["dataset_type"],
                "stage": "extractor",
                "error": str(exc),
            })
            continue

        n_raw = len(extracted["raw_records"])
        print(f"  Extracted:  {n_raw} raw records")
        print(f"  Columns:    {extracted['raw_columns']}")

        if n_raw == 0:
            print("  [WARN] No records extracted — skipping analyst.")
            continue

        # ── Step 2: Analyst ───────────────────────────────────────
        try:
            analysed = analyst_agent.run(
                state_code=entry["state_code"],
                dataset_type=entry["dataset_type"],
                raw_records=extracted["raw_records"],
            )
        except Exception as exc:
            print(f"  [ERROR] Analyst failed: {exc}")
            all_rejected.append({
                "state_code": entry["state_code"],
                "dataset_type": entry["dataset_type"],
                "stage": "analyst",
                "error": str(exc),
            })
            continue

        print(f"  Mapping:    {analysed['mapping_log']}")

        # ── Step 3: Validate ──────────────────────────────────────
        result = schema_validator.run(analysed["mapped_records"])

        n_valid    = len(result["valid"])
        n_rejected = len(result["rejected"])
        print(f"  Valid:      {n_valid}")
        print(f"  Rejected:   {n_rejected}")

        all_valid.extend(result["valid"])
        all_rejected.extend(result["rejected"])

    # ── Write outputs ─────────────────────────────────────────────
    output_path     = OUTPUT_DIR / "output.json"
    rejections_path = OUTPUT_DIR / "rejections.json"

    output_path.write_text(json.dumps(all_valid, indent=2, default=str))
    rejections_path.write_text(json.dumps(all_rejected, indent=2, default=str))

    print(f"\n{'=' * 60}")
    print(f"  DONE")
    print(f"  Valid records  : {len(all_valid):>6}  →  {output_path}")
    print(f"  Rejected records: {len(all_rejected):>5}  →  {rejections_path}")
    print(f"{'=' * 60}\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sentinel-State MVP Pipeline")
    parser.add_argument(
        "--state", type=str, default=None,
        help="Process only entries for this state code (e.g. FL)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the URL list without downloading anything"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(state_filter=args.state, dry_run=args.dry_run)

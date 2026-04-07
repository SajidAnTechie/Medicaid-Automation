"""
CSV Exporter Agent - Exports raw data with column mappings.
"""

import logging
from strands import Agent
import json
import sys
from typing import Any
from .models import CSVExporterInput, CSVExporterOutput
from .tools import download_or_cache_file, read_and_map_data, export_to_csv
from bedrock_agentcore.runtime import BedrockAgentCoreApp

app = BedrockAgentCoreApp()

logger = logging.getLogger(__name__)

# Optional S3 support
try:
    import boto3
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    logger.warning("boto3 not available - S3 upload disabled")


# System prompt for CSV Exporter Agent
CSV_EXPORTER_SYSTEM_PROMPT = """You are an expert at exporting raw Medicaid fee schedule data to CSV format.

Your mission is to:
1. Download or use cached source files (Excel, CSV)
2. Read the original data from the correct sheet/file
3. Map columns from original names to canonical schema names
4. Export the complete dataset as a clean CSV file

**WORKFLOW:**

Step 1: Download or Cache
- Check if cached_file_path exists and use it
- If not, download from source_url
- Validate file type (xlsx, csv)

Step 2: Read Original Data
- For xlsx: read from specified sheet_name
- For csv: read the file directly
- Log the number of rows and columns loaded

Step 3: Apply Column Mapping (Preserve All Data)
- Keep ALL original columns from the source file
- Do NOT remove any columns, even if they're not in column_mapping
- Preserve exact column names as they appear in source
- This ensures no data is lost and everything can be analyzed later

Step 4: Export CSV
- Save to output_path
- Use UTF-8 encoding
- No index column
- Log success with row count

**IMPORTANT:**
- Preserve ALL rows from the original file
- Preserve ALL columns from the original file (do not filter)
- Keep original column names exactly as they appear
- Maintain data integrity (no modifications to cell values)
- Add metadata columns at the beginning only
- Handle missing columns gracefully
- Provide clear error messages
- Log progress at each step

**DATA PRESERVATION POLICY:**
The exported CSV must contain:
1. 5 metadata columns (state_name, state_code, source_url, category, extraction_date)
2. ALL original columns from the source file (no filtering, no renaming)
3. ALL original rows (no sampling, no filtering)

This ensures complete data preservation for downstream analysis and transformation.

**CANONICAL SCHEMA:**
The column_mapping translates original columns to these standard names:
- procedure_code (required)
- procedure_modifier
- procedure_description
- requires_medical_justification
- requires_comagine_authorization
- requires_fiscal_agent_authorization
- requires_ndc
- maximum_allowable
- billing_notes
- effective_date
- end_date

**ERROR HANDLING:**
- If file download fails: retry once, then report error
- If sheet not found: list available sheets
- If column not found: warn but continue with other columns
- If export fails: report full error with context

Always be thorough and precise in your data export.
"""


def get_csv_exporter_agent():
    """
    Create and return the CSV Exporter agent instance.

    Returns:
        Agent: Configured CSV Exporter agent
    """
    from strands.models import BedrockModel
    import os

    bedrock_model = BedrockModel(
        model_id=os.getenv("BEDROCK_MODEL"),
        region=os.getenv("AWS_REGION", "eu-north-1"),
    )

    return Agent(
        model=bedrock_model,
        system_prompt=CSV_EXPORTER_SYSTEM_PROMPT,
        tools=[download_or_cache_file, read_and_map_data, export_to_csv],
    )


def upload_to_s3(
    file_path: str,
    bucket_name: str = "medicaid-fee-raw",
    s3_key: str | None = None,
    region: str = "eu-north-1",
) -> dict[str, str]:
    """
    Upload a file to Amazon S3.

    Args:
        file_path: Local path to the file to upload
        bucket_name: S3 bucket name
        s3_key: S3 object key (if None, uses filename)
        region: AWS region for S3 bucket

    Returns:
        dict with s3_uri and s3_url

    Raises:
        RuntimeError: If boto3 is not available or upload fails
    """
    if not S3_AVAILABLE:
        raise RuntimeError(
            "boto3 is required for S3 upload. Install with: pip install boto3")

    from pathlib import Path

    # Auto-generate S3 key if not provided
    if not s3_key:
        s3_key = Path(file_path).name

    try:
        # Create S3 client
        s3_client = boto3.client('s3', region_name=region)

        # Upload file
        res = s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"res..................: {json.dumps(res)}")

        # Generate URIs
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        s3_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"

        logger.info(f"✅ Uploaded to S3: {s3_uri}")

        return {
            "s3_uri": s3_uri,
            "s3_url": s3_url,
            "bucket": bucket_name,
            "key": s3_key,
            "region": region,
            "res": json.dumps(res)
        }
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise RuntimeError(f"S3 upload failed: {e}")


@app.entrypoint
def mainFunction(payload: dict[str, Any]):
    """
    Main function to run the CSV Exporter agent.

    Accepts either:
      1. A ready-made CSVExporterInput dict (flat fields).
      2. An ExtractorOutput dict (has ``extracted_tables`` list).
         In this case we pick the first table and build CSVExporterInput
         from the Extractor's output fields.

    Returns:
        CSVExporterOutput with results of the export operation
    """
    # ── Unwrap multi-layer JSON encoding ──────────────────────────────
    data = payload
    for _ in range(5):
        if isinstance(data, dict):
            break
        data = json.loads(data)

    extra_data = data.get('prompt', data)
    for _ in range(5):
        if isinstance(extra_data, dict):
            break
        extra_data = json.loads(extra_data)

    logger.info("Parsed payload keys: %s", list(extra_data.keys()))

    # ── Build CSVExporterInput ────────────────────────────────────────
    # If the payload comes from the Extractor agent it will have
    # ``extracted_tables`` instead of the flat fields CSVExporterInput
    # expects.  Translate on the fly.
    if 'extracted_tables' in extra_data:
        tables = extra_data['extracted_tables']
        if not tables:
            return CSVExporterOutput(
                success=False,
                error="Extractor output contains no extracted tables",
            ).model_dump()

        table = tables[0]  # process first table
        state_name = extra_data.get('state_name', 'Unknown')
        state_code = extra_data.get('state_code', '')
        state_key = (state_code.lower() or
                     state_name.lower().replace(' ', '_'))

        input_data = CSVExporterInput(
            source_url=extra_data.get('source_url', ''),
            file_type=extra_data.get('file_type', 'xlsx'),
            sheet_name=table.get('sheet_name', 'Sheet1'),
            column_mapping=table.get('column_mapping', {}),
            state_name=state_name,
            state_code=state_code,
            category=extra_data.get('category', 'unknown'),
            download_timestamp=extra_data.get('download_timestamp', ''),
            output_path=f"/tmp/{state_key}_raw_data.csv",
        )
    else:
        # Already flat CSVExporterInput-shaped dict
        input_data = CSVExporterInput(**extra_data)

    result = run_csv_exporter(input_data=input_data)
    return result.model_dump() if hasattr(result, 'model_dump') else result


def run_csv_exporter(
    input_data: CSVExporterInput,
    upload_to_s3_bucket: str | None = "medicaid-fee-raw",
    s3_region: str = "eu-north-1",
) -> CSVExporterOutput:
    """
    Run the CSV Exporter agent.

    Args:
        input_data: CSVExporterInput with source info and mappings
        upload_to_s3_bucket: Optional S3 bucket name to upload raw CSV
        s3_region: AWS region for S3 bucket (default: us-east-1)

    Returns:
        CSVExporterOutput with export results and optional S3 info
    """
    logger.info(f"Starting CSV export for {input_data.state_name}")

    # Instead of using the agent (which has tool issues), directly call the export logic
    try:
        from .tools.download import download_or_cache_file
        from .tools.reader import read_and_map_data
        from .tools.writer import export_to_csv
        from pathlib import Path

        # Step 1: Download or use cached file
        output_dir = str(Path(input_data.output_path).parent)
        download_result = download_or_cache_file(
            source_url=input_data.source_url,
            file_type=input_data.file_type,
            output_dir=output_dir,
            cached_file_path=input_data.cached_file_path
        )

        if download_result["error"]:
            logger.error(f"Download failed: {download_result['error']}")
            return CSVExporterOutput(
                success=False,
                error=f"Download failed: {download_result['error']}"
            )

        file_path = download_result["file_path"]
        logger.info(
            f"File ready: {file_path} (cached={download_result['cached']})")

        # Step 2: Read and map data
        read_result = read_and_map_data(
            file_path=file_path,
            file_type=input_data.file_type,
            sheet_name=input_data.sheet_name,
            column_mapping=input_data.column_mapping,
            state_name=input_data.state_name,
            state_code=input_data.state_code,
            source_url=input_data.source_url,
            category=input_data.category,
            extraction_date=input_data.download_timestamp
        )

        if read_result["error"]:
            logger.error(f"Read/map failed: {read_result['error']}")
            return CSVExporterOutput(
                success=False,
                error=f"Read/map failed: {read_result['error']}"
            )

        dataframe = read_result["dataframe"]
        logger.info(
            f"Mapped {read_result['rows']} rows with {len(read_result['columns'])} columns")

        # Step 3: Export to CSV
        export_result = export_to_csv(
            dataframe=dataframe,
            output_path=input_data.output_path
        )

        if not export_result["success"]:
            logger.error(f"Export failed: {export_result['error']}")
            return CSVExporterOutput(
                success=False,
                error=f"Export failed: {export_result['error']}"
            )

        # Step 4: Upload to S3 if requested
        s3_info = None

        if upload_to_s3_bucket:
            try:
                state_name = input_data.state_name.lower().replace(' ', '_')
                s3_key = f"raw_exports/{state_name}/{Path(input_data.output_path).name}"

                s3_info = upload_to_s3(
                    file_path=input_data.output_path,
                    bucket_name=upload_to_s3_bucket,
                    s3_key=s3_key,
                    region=s3_region,
                )
                logger.info(
                    f"✅ Uploaded raw CSV to S3..............: {json.dumps(s3_info)}")
                logger.info(f"✅ Uploaded raw CSV to S3: {s3_info['s3_uri']}")
            except Exception as e:
                logger.error(f"S3 upload failed: {e}")
                # Don't fail the whole export, just log the error

        # Success!
        logger.info(f"✅ CSV export completed: {input_data.output_path}")

        output = CSVExporterOutput(
            success=True,
            rows_exported=export_result["rows"],
            columns=read_result["columns"],
            output_path=export_result["path"],
            metadata={
                "source_url": input_data.source_url,
                "state_name": input_data.state_name,
                "category": input_data.category,
                "cached_file_used": download_result["cached"],
                "res": s3_info['res'] if s3_info else None
            },
        )

        # Add S3 info if uploaded
        if s3_info:
            output.output_path = s3_info['s3_uri']

        return output

    except Exception as e:
        logger.error(f"CSV export failed: {e}", exc_info=True)
        return CSVExporterOutput(
            success=False,
            error=str(e)
        )


if __name__ == "__main__":
    app.run()

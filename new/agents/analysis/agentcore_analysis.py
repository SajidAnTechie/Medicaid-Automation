"""
Analysis Agent — "The Mapper"
------------------------------
Strands-agents powered agent that receives CSV data and metadata JSON,
then creates canonical header mappings aligned to the schema structure.
"""

from __future__ import annotations
from dotenv import load_dotenv
import json
from strands.models.bedrock import BedrockModel
from strands import Agent
from typing import Any
import logging
import urllib.request
import csv
from bedrock_agentcore.runtime import BedrockAgentCoreApp

# Inlined from schema.models to avoid cross-package dependency in AgentCore container
SCHEMA_DESCRIPTION = """
Canonical output fields — all records must use these exact key names:

  procedure_code  (str)   : HCPCS or CPT code, e.g. "D0120" or "99213"
  modifier        (str)   : optional procedure modifier, e.g. "TC" — use null if absent
  fee_amount      (float) : reimbursement rate as a plain number, e.g. 45.50
  effective_date  (str)   : ISO date string YYYY-MM-DD — use null if not present
  end_date        (str)   : ISO date string YYYY-MM-DD — use null if not present

Do NOT include state_code or dataset_type — those are added automatically.
"""

load_dotenv()


logger = logging.getLogger(__name__)

# Optional S3 support
try:
    import boto3
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    logger.warning("boto3 not available - S3 download/upload disabled")

app = BedrockAgentCoreApp()

# ── System Prompt ─────────────────────────────────────────────────────────────

ANALYSIS_SYSTEM_PROMPT = f"""You are the **Analysis Agent** (alias: "The Mapper") in the Sentinel-State pipeline.
Your mission is to analyze CSV data and metadata JSON to create canonical header mappings that align with the schema structure.

## CONTEXT
You are part of an automated Medicaid fee schedule ingestion pipeline. Your job is to map raw CSV headers 
to canonical schema-compliant field names for downstream processing.

{SCHEMA_DESCRIPTION}

═══════════════════════════════════════════════════════════════════════════════
## WORKFLOW
═══════════════════════════════════════════════════════════════════════════════

### STEP 1: ANALYZE CSV HEADERS
────────────────────────────────────────────────────────────────────────────────
**1.1 Review Headers**
   - Examine all CSV column headers
   - Note any patterns, prefixes, or suffixes
   - Identify potential groupings (e.g., fee columns, modifier columns)

**1.2 Review Sample Data**
   - Examine sample rows to understand data types and formats
   - Look for patterns that clarify ambiguous headers
   - Note any special formatting or encoding

### STEP 2: REVIEW METADATA CONTEXT
────────────────────────────────────────────────────────────────────────────────
**2.1 Extract Metadata Insights**
   - Review state name and context
   - Note any field descriptions or documentation
   - Identify date formats and currency conventions

### STEP 3: CREATE CANONICAL MAPPINGS
────────────────────────────────────────────────────────────────────────────────
**3.1 Map Each Header**
   For each CSV header:
   - Identify the most appropriate canonical field name from the schema
   - Assign confidence level: high (90%+ certain), medium (60-89%), low (<60%)
   - Provide clear reasoning for the mapping choice

**3.2 Confidence Criteria**
   - HIGH: Direct match or obvious synonym (e.g., "CPT Code" → "procedure_code")
   - MEDIUM: Reasonable inference needed (e.g., "Rate" → "fee_medicaid")
   - LOW: Ambiguous or uncertain mapping

**3.3 Handle Unmapped Headers**
   For headers that don't map to existing schema fields:
   - Generate a descriptive canonical name based on the header title
   - Use snake_case format (e.g., "Provider Type" → "provider_type")
   - Keep names concise but meaningful
   - Mark confidence as "low" and note that it's a generated field
   - Include these in canonical_headers, not in unmapped_headers
   
   Examples:
   - "Internal Notes" → canonical_name: "notes" (confidence: low, notes: "State-specific field, generated name")
   - "Last Updated" → canonical_name: "last_updated" (confidence: low, notes: "Metadata field, generated name")
   - "Region Code" → canonical_name: "region_code" (confidence: low, notes: "State-specific identifier, generated name")

### STEP 4: TRANSFORM AND CLEAN CSV DATA
────────────────────────────────────────────────────────────────────────────────
**IMPORTANT**: Process data in manageable chunks to avoid token limits.

**4.1 Clean Data Values**
   For each data value in the current chunk:
   - Remove leading/trailing whitespace
   - Replace multiple spaces with single space
   - Remove zero-width characters (\\u200b, \\u200c, \\u200d, \\ufeff)
   - Replace smart quotes with standard quotes (" " → ", ' ' → ')
   - Replace em/en dashes with standard hyphens (— → -, – → -)
   - Replace non-breaking spaces (\\xa0, \\u00a0) with regular spaces
   - Remove other problematic unicode characters that may cause database issues
   - Convert empty strings or "N/A", "NULL", "None", "-", "" to null/None
   
   Example cleaning:
     ```
     Before: "  99213\\u200b  " → After: "99213"
     Before: "$75.00\\xa0" → After: "$75.00"
     Before: "Dr's Office" → After: "Dr's Office"
     Before: "N/A" → After: null
     ```

**4.2 Transform Amount and Rate Fields**
   For columns that contain monetary amounts or rates (e.g., fee_medicaid, fee_medicare, rate, amount, price, fee_amount):
   - Remove currency symbols ($, €, £, etc.)
   - Remove comma separators (1,234.56 → 1234.56)
   - Convert to float/decimal type
   - If value is empty, "N/A", "-", "0.00", or cannot be converted → set to null
   - Preserve precision (e.g., 75.00 stays as 75.00, not 75)
   
   Example transformations:
     ```
     "$75.00" → 75.00 (float)
     "1,234.56" → 1234.56 (float)
     "N/A" → null
     "-" → null
     "" → null
     "$0.00" → 0.00 (float, keep zero values)
     "75" → 75.00 (float)
     ```
   
   **Rate field identification**: A field is considered a rate/amount field if:
   - Canonical name contains: "fee", "rate", "amount", "price", "cost", "payment", "reimbursement"
   - OR raw header contains: "fee", "rate", "$", "amount", "price", "cost", "payment", "reimbursement"

**4.3 Transform Boolean/Flag Fields**
   For columns that contain boolean flags or indicators (e.g., requires_*, is_*, has_*, needs_*):
   - Convert "X", "x" → true (boolean)
   - Convert all other values (empty string, null, any other text) → false (boolean)
   
   Example transformations:
     ```
     "X" → true (boolean)
     "x" → true (boolean)
     ```
   
   **Boolean field identification**: A field is considered a boolean/flag field if:
   - Canonical name contains: "requires_", "is_", "has_", "needs_", "allows_", "enable"
   - OR canonical name ends with: "_flag", "_indicator", "_required", "_allowed", "_enabled"
   - OR raw header contains: "requires", "is", "has", "needs", "flag", "indicator"

**4.4 Apply Header Mappings**
   - Transform each row in the current chunk using the canonical header mappings
   - Replace raw header names with canonical names
   - Use cleaned and type-converted data values

**4.5 Create Transformed Chunk**
   - Convert ALL rows in this chunk to use canonical headers with cleaned data
   - Maintain data integrity while ensuring database compatibility
   - All rate/amount fields should be float or null
   - All boolean/flag fields should be true or false (never null unless explicitly missing)
   - Example transformation:
     ```
     Original: {{"CPT Code": "  99213\\u200b", "Medicaid Rate": "$75.00\\xa0", "Requires Auth": "X", "Fee": "N/A"}}
     Cleaned & Transformed: {{"procedure_code": "99213", "fee_medicaid": 75.00, "requires_auth": true, "fee": null}}
     ```

### STEP 5: OUTPUT RESULTS
────────────────────────────────────────────────────────────────────────────────
Return a single JSON object (NO markdown fences):
{{
  "canonical_headers": [
    {{
      "raw_header": "original_column_name",
      "canonical_name": "schema_field_name",
      "confidence": "high|medium|low",
      "notes": "why this mapping was chosen"
    }}
  ],
  "transformed_data": [
    {{
      "canonical_field_1": "value1",
      "canonical_field_2": "value2"
    }}
    // ... ALL rows from the current chunk with canonical headers and cleaned values
  ],
  "unmapped_headers": [],
  "analysis_notes": "overall observations about the data structure",
  "cleaning_summary": {{
    "total_values_cleaned": <int>,
    "rate_fields_converted": <int>,
    "boolean_fields_converted": <int>,
    "cleaning_issues_found": ["whitespace", "unicode_characters", "smart_quotes", "currency_symbols", "flag_values", "etc"]
  }}
}}

**IMPORTANT**: 
- unmapped_headers should be empty. ALL headers must be mapped to either:
  - Existing schema fields (with high/medium confidence), OR
  - Generated canonical names (with low confidence and clear notes)
- transformed_data must contain ALL rows from the current chunk, with canonical header names applied and cleaned data values
- All rate/amount fields must be converted to float or null (never string)
- All boolean/flag fields must be converted to true or false (never "X", "Y", or empty string)
- cleaning_summary should report what data cleaning operations were performed, including rate and boolean field conversions

═══════════════════════════════════════════════════════════════════════════════
## MAPPING RULES
═══════════════════════════════════════════════════════════════════════════════
- Prefer canonical names from the schema for standard fields
- For headers without schema match, generate meaningful canonical names in snake_case
- Mark confidence level based on certainty of the mapping
- ALL headers must be mapped - no entries in unmapped_headers
- Provide clear reasoning in notes for ambiguous or generated mappings
- Ensure all important data fields are captured with descriptive names
"""


# ── Factory ───────────────────────────────────────────────────────────────────

def create_analysis_agent(
    model_id: str = "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
    region: str = "eu-north-1",
) -> Agent:
    """
    Factory that builds a Strands Analysis Agent wired to Amazon Bedrock.

    The agent is configured with:
      - ANALYSIS_SYSTEM_PROMPT (schema-aware mapping instructions)
      - Low temperature (0.1) for deterministic, consistent output

    Args:
        model_id: Bedrock model identifier.
                  Default: 'eu.anthropic.claude-sonnet-4-5-20250929-v1:0'
        region:   AWS region for the Bedrock runtime.
                  Default: 'eu-north-1'

    Returns:
        Agent: A ready-to-invoke Strands Agent instance.
    """
    bedrock_model = BedrockModel(
        model_id=model_id,
        region_name=region,
        temperature=0.1,
        max_tokens=8192,
    )

    return Agent(
        model=bedrock_model,
        system_prompt=ANALYSIS_SYSTEM_PROMPT,
        tools=[],
    )


# ── Runner ────────────────────────────────────────────────────────────────────
def run_analysis(
    csv_url: str,
    metadata_json: dict = {},
    chunk_size: int = 50,
    skip_chunking: bool = False,
) -> dict:
    """
    Execute the Analysis Agent end-to-end for CSV header mapping and transformation.

    Uses chunking to process large datasets without exceeding token limits.

    Workflow:
      1. Fetch CSV data from the provided URL (max 100 rows by default)
      2. If skip_chunking=True: Process all data at once
      3. If skip_chunking=False: Split data into chunks and process
      4. Return structured results with mappings and fully transformed dataset

    Args:
        csv_url: URL to the CSV file from csv_exporter agent
        metadata_json: Metadata from extractor agent
        chunk_size: Number of rows to process per chunk (default: 50)
        skip_chunking: If True, process all fetched data at once (default: False)

    Returns:
        dict with:
          - canonical_headers: list[dict] — header mappings with confidence
          - transformed_data: list[dict] — ALL data with canonical headers and cleaned values
          - unmapped_headers: list[str] — should be empty (all headers mapped)
          - analysis_notes: str — overall analysis observations
          - cleaning_summary: dict — summary of data cleaning operations performed
    """
    agent = create_analysis_agent()

    # Fetch CSV data (max 100 rows by default from _fetch_csv_data)
    csv_headers, all_data = _fetch_csv_data(csv_url)

    total_rows = len(all_data)
    logger.info(f"Fetched {total_rows} rows from CSV")

    # If data is small or skip_chunking is True, process all at once
    if skip_chunking or total_rows <= chunk_size:
        logger.info(f"Processing all {total_rows} rows at once")

        analysis_result = _analyze_chunk(
            agent,
            csv_headers,
            all_data,
            metadata_json,
            canonical_headers=None,
            chunk_idx=0,
        )

        return {
            'canonical_headers': analysis_result['canonical_headers'],
            'transformed_data': analysis_result['transformed_data'],
            'unmapped_headers': [],
            'analysis_notes': analysis_result.get('analysis_notes', ''),
            'cleaning_summary': analysis_result.get('cleaning_summary', {}),
        }

    # Otherwise, use chunking for larger datasets
    logger.info(f"Processing {total_rows} rows in chunks of {chunk_size}")

    # Split data into chunks
    chunks = [all_data[i:i + chunk_size]
              for i in range(0, total_rows, chunk_size)]

    all_transformed_data = []
    canonical_headers = None
    analysis_notes = ""
    cleaning_summary = {}

    # Process each chunk
    for chunk_idx, chunk in enumerate(chunks):
        logger.info(
            f"Processing chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk)} rows)")

        # For first chunk, get canonical headers
        # For subsequent chunks, only transform data
        chunk_result = _analyze_chunk(
            agent,
            csv_headers,
            chunk,
            metadata_json,
            canonical_headers,
            chunk_idx,
        )

        # Save canonical headers from first chunk
        if chunk_idx == 0:
            canonical_headers = chunk_result['canonical_headers']
            analysis_notes = chunk_result.get('analysis_notes', '')
            cleaning_summary = chunk_result.get('cleaning_summary', {})

        # Accumulate transformed data
        all_transformed_data.extend(chunk_result['transformed_data'])

    # Return combined results
    return {
        'canonical_headers': canonical_headers,
        'transformed_data': all_transformed_data,
        'unmapped_headers': [],
        'analysis_notes': analysis_notes,
        'cleaning_summary': cleaning_summary,
    }


def export_to_csv(transformed_data: list[dict], output_path: str) -> None:
    """
    Export transformed data to CSV format.

    Args:
        transformed_data: List of transformed data rows with canonical headers
        output_path: Path where CSV file should be saved
    """
    if not transformed_data:
        raise ValueError("No data to export")

    # Get headers from first row
    headers = list(transformed_data[0].keys())

    # Write to CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(transformed_data)

    logger.info(f"Exported {len(transformed_data)} rows to {output_path}")


def upload_to_s3(
    file_path: str,
    bucket_name: str,
    s3_key: str | None = None,
    region: str = "us-east-1",
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
        s3_client.upload_file(file_path, bucket_name, s3_key)

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
        }
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise RuntimeError(f"S3 upload failed: {e}")


# ── Backwards Compatibility Alias ─────────────────────────────────────────────

def run(
    csv_url: str,
    metadata_json: dict = {},
    output_csv_path: str | None = None,
    upload_to_s3_bucket: str = "medicaid-fee-stage",
    s3_region: str = "eu-north-1",
) -> dict:
    """
    Alias for run_analysis() for backwards compatibility.

    Args:
        csv_url: URL to the CSV file from csv_exporter agent
        metadata_json: Metadata from extractor agent
        output_csv_path: Optional path to export cleaned CSV (auto-generated if None)
        upload_to_s3_bucket: S3 bucket name to upload cleaned CSV (default: medicaid-fee-stage)
        s3_region: AWS region for S3 bucket (default: eu-north-1)

    Returns:
        dict with canonical_headers, transformed_data, csv_output_path, s3_upload, etc.
    """
    result = run_analysis(csv_url=csv_url, metadata_json=metadata_json)

    # Auto-generate CSV output path if not provided
    if not output_csv_path and result.get('transformed_data'):
        # Try to derive path from csv_url
        from pathlib import Path
        import re

        # Use /tmp inside AgentCore containers (output/ doesn't exist)
        _tmp_dir = Path("/tmp/analysis_output")
        _tmp_dir.mkdir(parents=True, exist_ok=True)

        # Extract filename from URL
        if csv_url.startswith('file://'):
            csv_path = csv_url.replace('file://', '')
            output_csv_path = str(_tmp_dir /
                                  f"{Path(csv_path).stem}_cleaned.csv")
        elif csv_url.startswith('s3://'):
            # For S3 URIs, extract filename
            filename = csv_url.split('/')[-1].replace('.csv', '_cleaned.csv')
            output_csv_path = str(_tmp_dir / filename)
        else:
            # For HTTP URLs, use tmp directory
            state_key = metadata_json.get(
                'state_name', 'unknown').lower().replace(' ', '_')
            output_csv_path = str(_tmp_dir / f"cleaned_data_{state_key}.csv")

    # Export to CSV if we have data
    if output_csv_path and result.get('transformed_data'):
        try:
            export_to_csv(result['transformed_data'], output_csv_path)
            result['csv_output_path'] = output_csv_path
            logger.info(f"✅ Exported cleaned CSV to {output_csv_path}")

            # Upload to S3 if bucket specified
            if upload_to_s3_bucket:
                try:
                    from pathlib import Path
                    # Derive state name from the CSV filename (e.g. "alaska_raw_data_cleaned.csv" → "alaska")
                    # e.g. "alaska_raw_data_cleaned"
                    csv_stem = Path(output_csv_path).stem
                    state_name = csv_stem.split(
                        '_')[0] if csv_stem else 'unknown'
                    s3_key = f"cleaned_data/{state_name}/{Path(output_csv_path).name}"

                    s3_info = upload_to_s3(
                        file_path=output_csv_path,
                        bucket_name=upload_to_s3_bucket,
                        s3_key=s3_key,
                        region=s3_region,
                    )
                    result['s3_file_path'] = s3_info['s3_uri']
                    logger.info(
                        f"✅ Uploaded cleaned CSV to S3: {s3_info['s3_uri']}")
                except Exception as e:
                    logger.error(f"S3 upload failed: {e}")
                    result['s3_upload_error'] = str(e)

        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            result['csv_export_error'] = str(e)

    return result


# ── AgentCore entrypoint ──────────────────────────────────────────────────────
@app.entrypoint
def invoke(payload: dict[str, Any]) -> dict[str, Any]:
    """
    AWS AgentCore–compatible entrypoint.

    Accepts a payload dict and returns a dict. Maps cleanly to::

        @app.entrypoint
        def handler(payload):
            return invoke(payload)

    Payload keys (both required)::

        {
            "csv_url": "https://...",           # REQUIRED - URL to CSV file
            "metadata_json": {...}              # REQUIRED - metadata from extractor
        }

    Returns:
        dict — the analysis result with canonical headers, unmapped headers, and notes.

    Raises:
        ValueError: If required fields are missing.
    """

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            # Treat a bare string as the portal URL itself
            payload = {"portal_url": payload}

    csv_url = (
        payload.get("portal_url")
        or payload.get("prompt")
        or payload.get("input", {}).get("prompt")  # AgentCore nested format
    )
    # metadata_json = payload.get("metadata_json", {})

    if not csv_url or not isinstance(csv_url, str):
        return {
            "success": False,
            "error": "Missing required field: 'csv_url' in payload.",
            "canonical_headers": [],
            "transformed_data": [],
            "unmapped_headers": [],
            "analysis_notes": "",
            "cleaning_summary": {},
        }

    # if not metadata_json or not isinstance(metadata_json, dict):
    #     return {
    #         "success": False,
    #         "error": "Missing required field: 'metadata_json' in payload.",
    #         "canonical_headers": [],
    #         "transformed_data": [],
    #         "unmapped_headers": [],
    #         "analysis_notes": "",
    #         "cleaning_summary": {},
    #     }

    try:
        result = run(csv_url=csv_url, metadata_json={})
        result["success"] = True
        return {
            "success": True,
            "output_filepath": result.get("s3_file_path", "")
        }
    except Exception as exc:  # noqa: BLE001
        logger.error("Analysis execution failed: %s", exc)
        return {
            "success": False,
            "error": f"Analysis error: {exc}",
            "canonical_headers": [],
            "transformed_data": [],
            "unmapped_headers": [],
            "analysis_notes": "",
            "cleaning_summary": {},
        }


# ── JSON extraction helper ────────────────────────────────────────────────────

def _extract_json_from_response(text: str) -> dict[str, Any]:
    """
    Robustly extract the first complete JSON object from free-form
    agent response text.

    Tries three strategies in order:
      1. Direct json.loads() — works when the response is pure JSON.
      2. Regex for ```json ... ``` markdown code blocks.
      3. Brace-depth matching — walks the string character by character,
         tracking { } nesting, and attempts to parse each outermost
         brace pair.

    Args:
        text: The raw string returned by the Strands agent.
              May contain markdown fences, surrounding prose, or
              explanation text before/after the JSON.

    Returns:
        dict: The parsed JSON object.

    Raises:
        ValueError: If no valid JSON object could be found anywhere
                    in the response text.
    """
    import re

    # 1. Direct parse
    try:
        return json.loads(text)  # type: ignore[return-value]
    except json.JSONDecodeError:
        pass

    # 2. Strip markdown code block
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))  # type: ignore[return-value]
        except json.JSONDecodeError:
            pass

    # 3. Brace-matching fallback
    depth = 0
    start: int | None = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    # type: ignore[return-value]
                    return json.loads(text[start: i + 1])
                except json.JSONDecodeError:
                    start = None

    raise ValueError("No valid JSON object found in agent response")


# ── CSV Data Fetching ─────────────────────────────────────────────────────────

def _fetch_csv_data(csv_url: str, max_rows: int = 100) -> tuple[list[str], list[dict]]:
    """
    Fetch CSV file and extract headers and data (limited to max_rows).
    Supports local files (file://), HTTP URLs, and S3 URIs (s3://).

    Args:
        csv_url: URL to the CSV file (file://, http://, https://, or s3://)
        max_rows: Maximum number of rows to fetch (default: 100)

    Returns:
        Tuple of (headers, data)
    """
    try:
        # Handle S3 URIs
        if csv_url.startswith('s3://'):
            if not S3_AVAILABLE:
                raise RuntimeError(
                    "boto3 is required to fetch from S3. Install with: pip install boto3")

            # Parse S3 URI: s3://bucket-name/key/path
            import re
            match = re.match(r's3://([^/]+)/(.+)', csv_url)
            if not match:
                raise ValueError(f"Invalid S3 URI format: {csv_url}")

            bucket_name = match.group(1)
            s3_key = match.group(2)

            logger.info(f"Fetching CSV from S3: {bucket_name}/{s3_key}")

            # Download from S3
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')

        # Handle local file:// URIs
        elif csv_url.startswith('file://'):
            with urllib.request.urlopen(csv_url) as response:
                content = response.read().decode('utf-8')

        # Handle HTTP/HTTPS URLs
        else:
            with urllib.request.urlopen(csv_url) as response:
                content = response.read().decode('utf-8')

        # Parse CSV content
        lines = content.splitlines()
        reader = csv.DictReader(lines)

        headers = reader.fieldnames or []
        data = []

        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            data.append(dict(row))

        logger.info(f"Fetched {len(data)} rows from {csv_url}")
        return headers, data

    except Exception as e:
        raise RuntimeError(f"Failed to fetch CSV data from {csv_url}: {e}")


# ── Header Analysis via LLM ───────────────────────────────────────────────────

def _analyze_chunk(
    agent: Agent,
    csv_headers: list[str],
    chunk_data: list[dict],
    metadata_json: dict,
    canonical_headers: list[dict] | None,
    chunk_idx: int,
) -> dict:
    """
    Use Strands Agent to analyze headers and transform a chunk of data.

    Args:
        agent: Strands Agent instance from create_analysis_agent
        csv_headers: List of raw CSV headers
        chunk_data: Chunk of rows from CSV
        metadata_json: Metadata from extractor agent
        canonical_headers: Previously determined headers (None for first chunk)
        chunk_idx: Index of the current chunk

    Returns:
        Analysis result with canonical headers, transformed data, and notes
    """
    if chunk_idx == 0:
        # First chunk: analyze headers AND transform data
        prompt = f"""Analyze the following data and create a canonical header list, then transform the data.

CSV Headers:
{json.dumps(csv_headers, indent=2)}

CSV Data Chunk 1 ({len(chunk_data)} rows):
{json.dumps(chunk_data, indent=2, default=str)}

Metadata JSON:
{json.dumps(metadata_json, indent=2, default=str)}

Based on the schema definition in the system prompt:
1. Create a comprehensive mapping of these headers to canonical field names
2. Transform ALL {len(chunk_data)} rows in this chunk with cleaned and type-converted values

Return ONLY a valid JSON object — no explanation, no markdown fences, no extra text."""
    else:
        # Subsequent chunks: only transform data using existing mappings
        prompt = f"""Transform the following data using the canonical header mappings you created.

Canonical Headers (use these):
{json.dumps(canonical_headers, indent=2)}

CSV Data Chunk {chunk_idx + 1} ({len(chunk_data)} rows):
{json.dumps(chunk_data, indent=2, default=str)}

Transform ALL {len(chunk_data)} rows in this chunk with cleaned and type-converted values.
Use the SAME canonical header mappings as before.

Return ONLY a valid JSON object with transformed_data field — no explanation, no markdown fences, no extra text.
Format: {{"transformed_data": [...], "cleaning_summary": {{...}}}}"""

    # Use Strands Agent to get response
    try:
        result = agent(prompt)
        response_text = str(result)
        logger.debug("Raw agent response length: %d", len(response_text))

        # Extract JSON from response
        parsed = _extract_json_from_response(response_text)

        # For subsequent chunks, add back the canonical_headers
        if chunk_idx > 0 and 'canonical_headers' not in parsed:
            parsed['canonical_headers'] = canonical_headers
            parsed['unmapped_headers'] = []
            parsed['analysis_notes'] = ''

        return parsed
    except Exception as exc:  # noqa: BLE001
        logger.error("Chunk analysis failed: %s", exc)
        raise RuntimeError(f"Failed to analyze chunk {chunk_idx}: {exc}")


def _analyze_headers(
    agent: Agent,
    csv_headers: list[str],
    all_data: list[dict],
    metadata_json: dict,
) -> dict:
    """
    DEPRECATED: Use run_analysis with chunking instead.

    Use Strands Agent to analyze headers and transform ALL data.

    Args:
        agent: Strands Agent instance from create_analysis_agent
        csv_headers: List of raw CSV headers
        all_data: ALL rows from CSV
        metadata_json: Metadata from extractor agent

    Returns:
        Analysis result with canonical headers, transformed data, and notes
    """
    prompt = f"""Analyze the following data and create a canonical header list, then transform ALL data.

CSV Headers:
{json.dumps(csv_headers, indent=2)}

ALL CSV Data ({len(all_data)} rows):
{json.dumps(all_data, indent=2, default=str)}

Metadata JSON:
{json.dumps(metadata_json, indent=2, default=str)}

Based on the schema definition in the system prompt:
1. Create a comprehensive mapping of these headers to canonical field names
2. Transform ALL {len(all_data)} rows with cleaned and type-converted values

Return ONLY a valid JSON object — no explanation, no markdown fences, no extra text."""

    # Use Strands Agent to get response
    try:
        result = agent(prompt)
        response_text = str(result)
        logger.debug("Raw agent response length: %d", len(response_text))

        # Extract JSON from response
        parsed = _extract_json_from_response(response_text)
        return parsed
    except Exception as exc:  # noqa: BLE001
        logger.error("Header analysis failed: %s", exc)
        raise RuntimeError(f"Failed to analyze headers: {exc}")


# ── CLI Entry Point ───────────────────────────────────────────────────────────


# def main():
#     """
#     CLI entry point for standalone usage.

#     Usage:
#         python -m agents.analysis.agent <csv_path> <metadata_json_path>
#     """
#     import sys
#     from pathlib import Path

#     if len(sys.argv) < 3:
#         print("Usage: python -m agents.analysis.agent <csv_path> <metadata_json_path>")
#         print("\nExample:")
#         print("  python -m agents.analysis.agent output/alaska_raw_data_1.csv output/alaska_extractor_result_0.json")
#         sys.exit(1)

#     csv_path = sys.argv[1]
#     metadata_json_path = sys.argv[2]

#     csv_file = Path(csv_path)
#     metadata_file = Path(metadata_json_path)

#     # Validate inputs
#     if not csv_file.exists():
#         print(f"❌ CSV file not found: {csv_path}")
#         sys.exit(1)

#     if not metadata_file.exists():
#         print(f"❌ Metadata JSON file not found: {metadata_json_path}")
#         sys.exit(1)

#     # Load metadata
#     with open(metadata_file, "r") as f:
#         metadata_json = json.load(f)

#     # Determine output path
#     output_path = csv_file.parent / f"{csv_file.stem}_analysis_result.json"
#     output_csv_path = csv_file.parent / f"{csv_file.stem}_cleaned.csv"

#     print()
#     print("=" * 80)
#     print("🔍 ANALYSIS AGENT - Canonical Header Mapping")
#     print("=" * 80)
#     print()
#     print(f"CSV File: {csv_file}")
#     print(f"Metadata: {metadata_file}")
#     print(f"Output JSON: {output_path}")
#     print(f"Output CSV: {output_csv_path}")
#     print()

#     # Run analysis
#     try:
#         csv_url = f"file://{csv_file.absolute()}"

#         print("Running analysis...")
#         analysis_result = run_analysis(
#             csv_url=csv_url,
#             metadata_json=metadata_json,
#             chunk_size=50,  # Process 50 rows at a time
#         )

#         # Save results
#         with open(output_path, "w") as f:
#             json.dump(analysis_result, f, indent=2)

#         # Export to CSV
#         export_to_csv(
#             analysis_result['transformed_data'], str(output_csv_path))

#         print()
#         print("✅ Analysis Complete!")
#         print(
#             f"   Canonical headers: {len(analysis_result['canonical_headers'])}")
#         print(
#             f"   Total rows transformed: {len(analysis_result.get('transformed_data', []))}")
#         print(
#             f"   Unmapped headers: {len(analysis_result['unmapped_headers'])}")

#         # Display cleaning summary
#         if analysis_result.get('cleaning_summary'):
#             cleaning = analysis_result['cleaning_summary']
#             print()
#             print("🧹 Data Cleaning Summary:")
#             print(
#                 f"   Values cleaned: {cleaning.get('total_values_cleaned', 0)}")
#             print(
#                 f"   Rate fields converted to float: {cleaning.get('rate_fields_converted', 0)}")
#             print(
#                 f"   Boolean fields converted: {cleaning.get('boolean_fields_converted', 0)}")
#             if cleaning.get('cleaning_issues_found'):
#                 print(
#                     f"   Issues found: {', '.join(cleaning['cleaning_issues_found'])}")

#         # Display mappings
#         if analysis_result['canonical_headers']:
#             print()
#             print("📋 Header Mappings:")
#             for mapping in analysis_result['canonical_headers']:
#                 confidence_icon = {'high': '✅', 'medium': '⚠️',
#                                    'low': '❓'}.get(mapping['confidence'], '•')
#                 print(
#                     f"  {confidence_icon} {mapping['raw_header']} -> {mapping['canonical_name']} ({mapping['confidence']})")

#         if analysis_result['unmapped_headers']:
#             print()
#             print("❌ Unmapped Headers:")
#             for header in analysis_result['unmapped_headers']:
#                 print(f"  • {header}")

#         # Display sample of transformed data
#         if analysis_result.get('transformed_data'):
#             print()
#             print("📊 Transformed Data (first row):")
#             first_row = analysis_result['transformed_data'][0]
#             # Show first 5 fields
#             for key, value in list(first_row.items())[:5]:
#                 print(f"  • {key}: {value}")
#             if len(first_row) > 5:
#                 print(f"  ... and {len(first_row) - 5} more fields")

#         print()
#         print(f"📄 Results saved:")
#         print(f"   JSON: {output_path}")
#         print(f"   CSV:  {output_csv_path}")
#         print()

#     except Exception as e:
#         print()
#         print(f"❌ Analysis failed: {e}")
#         import traceback
#         traceback.print_exc()
#         sys.exit(1)


if __name__ == "__main__":
    app.run()

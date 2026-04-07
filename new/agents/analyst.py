"""
Analyst Agent
-------------
Receives raw records with original column names and uses an LLM to:
  1. Map each raw column to the canonical schema field name.
  2. Apply the mapping to produce canonically-keyed records.

Unknown or unmappable columns are logged in mapping_log with a null value.
No HITL in this MVP — unmapped columns are captured in rejections.json downstream.
"""

from __future__ import annotations

import json

import boto3

from schema.models import SCHEMA_DESCRIPTION

MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")


# ── Public entry point ────────────────────────────────────────────────────────

def run(
    state_code: str,
    dataset_type: str,
    raw_records: list[dict],
) -> dict:
    """
    Map raw column names to canonical schema fields via LLM.

    Returns:
        {
            "mapped_records": list[dict],   # canonical key names
            "mapping_log":    dict,         # {raw_col: canonical_col | null}
        }
    """
    if not raw_records:
        return {"mapped_records": [], "mapping_log": {}}

    raw_columns = list(raw_records[0].keys())
    sample_rows = raw_records[:5]

    mapping = _get_column_mapping(raw_columns, sample_rows)
    mapped_records = _apply_mapping(raw_records, mapping, state_code, dataset_type)

    return {
        "mapped_records": mapped_records,
        "mapping_log": mapping,
    }


# ── Column mapping via LLM ────────────────────────────────────────────────────

def _get_column_mapping(
    raw_columns: list[str],
    sample_rows: list[dict],
) -> dict:
    """
    Ask Claude to map raw column names to canonical field names.
    Returns a dict: {raw_column_name: canonical_field_name_or_null}
    """
    prompt = f"""You are a data mapping assistant for Medicaid fee schedule processing.

Your task: map each raw column name to the correct canonical field name.

Raw column names:
{json.dumps(raw_columns, indent=2)}

Sample data (first 5 rows — original column names):
{json.dumps(sample_rows, indent=2, default=str)}

{SCHEMA_DESCRIPTION}

Rules:
- Map each raw column to exactly one canonical field name from the schema above.
- If a raw column clearly has no canonical match, map it to null.
- Use the sample data values as context clues (e.g. a column with values like "D0120" is likely procedure_code).
- Return ONLY a valid JSON object — no explanation, no markdown fences, no extra text.

Expected format:
{{
  "RAW_COLUMN_NAME": "canonical_field_name",
  "ANOTHER_RAW_COL": null
}}"""

    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    body = json.loads(response["body"].read())
    text = body["content"][0]["text"].strip()

    # Strip accidental markdown fences if present
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.lower().startswith("json"):
            text = text[4:]

    return json.loads(text.strip())


# ── Apply mapping ─────────────────────────────────────────────────────────────

def _apply_mapping(
    raw_records: list[dict],
    mapping: dict,
    state_code: str,
    dataset_type: str,
) -> list[dict]:
    """
    Rename keys in every record according to the mapping dict.
    Adds state_code and dataset_type to every record.
    Columns mapped to null are dropped.
    """
    mapped = []
    for row in raw_records:
        new_row: dict = {
            "state_code": state_code,
            "dataset_type": dataset_type,
        }
        for raw_key, canonical_key in mapping.items():
            if canonical_key is not None and raw_key in row:
                value = row[raw_key]
                # Skip NaN / None / empty string values
                if value is None:
                    continue
                if isinstance(value, float) and value != value:  # NaN check
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                new_row[canonical_key] = value
        mapped.append(new_row)
    return mapped

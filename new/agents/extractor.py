"""
Extractor Agent
---------------
Downloads a file from a given URL and extracts raw records (list of dicts)
using the appropriate strategy per file type.

For scanned PDFs the agent falls back to Claude Vision via Amazon Bedrock.
"""

from __future__ import annotations

import base64
import json
import tempfile
import os
from io import BytesIO

import boto3
import httpx

MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")


# ── Public entry point ────────────────────────────────────────────────────────

def run(
    state_code: str,
    dataset_type: str,
    url: str,
    file_type: str,
) -> dict:
    """
    Download the file at `url` and extract raw records.

    Returns:
        {
            "state_code":   str,
            "dataset_type": str,
            "raw_records":  list[dict],   # original column names preserved
            "raw_columns":  list[str],
        }
    """
    raw_bytes = _download(url)

    dispatch = {
        "csv":   _extract_csv,
        "excel": _extract_excel,
        "pdf":   _extract_pdf,
    }
    if file_type not in dispatch:
        raise ValueError(f"Unsupported file_type: '{file_type}'")

    records = dispatch[file_type](raw_bytes)

    return {
        "state_code":   state_code,
        "dataset_type": dataset_type,
        "raw_records":  records,
        "raw_columns":  list(records[0].keys()) if records else [],
    }


# ── Download ──────────────────────────────────────────────────────────────────

def _download(url: str) -> bytes:
    response = httpx.get(url, follow_redirects=True, timeout=60)
    response.raise_for_status()
    return response.content


# ── CSV ───────────────────────────────────────────────────────────────────────

def _extract_csv(data: bytes) -> list[dict]:
    import csv
    import io

    text = data.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


# ── Excel ─────────────────────────────────────────────────────────────────────

def _extract_excel(data: bytes) -> list[dict]:
    import pandas as pd

    xf = pd.ExcelFile(BytesIO(data))

    # Pick the most relevant sheet by name
    keywords = ["rate", "fee", "schedule", "procedure", "code"]
    best_sheet = max(
        xf.sheet_names,
        key=lambda s: sum(k in s.lower() for k in keywords),
        default=xf.sheet_names[0],
    )

    # Ask LLM to identify the real header row
    raw = pd.read_excel(BytesIO(data), sheet_name=best_sheet, header=None)
    sample_csv = raw.head(10).to_csv(index=False)

    prompt = (
        "Given these first 10 rows of an Excel sheet (as CSV), "
        "identify the row index (0-based) of the true column header row. "
        "Rows above it are titles, merge cells, or blank lines. "
        "Reply with only the integer — nothing else.\n\n"
        f"{sample_csv}"
    )
    header_idx = int(_llm_text(prompt).strip())

    df = pd.read_excel(
        BytesIO(data), sheet_name=best_sheet, header=header_idx
    ).dropna(how="all")

    # Normalise column names: strip whitespace
    df.columns = [str(c).strip() for c in df.columns]

    return df.to_dict(orient="records")


# ── PDF ───────────────────────────────────────────────────────────────────────

def _extract_pdf(data: bytes) -> list[dict]:
    # Strategy 1: unstructured text extraction
    try:
        records = _extract_pdf_text(data)
        if records:
            return records
    except Exception:
        pass

    # Strategy 2: Vision LLM OCR fallback
    return _extract_pdf_vision(data)


def _extract_pdf_text(data: bytes) -> list[dict]:
    from unstructured.partition.pdf import partition_pdf
    import pandas as pd

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(data)
        tmp_path = f.name

    try:
        elements = partition_pdf(filename=tmp_path, strategy="hi_res")
    finally:
        os.unlink(tmp_path)

    tables = [e for e in elements if e.category == "Table"]
    if not tables:
        return []

    df = pd.read_html(tables[0].metadata.text_as_html)[0]
    return df.to_dict(orient="records")


def _extract_pdf_vision(data: bytes) -> list[dict]:
    """Render each PDF page to PNG and send to Claude Vision."""
    import fitz  # PyMuPDF

    doc = fitz.open(stream=data, filetype="pdf")
    all_records: list[dict] = []

    for page_num, page in enumerate(doc):
        pix = page.get_pixmap(dpi=150)
        img_b64 = base64.b64encode(pix.tobytes("png")).decode()

        prompt = (
            "This is a page from a Medicaid fee schedule document.\n"
            "Extract every data row as a JSON array of objects.\n"
            "Each object must use the original column headers as keys.\n"
            "If this page contains no table data, return an empty array [].\n"
            "Output ONLY valid JSON — no explanation, no markdown fences."
        )

        response = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "messages": [{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": img_b64,
                            },
                        },
                        {"type": "text", "text": prompt},
                    ],
                }],
            }),
        )
        body = json.loads(response["body"].read())
        text = body["content"][0]["text"].strip()

        try:
            page_records = json.loads(text)
            if isinstance(page_records, list):
                all_records.extend(page_records)
        except json.JSONDecodeError:
            print(f"  [extractor] Could not parse JSON from page {page_num + 1} — skipping")

    return all_records


# ── Shared LLM helper ─────────────────────────────────────────────────────────

def _llm_text(prompt: str, max_tokens: int = 64) -> str:
    """Single-turn text completion via Amazon Bedrock."""
    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    body = json.loads(response["body"].read())
    return body["content"][0]["text"]

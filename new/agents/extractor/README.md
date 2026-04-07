# Extractor Agent — "The Parser & Analyst"

The Extractor Agent is the **second stage** of the Sentinel-State pipeline. It receives URLs from the Navigator Agent, downloads fee schedule files, parses them into structured data, maps columns to a canonical schema, and validates data quality.

---

## Architecture

```
NavigatorOutput (ranked dataset URLs)
        │
        ▼
┌──────────────────────────────────┐
│     Extractor Agent              │
│     (Strands + Bedrock)          │
│                                  │
│  Phase 1: Extract                │
│    1. download_file  ──► HTTP download with retry
│    2. parse_file     ──► Excel/CSV/PDF/ZIP parsing
│                                  │
│  Phase 2: Analyze                │
│    3. map_columns    ──► LLM column mapping
│    4. validate       ──► Data quality checks
│                                  │
└──────────────────────────────────┘
        │
        ▼
ExtractorOutput (structured tables + mappings)
        │
        ▼
  Downstream: Archivist Agent (loads to database)
```

## File Structure

```
agents/extractor/
├── __init__.py               # Package exports
├── agent.py                  # Strands Agent definition + system prompt
├── models.py                 # Pydantic input/output schemas
├── README.md                 # This file
└── tools/
    ├── __init__.py
    ├── download_file.py      # HTTP download tool
    ├── parse_file.py         # Multi-format parser (Excel/CSV/PDF/ZIP)
    └── map_columns.py        # LLM column mapping prompt builder
```

---

## Prerequisites

```bash
pip install strands-agents pandas openpyxl pdfplumber requests boto3 pydantic
```

---

## Quick Start

### CLI Runner (Recommended)

```bash
# Run from project root (new/)
cd new

# Process top dataset from Navigator output
python -m agents.extractor.run --navigator-output output/alaska_navigator_result.json

# Process top 3 datasets
python -m agents.extractor.run --navigator-output output/florida_navigator_result.json --top 3

# Extract from a single URL
python -m agents.extractor.run \
  --url "https://example.com/physician_schedule.xlsx" \
  --state "Alaska" \
  --state-code "AK" \
  --file-type xlsx \
  --category physician \
  --title "Physician Fee Schedule FY2025"

# Enable debug logging
python -m agents.extractor.run --navigator-output output/alaska_navigator_result.json --debug
```

### Programmatic Usage

```python
from agents.extractor import run_extractor, ExtractorInput
from agents.extractor.models import FileType, DatasetCategory

# Create input (typically from Navigator output)
input_data = ExtractorInput(
    url="https://extranet-sp.dhss.alaska.gov/.../physician_fee_schedule_2025.xlsx",
    state_name="Alaska",
    state_code="AK",
    file_type=FileType.XLSX,
    category=DatasetCategory.PHYSICIAN,
    title="Physician Fee Schedule FY2025",
    extract_all_sheets=True,
    ocr_enabled=False,
)

# Run extractor
result = run_extractor(input_data)

# Access results
for table in result.extracted_tables:
    print(f"Sheet: {table.sheet_name}")
    print(f"Rows: {table.row_count}")
    print(f"Mapping: {table.column_mapping}")
    print(f"Confidence: {table.mapping_confidence}")
```

---

## Input — `ExtractorInput`

| Field                | Type              | Default   | Description                                                  |
| -------------------- | ----------------- | --------- | ------------------------------------------------------------ |
| `url`                | `str`             | —         | **Required.** Direct download URL of the dataset file.       |
| `state_name`         | `str`             | `""`      | State name (e.g., `"Alaska"`).                               |
| `state_code`         | `str`             | `""`      | Two-letter state code (e.g., `"AK"`).                        |
| `file_type`          | `FileType`        | `UNKNOWN` | Expected file type (`PDF`, `XLSX`, `CSV`, `ZIP`).            |
| `category`           | `DatasetCategory` | `UNKNOWN` | Expected category (`PHYSICIAN`, `DENTAL`, `PHARMACY`, etc.). |
| `title`              | `str`             | `""`      | Descriptive title of the dataset.                            |
| `extract_all_sheets` | `bool`            | `True`    | For Excel: extract all sheets or just the first.             |
| `ocr_enabled`        | `bool`            | `False`   | Enable OCR for scanned PDFs (slower but more accurate).      |

---

## Output — `ExtractorOutput`

| Field                     | Type                   | Description                                           |
| ------------------------- | ---------------------- | ----------------------------------------------------- |
| `success`                 | `bool`                 | Whether extraction completed successfully.            |
| `source_url`              | `str`                  | The URL that was processed.                           |
| `state_name`              | `str`                  | State name.                                           |
| `state_code`              | `str`                  | Two-letter state code.                                |
| `file_type`               | `FileType`             | Detected file type.                                   |
| `category`                | `DatasetCategory`      | Dataset category.                                     |
| `extracted_tables`        | `list[ExtractedTable]` | All tables extracted from the file.                   |
| `file_size_bytes`         | `int`                  | Downloaded file size.                                 |
| `download_timestamp`      | `str`                  | ISO-8601 timestamp.                                   |
| `file_metadata`           | `dict`                 | Additional metadata (effective dates, version, etc.). |
| `schema_drift_detected`   | `bool`                 | Whether schema differs from expected.                 |
| `schema_drift_details`    | `list[str]`            | Details about schema changes.                         |
| `data_quality_issues`     | `list[str]`            | Data quality warnings.                                |
| `errors`                  | `list[str]`            | Any errors encountered.                               |
| `total_rows_extracted`    | `int`                  | Total rows across all tables.                         |
| `processing_time_seconds` | `float`                | Time taken to extract and analyze.                    |

### `ExtractedTable` (each item in `extracted_tables`)

| Field                 | Type             | Description                                             |
| --------------------- | ---------------- | ------------------------------------------------------- |
| `sheet_name`          | `str`            | Sheet name (Excel) or "Page N" (PDF) or filename (CSV). |
| `headers`             | `list[str]`      | Column headers detected.                                |
| `data`                | `list[dict]`     | Rows as list of dictionaries (column → value).          |
| `row_count`           | `int`            | Number of data rows.                                    |
| `detected_header_row` | `int`            | Row index where headers were found.                     |
| `footer_notes`        | `str \| None`    | Footer text or disclaimers.                             |
| `column_mapping`      | `dict[str, str]` | Raw column → canonical column mapping.                  |
| `mapping_confidence`  | `float`          | LLM confidence score (0.0-1.0).                         |

---

## Canonical Schema

All extracted tables are mapped to this standard schema:

| Field                | Type          | Required | Description                     |
| -------------------- | ------------- | -------- | ------------------------------- |
| `procedure_code`     | `str`         | ✅       | CPT, HCPCS, CDT, or NDC code    |
| `description`        | `str`         | ✅       | Procedure/service description   |
| `reimbursement_rate` | `float`       | ✅       | Reimbursement amount in dollars |
| `modifier`           | `str \| None` | ❌       | Procedure modifier code         |
| `effective_date`     | `str \| None` | ❌       | Date when rate became effective |
| `end_date`           | `str \| None` | ❌       | Date when rate expires          |
| `unit_type`          | `str \| None` | ❌       | Unit of service                 |
| `place_of_service`   | `str \| None` | ❌       | Where service is provided       |
| `provider_type`      | `str \| None` | ❌       | Type of provider                |
| `notes`              | `str \| None` | ❌       | Additional notes                |

---

## Tools

### `download_file`

**Purpose:** Downloads a file from URL to temporary storage.

| Feature                     | Detail                                 |
| --------------------------- | -------------------------------------- |
| **Max file size**           | 500 MB                                 |
| **Streaming**               | Yes (low memory usage)                 |
| **Retry logic**             | 3 attempts with exponential backoff    |
| **Timeout**                 | 60 seconds (configurable)              |
| **Content-type validation** | Yes                                    |
| **Filename detection**      | From URL or Content-Disposition header |

### `parse_file`

**Purpose:** Parses downloaded files into structured tables.

| Format                  | Features                                                       |
| ----------------------- | -------------------------------------------------------------- |
| **Excel (.xlsx, .xls)** | Multi-sheet extraction, header detection, merged cell handling |
| **CSV (.csv)**          | Auto delimiter detection, encoding detection (UTF-8, Latin-1)  |
| **PDF (.pdf)**          | Table extraction via `pdfplumber`, optional OCR                |
| **ZIP (.zip)**          | Recursive extraction and processing of contained files         |

**Header Detection:** Heuristic-based (finds first row with >50% non-null cells)

### `map_columns`

**Purpose:** Generates LLM prompt for column mapping analysis.

**Mapping Criteria:**

1. **Column name similarity** to canonical schema fields
2. **Sample data patterns** (code formats, rate ranges, date formats)
3. **Category context** (physician vs dental vs pharmacy)
4. **Confidence scoring** (0.0-1.0)

**Handles:**

- Multiple possible mappings per raw column
- Ambiguous columns (manual review flagged)
- Missing canonical fields (schema drift detected)

---

## Data Quality Checks

The agent automatically validates:

| Check                  | Description                            |
| ---------------------- | -------------------------------------- |
| **Missing codes**      | Rows without procedure codes           |
| **Invalid rates**      | Rates ≤ $0.00 or non-numeric           |
| **Empty rows**         | Completely blank rows                  |
| **Duplicate codes**    | Same code + modifier combination       |
| **Date format issues** | Unparseable dates                      |
| **Schema drift**       | New/removed columns vs expected schema |

---

## Example Output (JSON)

```json
{
  "success": true,
  "source_url": "https://.../physician_fee_schedule_2025.xlsx",
  "state_name": "Alaska",
  "state_code": "AK",
  "file_type": "xlsx",
  "category": "physician",
  "extracted_tables": [
    {
      "sheet_name": "Professional Services",
      "headers": ["Procedure Code", "Description", "Rate", "Mod", "Eff Date"],
      "data": [
        {
          "Procedure Code": "99213",
          "Description": "Office visit, established patient",
          "Rate": "75.50",
          "Mod": null,
          "Eff Date": "01/01/2025"
        },
        ...
      ],
      "row_count": 15234,
      "detected_header_row": 2,
      "column_mapping": {
        "Procedure Code": "procedure_code",
        "Description": "description",
        "Rate": "reimbursement_rate",
        "Mod": "modifier",
        "Eff Date": "effective_date"
      },
      "mapping_confidence": 0.95
    }
  ],
  "file_size_bytes": 2457600,
  "file_metadata": {
    "effective_date": "2025-01-01",
    "fiscal_year": "FY2025"
  },
  "schema_drift_detected": false,
  "data_quality_issues": [
    "Row 1234: Missing procedure code",
    "Row 5678: Invalid rate ($0.00)"
  ],
  "total_rows_extracted": 15234,
  "processing_time_seconds": 12.4,
  "errors": []
}
```

---

## How It Fits in the Pipeline

```
┌────────────┐     ┌────────────┐     ┌────────────┐
│  Navigator  │ ──► │  Extractor  │ ──► │  Archivist  │
│             │     │ (this agent)│     │             │
│ Finds URLs  │     │ Downloads + │     │ Loads to DB │
│             │     │ Parses +    │     │ (Bronze /   │
│             │     │ Maps schema │     │  Silver /   │
│             │     │             │     │  Gold)      │
└────────────┘     └────────────┘     └────────────┘
```

The Extractor's `ExtractorOutput.extracted_tables` becomes the input for the Archivist Agent.

---

## Integration with Navigator

```python
# Complete pipeline: Navigator → Extractor
from agents.navigator import run_navigator, NavigatorInput
from agents.extractor import run_extractor, ExtractorInput

# Step 1: Discover datasets
nav_input = NavigatorInput(
    portal_url="https://extranet-sp.dhss.alaska.gov/.../FeeSchedule.html",
    state_name="Alaska",
    state_code="AK",
)
nav_result = run_navigator(nav_input)

# Step 2: Extract top dataset
if nav_result.relevant_datasets:
    top_dataset = nav_result.relevant_datasets[0]

    ext_input = ExtractorInput(
        url=top_dataset.url,
        state_name=nav_result.state_name,
        state_code=nav_result.state_code,
        file_type=top_dataset.file_type,
        category=top_dataset.category,
        title=top_dataset.title,
    )

    ext_result = run_extractor(ext_input)

    print(f"✅ Extracted {ext_result.total_rows_extracted} rows")
    print(f"📊 Tables: {len(ext_result.extracted_tables)}")
    print(f"⚠️  Quality issues: {len(ext_result.data_quality_issues)}")
```

---

## Environment Variables

Same as Navigator:

| Variable        | Required | Default                                 | Description                  |
| --------------- | -------- | --------------------------------------- | ---------------------------- |
| `AWS_REGION`    | Yes      | `eu-north-1`                            | AWS region for Bedrock       |
| AWS credentials | Yes      | (from `~/.aws/credentials` or IAM role) | Access to Bedrock Claude 3.5 |

---

## Dependencies

Add to `requirements.txt`:

```txt
# Extractor-specific dependencies
pandas>=2.2.0,<3.0
openpyxl>=3.1.0,<4.0       # Excel support
pdfplumber>=0.10.0,<1.0    # PDF table extraction
requests>=2.31.0,<3.0      # HTTP downloads
```

# Extractor Agent - Implementation Complete ✅

## Summary

The Extractor Agent has been fully implemented and integrated with the Navigator Agent. It downloads, parses, and analyzes Medicaid fee schedule files.

---

## 📁 Files Created

```
new/agents/extractor/
├── __init__.py              # Package exports
├── agent.py                 # Main Strands agent (400+ lines)
├── models.py                # Pydantic models (250+ lines)
├── run.py                   # CLI runner with Navigator integration
├── README.md                # Complete documentation
└── tools/
    ├── __init__.py
    ├── download_file.py     # HTTP download with retry (120 lines)
    ├── parse_file.py        # Multi-format parser (350 lines)
    └── map_columns.py       # LLM column mapping (100 lines)

new/
└── pipeline_example.py      # Complete Navigator→Extractor pipeline
```

---

## 🚀 Quick Start

### Option 1: Complete Pipeline (Recommended)

```bash
cd /Users/sudipkunwar/Medicaid_Feeschedule/new

# Run complete pipeline for Alaska
python pipeline_example.py alaska

# Process top 3 datasets from Florida
python pipeline_example.py florida --top 3
```

### Option 2: Extractor Only (from Navigator output)

```bash
# First run Navigator
python -m agents.navigator.run alaska

# Then run Extractor on Navigator results
python -m agents.extractor.run --navigator-output output/alaska_navigator_result.json
```

### Option 3: Extractor Standalone (single URL)

```bash
python -m agents.extractor.run \
  --url "https://example.com/physician_schedule.xlsx" \
  --state "Alaska" \
  --state-code "AK" \
  --file-type xlsx \
  --category physician
```

---

## 🔧 What the Extractor Does

### Phase 1: Extract (Download & Parse)

1. **Download File**
   - HTTP download with streaming (up to 500MB)
   - Retry logic with exponential backoff
   - Content-type validation

2. **Parse File**
   - **Excel** (.xlsx, .xls): Multi-sheet extraction, header detection
   - **CSV**: Auto delimiter/encoding detection
   - **PDF**: Table extraction via pdfplumber
   - **ZIP**: Recursive extraction and processing

### Phase 2: Analyze (Map & Validate)

3. **Map Columns**
   - LLM analyzes raw columns vs canonical schema
   - Maps: `Procedure Code` → `procedure_code`
   - Confidence scoring (0.0-1.0)

4. **Validate Quality**
   - Missing procedure codes
   - Invalid rates ($0.00, negative)
   - Duplicate codes
   - Schema drift detection

---

## 📊 Example Output

```json
{
  "success": true,
  "source_url": "https://.../physician_fee_schedule_2025.xlsx",
  "state_name": "Alaska",
  "file_type": "xlsx",
  "extracted_tables": [
    {
      "sheet_name": "Professional Services",
      "headers": ["Procedure Code", "Description", "Rate"],
      "row_count": 15234,
      "column_mapping": {
        "Procedure Code": "procedure_code",
        "Description": "description",
        "Rate": "reimbursement_rate"
      },
      "mapping_confidence": 0.95
    }
  ],
  "total_rows_extracted": 15234,
  "data_quality_issues": ["Row 1234: Missing procedure code"]
}
```

---

## 🔗 Integration with Navigator

The Extractor automatically reads Navigator output:

```python
# Navigator discovers URLs
navigator_result = run_navigator(NavigatorInput(...))

# Extractor processes top dataset
dataset = navigator_result.relevant_datasets[0]
extractor_result = run_extractor(ExtractorInput(
    url=dataset.url,
    state_name=navigator_result.state_name,
    file_type=dataset.file_type,
    category=dataset.category,
))

# Result: Structured, mapped, validated data
print(f"Extracted {extractor_result.total_rows_extracted:,} rows")
```

---

## 📋 Canonical Schema

All data is mapped to these standard fields:

| Field                | Required | Example                               |
| -------------------- | -------- | ------------------------------------- |
| `procedure_code`     | ✅       | `"99213"`                             |
| `description`        | ✅       | `"Office visit, established patient"` |
| `reimbursement_rate` | ✅       | `75.50`                               |
| `modifier`           | ❌       | `"26"`                                |
| `effective_date`     | ❌       | `"2025-01-01"`                        |
| `end_date`           | ❌       | `"2025-12-31"`                        |

---

## 🛠️ Tools

### `download_file`

- Max size: 500 MB
- Timeout: 60s
- Retry: 3 attempts
- Streaming: Yes

### `parse_file`

- Excel: Multi-sheet, merged cells, header detection
- CSV: Auto delimiter (`,`, `\t`, `|`, `;`)
- PDF: Table extraction (pdfplumber)
- ZIP: Recursive processing

### `map_columns`

- LLM-powered column mapping
- Sample data analysis
- Confidence scoring

---

## ⚙️ Environment Variables

Same as Navigator:

```bash
AWS_REGION=eu-north-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
BEDROCK_MODEL=eu.anthropic.claude-3-5-sonnet-20241022-v2:0
```

---

## 📦 Dependencies

Add to `requirements.txt`:

```txt
# Core
strands-agents>=1.0.0
pydantic>=2.5.0
boto3>=1.34.0

# Web crawling (Navigator)
playwright>=1.40.0

# Data processing (Extractor)
pandas>=2.2.0
openpyxl>=3.1.0      # Excel
pdfplumber>=0.10.0   # PDF
requests>=2.31.0     # Downloads
```

---

## 🎯 Next Steps

1. ✅ **Navigator Agent** - Complete
2. ✅ **Extractor Agent** - Complete
3. ⏳ **Archivist Agent** - TODO
   - Load data to PostgreSQL (Bronze/Silver/Gold tables)
   - SCD Type 2 versioning
   - Deduplication

---

## 🔄 Pipeline Flow

```
User Input
   ↓
┌─────────────┐
│  Navigator  │ ← Crawls portal, finds URLs
└──────┬──────┘
       ↓ (URLs with scores)
┌─────────────┐
│  Extractor  │ ← Downloads, parses, maps columns
└──────┬──────┘
       ↓ (Structured tables)
┌─────────────┐
│  Archivist  │ ← Loads to database (TODO)
└─────────────┘
```

---

## 📝 Usage Examples

### Complete Pipeline

```bash
# Alaska (top 1 dataset)
python pipeline_example.py alaska

# Florida (top 3 datasets)
python pipeline_example.py florida --top 3

# California (deeper crawl)
python pipeline_example.py california --max-depth 3 --top 2
```

### Navigator Only

```bash
python -m agents.navigator.run alaska
```

### Extractor Only

```bash
python -m agents.extractor.run --navigator-output output/alaska_navigator_result.json
```

---

## 🎉 Status

- ✅ Navigator Agent: **COMPLETE**
- ✅ Extractor Agent: **COMPLETE**
- ✅ Integration: **COMPLETE**
- ✅ Documentation: **COMPLETE**
- ✅ CLI Tools: **COMPLETE**
- ⏳ Archivist Agent: **TODO**

Ready for testing and deployment!

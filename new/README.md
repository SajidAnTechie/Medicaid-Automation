# Sentinel-State MVP

Simplified Medicaid fee schedule ingestion pipeline.

```
Hardcoded URLs → Extractor Agent (LLM) → Analyst Agent (LLM) → JSON Schema Validation → output.json
```

## Project structure

```
sentinel_state_mvp/
├── config/
│   └── urls.py          # hardcoded URL registry — edit this to add states
├── agents/
│   ├── extractor.py     # downloads + extracts raw records via LLM
│   └── analyst.py       # maps raw columns to canonical schema via LLM
├── schema/
│   └── models.py        # Pydantic MedicaidRate model + schema description
├── validator.py         # validates mapped records, splits valid / rejected
├── pipeline.py          # main runner
├── requirements.txt
└── output/              # created automatically on first run
    ├── output.json      # valid records
    └── rejections.json  # invalid records with error reason
```

## Prerequisites

- Python 3.11+
- AWS credentials configured with `bedrock:InvokeModel` permission
- Model access enabled for `anthropic.claude-3-5-sonnet-20241022-v2:0` in Amazon Bedrock

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Process all URLs in config/urls.py
python pipeline.py

# Process only one state
python pipeline.py --state FL

# Preview URLs without downloading
python pipeline.py --dry-run
```

## Adding a new state

Edit `config/urls.py` and add an entry to `URL_REGISTRY`:

```python
{
    "state_code": "CA",
    "dataset_type": "dental",
    "url": "https://...",
    "file_type": "excel",   # "excel" | "pdf" | "csv"
},
```

## Output format

### output.json

```json
[
  {
    "state_code": "FL",
    "dataset_type": "dental",
    "procedure_code": "D0120",
    "modifier": null,
    "fee_amount": 24.50,
    "effective_date": "2024-01-01",
    "end_date": null
  }
]
```

### rejections.json

```json
[
  {
    "row_index": 42,
    "record": { "state_code": "FL", "procedure_code": "INVALID", ... },
    "error": "Invalid procedure code format: 'INVALID'"
  }
]
```

## Canonical schema fields

| Field | Type | Description |
|---|---|---|
| `state_code` | str | Two-letter state code |
| `dataset_type` | str | Fee schedule category |
| `procedure_code` | str | HCPCS or CPT code |
| `modifier` | str \| null | Optional procedure modifier |
| `fee_amount` | float | Reimbursement rate |
| `effective_date` | date \| null | Rate effective date |
| `end_date` | date \| null | Rate end date |

## AWS configuration

The pipeline uses Amazon Bedrock in `us-east-1` by default.
To change the region, edit the `bedrock` client in `agents/extractor.py` and `agents/analyst.py`:

```python
bedrock = boto3.client("bedrock-runtime", region_name="us-west-2")
```

## Scaling up

When ready to graduate from MVP to the full architecture, these are the addition points:

| Feature | Where to add |
|---|---|
| Auto URL discovery | Replace `config/urls.py` with Navigator agent |
| HITL review | Add confidence scoring + SNS alert in `agents/analyst.py` |
| Workflow orchestration | Wrap `pipeline.py` in LangGraph `StateGraph` |
| S3 persistence | Add Bronze/Silver writes in a new `agents/archivist.py` |
| Aurora / database | Replace JSON file output with Aurora upsert in archivist |
| Scheduling | Replace manual run with EventBridge trigger |

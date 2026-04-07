# Analysis Agent

The **Analysis Agent** processes raw CSV data and metadata JSON to create canonical header mappings aligned with the Medicaid fee schedule schema.

## Purpose

Receives:

- CSV URL from csv_exporter agent
- Metadata JSON from extractor agent

Produces:

- Canonical header list with confidence levels
- Mapping of raw headers to schema-compliant field names
- Analysis notes and unmapped headers

## Architecture

```
┌─────────────────┐
│  CSV Exporter   │──┐
│     Agent       │  │
└─────────────────┘  │
                     ├──► ┌─────────────────┐      ┌──────────────────┐
┌─────────────────┐  │    │  Analysis Agent │─────►│ Canonical Headers│
│   Extractor     │──┘    │                 │      │   + Confidence   │
│     Agent       │        └─────────────────┘      └──────────────────┘
└─────────────────┘
```

## Usage

### Standalone Mode

```bash
# Basic usage
python run_analysis_agent.py output/alaska_raw_data_1.csv output/alaska_extractor_result_0.json

# With custom output path
python run_analysis_agent.py \
  output/florida_raw_data_1.csv \
  output/florida_extractor_result_0.json \
  --output my_analysis.json

# With debug logging
python run_analysis_agent.py \
  output/alaska_raw_data_1.csv \
  output/alaska_extractor_result_0.json \
  --debug
```

### In Pipeline

```python
from agents.analysis import agent as analysis_agent

analysis_result = analysis_agent.run(
    csv_url="file:///path/to/data.csv",
    metadata_json=extractor_output_dict,
)

print(f"Mapped headers: {len(analysis_result['canonical_headers'])}")
print(f"Unmapped: {len(analysis_result['unmapped_headers'])}")
```

## Output Format

```json
{
  "canonical_headers": [
    {
      "raw_header": "Proc Code",
      "canonical_name": "procedure_code",
      "confidence": "high",
      "notes": "Clear match to procedure code field"
    },
    {
      "raw_header": "Rate",
      "canonical_name": "rate_amount",
      "confidence": "medium",
      "notes": "Could be rate_amount or fee_amount"
    }
  ],
  "unmapped_headers": ["Internal_ID", "LastModified"],
  "analysis_notes": "Dataset contains standard procedure codes with rates..."
}
```

## Confidence Levels

- **high** ✅ - Clear, unambiguous mapping
- **medium** ⚠️ - Reasonable mapping with some ambiguity
- **low** ❓ - Uncertain mapping, may need review

## Examples

### Example 1: Alaska Fee Schedule

```bash
python run_analysis_agent.py \
  output/alaska_raw_data_1.csv \
  output/alaska_extractor_result_0.json
```

Output:

```
================================================================================
🔍 ANALYSIS AGENT - Canonical Header Mapping
================================================================================

CSV File: output/alaska_raw_data_1.csv
Metadata JSON: output/alaska_extractor_result_0.json
Output: output/alaska_raw_data_1_analysis_result.json

--------------------------------------------------------------------------------

Running analysis...

================================================================================
✅ ANALYSIS COMPLETE
================================================================================

Canonical headers mapped: 8
Unmapped headers: 2

📋 Header Mappings:
--------------------------------------------------------------------------------
✅ Procedure Code              -> procedure_code
✅ Description                 -> procedure_description
✅ Fee Amount                  -> rate_amount
⚠️ Modifier                    -> modifier
   Note: Could be modifier or modifier_code

❌ Unmapped Headers:
--------------------------------------------------------------------------------
   • Internal_ID
   • Last_Updated

📝 Analysis Notes:
--------------------------------------------------------------------------------
   Standard dental fee schedule with procedure codes and rates...

Results saved to: output/alaska_raw_data_1_analysis_result.json
```

### Example 2: With Custom Output

```bash
python run_analysis_agent.py \
  data/florida_dental.csv \
  metadata/florida_metadata.json \
  -o results/florida_analysis.json
```

## Configuration

The agent uses Claude 3.5 Sonnet via AWS Bedrock:

```python
MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"
MAX_TOKENS = 4096
TEMPERATURE = 0.0  # Deterministic for consistent mapping
```

## Error Handling

- Missing files: Validates CSV and metadata JSON exist
- Invalid JSON: Handles malformed metadata gracefully
- Network errors: Reports CSV fetch failures
- LLM errors: Catches and logs Bedrock API errors

## Integration with Pipeline

The analysis agent runs automatically after CSV export in the main pipeline:

```bash
python pipeline_example.py alaska
```

This will:

1. Navigate and discover datasets
2. Extract data to tables
3. Export to CSV
4. **Run analysis to create canonical mappings** ← Analysis Agent
5. Save results to `{state}_analysis_result_{n}.json`

## Development

To modify the system prompt or analysis logic, edit:

- `agents/analysis/agent.py` - Main agent logic
- System prompt in `SYSTEM_PROMPT` constant

## Next Steps

After analysis, the canonical header mappings are used by:

- **Analyst Agent** - Applies mappings to transform data
- **Validator Agent** - Validates transformed data
- **Archivist Agent** - Loads data to database with canonical schema

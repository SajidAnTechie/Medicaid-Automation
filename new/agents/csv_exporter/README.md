# CSV Exporter Agent 📊

The CSV Exporter Agent is a specialized agent that exports raw Medicaid fee schedule data to CSV format with proper column mappings.

## Purpose

Takes extraction results from the Extractor Agent and:

1. Downloads or uses cached source files
2. Reads original data (Excel/CSV)
3. Maps columns to canonical schema
4. Adds metadata columns
5. Exports complete dataset as CSV

## Architecture

```
csv_exporter/
├── __init__.py          # Package exports
├── agent.py             # Main CSV Exporter agent
├── models.py            # Pydantic models
├── README.md            # This file
└── tools/
    ├── __init__.py
    ├── download.py      # Download or cache file
    ├── reader.py        # Read and map data
    └── writer.py        # Export to CSV
```

## Usage

### Integrated with Extractor

```python
from agents.extractor import run_extractor
from agents.csv_exporter import run_csv_exporter, CSVExporterInput

# Run extractor first
extractor_result = run_extractor(...)

# Export raw CSV
if extractor_result.success:
    table = extractor_result.extracted_tables[0]

    csv_input = CSVExporterInput(
        source_url=extractor_result.source_url,
        file_type=extractor_result.file_type,
        sheet_name=table.sheet_name,
        column_mapping=table.column_mapping,
        state_name=extractor_result.state_name,
        state_code=extractor_result.state_code,
        category=extractor_result.category,
        download_timestamp=extractor_result.download_timestamp,
        output_path="output/alaska_raw_data.csv"
    )

    csv_result = run_csv_exporter(csv_input)
    print(f"Exported {csv_result.rows_exported} rows")
```

### Standalone

```python
from agents.csv_exporter import run_csv_exporter, CSVExporterInput

csv_input = CSVExporterInput(
    source_url="https://example.com/fees.xlsx",
    file_type="xlsx",
    sheet_name="Physician Fee Schedule",
    column_mapping={
        "Proc Code": "procedure_code",
        "Description": "procedure_description",
        "Rate": "maximum_allowable"
    },
    state_name="Alaska",
    state_code="AK",
    category="physician",
    download_timestamp="2025-01-10T00:00:00Z",
    output_path="output/alaska_raw_data.csv"
)

result = run_csv_exporter(csv_input)
```

## Output Format

The exported CSV includes:

### Metadata Columns

- `state_name` - State name
- `state_code` - State code (AK, CA, etc.)
- `source_url` - Original file URL
- `category` - Category (physician, dental, etc.)
- `extraction_date` - When data was extracted

### Data Columns (mapped)

- `procedure_code` - CPT/HCPCS code
- `procedure_modifier` - Modifier code
- `procedure_description` - Service description
- `maximum_allowable` - Reimbursement rate
- `requires_*` - Various requirement flags
- `billing_notes` - Additional notes
- etc.

## Data Preservation Policy

The CSV Exporter follows a strict **no data loss** policy:

1. ✅ **All Rows**: Every row from source file is included
2. ✅ **All Columns**: Every column from source file is included
3. ✅ **Original Names**: Column names are preserved exactly as they appear
4. ✅ **Original Values**: Cell values are not modified or transformed
5. ✅ **Metadata**: 5 additional columns added at the beginning for tracking

This allows downstream processes (like the Archivist) to:

- Apply their own column mappings
- Perform data quality checks
- Transform data as needed
- Analyze unmapped columns
- Track data lineage

## Example Output

**Input File** (Alaska Physician Fee Schedule.xlsx):

```
Proc Code | Proc Mod | OMK | Maximum Allowable | Billing Notes
99213     |          | ... | 75.50             | ...
99214     |          | ... | 110.00            | ...
```

**Output CSV** (alaska_raw_data.csv):

```csv
state_name,state_code,source_url,category,extraction_date,Proc Code,Proc Mod,OMK,Maximum Allowable,Billing Notes
Alaska,AK,https://...,physician,2025-01-10T00:00:00Z,99213,,,...,75.50,...
Alaska,AK,https://...,physician,2025-01-10T00:00:00Z,99214,,,...,110.00,...
```

Notice:

- All 5 metadata columns added at the beginning
- All original columns preserved with exact names
- All rows included
- No data transformations

## Integration Flow

```
Navigator Agent
    ↓ (finds URLs)
Extractor Agent
    ↓ (parses & maps)
CSV Exporter Agent
    ↓ (exports raw CSV)
CSV File with ALL data
```

## Benefits

1. **Automated**: Integrates seamlessly with Extractor
2. **Complete Data**: Exports ALL rows (not just samples)
3. **Consistent Schema**: Uses canonical column names
4. **Metadata Rich**: Includes provenance information
5. **Agent-Based**: Uses LLM for intelligent error handling

## Tools

- `download_or_cache_file`: Downloads or uses cached files
- `read_and_map_data`: Reads Excel/CSV and maps columns
- `export_to_csv`: Exports DataFrame to CSV

## Next Steps

The raw CSV data can be:

- Loaded into databases (Archivist Agent)
- Analyzed with data tools
- Shared with stakeholders
- Version controlled

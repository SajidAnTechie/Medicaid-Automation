"""
map_columns tool
----------------
Structures table data for LLM-based column mapping to canonical schema.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from strands import tool

logger = logging.getLogger(__name__)


@tool
def map_columns(
    headers: list[str],
    sample_rows: list[dict[str, Any]],
    category: str = "unknown",
) -> str:
    """
    Structure a table's headers and sample data into a prompt for LLM column mapping.
    
    The LLM will analyze the headers and sample data to map raw columns to the
    canonical Medicaid fee schedule schema.
    
    Args:
        headers: List of column headers from the extracted table
        sample_rows: First 5-10 rows of data for context (as list of dicts)
        category: Expected dataset category (physician, dental, pharmacy, etc.)
    
    Returns:
        A structured prompt string for the LLM to analyze and map columns
    """
    
    prompt = f"""
## Column Mapping Task

You have extracted a table from a {category} Medicaid fee schedule file.
Your task is to map the raw column headers to the canonical schema fields.

### Raw Column Headers ({len(headers)} columns)
{json.dumps(headers, indent=2)}

### Sample Data (first {len(sample_rows)} rows)
{json.dumps(sample_rows[:5], indent=2)}

### Canonical Schema Fields
The target schema has these fields (map as many as possible):

**Required fields:**
- `procedure_code` — CPT, HCPCS, CDT, or NDC code
- `description` — Procedure/service description  
- `reimbursement_rate` — Reimbursement amount in dollars

**Optional fields:**
- `modifier` — Procedure modifier code
- `effective_date` — Date when rate became effective
- `end_date` — Date when rate expires/expired
- `unit_type` — Unit of service (per procedure, per day, etc.)
- `place_of_service` — Where service is provided (office, hospital, etc.)
- `provider_type` — Type of provider eligible for this rate
- `notes` — Additional notes or restrictions

### Mapping Rules

**Procedure Code Detection:**
Look for columns with names like:
- "CPT Code", "HCPCS", "Procedure Code", "Code", "Service Code"
- "CDT Code" (dental)
- "NDC" (pharmacy)

Raw header variations → `procedure_code`:
- "CPT Code", "CPT", "HCPCS", "Procedure Code", "Code", "Service Code"
- "Proc Code", "Proc", "ProcCode"

**Rate Detection:**
Look for columns like:
- "Rate", "Fee", "Amount", "Reimbursement", "Allowable", "Max Fee"
- "Medicaid Rate", "Provider Fee", "Allowable Amount"

Raw header variations → `reimbursement_rate`:
- "Rate", "Fee", "Amount", "Allowable", "Max Fee", "Reimbursement"
- "Medicaid Rate", "Fee Amount", "Allowable Fee"

Rates should be numeric (strip "$" and "," if present).

**Description Detection:**
Look for:
- "Description", "Service Description", "Procedure Description"
- "Long Description", "Service", "Proc Description"

Raw header variations → `description`:
- "Description", "Desc", "Service Description", "Procedure Description"
- "Service", "Long Description"

**Modifier Detection:**
Raw header variations → `modifier`:
- "Mod", "Modifier", "Modifiers", "Proc Modifier"

**Date Detection:**
Look for:
- Effective Date: "Effective Date", "Begin Date", "Start Date", "From", "Eff Date"
- End Date: "End Date", "Through", "Expiration", "Exp Date"

Raw header variations → `effective_date`:
- "Effective Date", "Eff Date", "Begin Date", "Start Date", "From"

Raw header variations → `end_date`:
- "End Date", "Through", "Expiration", "Exp Date", "Thru"

**Other Optional Fields:**
- `unit_type`: "Unit", "Unit Type", "UOM"
- `place_of_service`: "POS", "Place of Service", "Location"
- `provider_type`: "Provider Type", "Provider", "Specialty"
- `notes`: "Notes", "Comments", "Remarks", "Special Instructions"

### Your Response Format

Return a JSON object with your column mapping:

{{
  "column_mapping": {{
    "Raw Column Name 1": "canonical_field_name_1",
    "Raw Column Name 2": "canonical_field_name_2",
    ...
  }},
  "mapping_confidence": 0.95,
  "mapping_notes": "Brief explanation of your mapping decisions and any ambiguities"
}}

**Example:**
{{
  "column_mapping": {{
    "CPT Code": "procedure_code",
    "Service Description": "description",
    "Allowable Fee": "reimbursement_rate",
    "Mod": "modifier",
    "Eff Date": "effective_date"
  }},
  "mapping_confidence": 0.92,
  "mapping_notes": "High confidence mapping. 'Mod' appears to be modifier based on sample values. Some columns like 'Notes' don't clearly map to canonical schema."
}}

**CRITICAL**: 
- Extract the EXACT raw headers from the file shown above
- Map them to the canonical field names using the rules
- Return ONLY valid JSON (no markdown, no code blocks)

Begin your analysis now.
"""
    
    return prompt

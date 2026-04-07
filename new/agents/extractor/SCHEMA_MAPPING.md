# Canonical Schema Mapping Reference

This document defines the canonical schema for Medicaid fee schedule data and provides mapping rules for converting raw column headers from state files.

---

## Canonical Schema Fields

### Required Fields

| Field                | Type    | Description                   | Example Values                        |
| -------------------- | ------- | ----------------------------- | ------------------------------------- |
| `procedure_code`     | `str`   | CPT, HCPCS, CDT, or NDC code  | `"99213"`, `"J1234"`, `"D0120"`       |
| `description`        | `str`   | Procedure/service description | `"Office visit, established patient"` |
| `reimbursement_rate` | `float` | Reimbursement amount in USD   | `75.50`, `120.00`                     |

### Optional Fields

| Field              | Type          | Description                | Example Values                 |
| ------------------ | ------------- | -------------------------- | ------------------------------ |
| `modifier`         | `str \| None` | Procedure modifier code    | `"26"`, `"TC"`, `"59"`         |
| `effective_date`   | `str \| None` | Date rate became effective | `"2025-01-01"`                 |
| `end_date`         | `str \| None` | Date rate expires          | `"2025-12-31"`                 |
| `unit_type`        | `str \| None` | Unit of service            | `"per procedure"`, `"per day"` |
| `place_of_service` | `str \| None` | Service location           | `"office"`, `"hospital"`       |
| `provider_type`    | `str \| None` | Provider type              | `"physician"`, `"dentist"`     |
| `notes`            | `str \| None` | Additional notes           | `"Prior auth required"`        |

---

## Column Mapping Rules

### Procedure Code Mapping

**Canonical Field:** `procedure_code`

**Raw Header Variations:**

```
✅ Maps to procedure_code:
- "CPT Code", "CPT", "HCPCS", "HCPCS Code"
- "Procedure Code", "Proc Code", "ProcCode", "Proc"
- "Code", "Service Code", "Svc Code"
- "CDT Code", "CDT" (dental)
- "NDC", "NDC Code" (pharmacy)
```

**Code Patterns:**

- CPT/HCPCS: 5 digits or letter+4 digits (e.g., `99213`, `J1234`)
- CDT: "D" + 4 digits (e.g., `D0120`)
- NDC: 11 digits with dashes (e.g., `12345-6789-01`)

---

### Description Mapping

**Canonical Field:** `description`

**Raw Header Variations:**

```
✅ Maps to description:
- "Description", "Desc"
- "Service Description", "Svc Description", "Service Desc"
- "Procedure Description", "Proc Description"
- "Long Description", "Long Desc"
- "Service", "Procedure"
```

---

### Reimbursement Rate Mapping

**Canonical Field:** `reimbursement_rate`

**Raw Header Variations:**

```
✅ Maps to reimbursement_rate:
- "Rate", "Fee", "Amount"
- "Reimbursement", "Reimbursement Rate", "Reimb Rate"
- "Allowable", "Allowable Fee", "Allowable Amount"
- "Max Fee", "Maximum Fee", "Max Allowable"
- "Medicaid Rate", "Medicaid Fee"
- "Provider Fee", "Provider Rate"
- "Fee Amount", "Rate Amount"
```

**Data Cleaning:**

- Strip "$" symbol: `"$75.50"` → `75.50`
- Strip commas: `"1,250.00"` → `1250.00`
- Handle decimals: `"75.50"` → `75.50`

---

### Modifier Mapping

**Canonical Field:** `modifier`

**Raw Header Variations:**

```
✅ Maps to modifier:
- "Mod", "Modifier", "Modifiers"
- "Proc Modifier", "Procedure Modifier"
- "Mod Code", "Modifier Code"
```

---

### Effective Date Mapping

**Canonical Field:** `effective_date`

**Raw Header Variations:**

```
✅ Maps to effective_date:
- "Effective Date", "Eff Date", "Eff. Date"
- "Begin Date", "Start Date", "From"
- "Effective", "Begin", "Start"
```

**Date Formats to Parse:**

- `MM/DD/YYYY` → `YYYY-MM-DD`
- `MM-DD-YYYY` → `YYYY-MM-DD`
- `YYYY-MM-DD` (keep as-is)
- `"January 1, 2025"` → `2025-01-01`

---

### End Date Mapping

**Canonical Field:** `end_date`

**Raw Header Variations:**

```
✅ Maps to end_date:
- "End Date", "Expiration", "Expiration Date"
- "Through", "Thru", "To"
- "Exp Date", "Exp. Date"
```

---

### Unit Type Mapping

**Canonical Field:** `unit_type`

**Raw Header Variations:**

```
✅ Maps to unit_type:
- "Unit", "Unit Type", "Units"
- "UOM", "Unit of Measure"
- "Service Unit", "Billing Unit"
```

---

### Place of Service Mapping

**Canonical Field:** `place_of_service`

**Raw Header Variations:**

```
✅ Maps to place_of_service:
- "POS", "Place of Service"
- "Location", "Service Location"
- "Setting", "Care Setting"
```

---

### Provider Type Mapping

**Canonical Field:** `provider_type`

**Raw Header Variations:**

```
✅ Maps to provider_type:
- "Provider Type", "Provider"
- "Specialty", "Provider Specialty"
- "Type", "Category"
```

---

### Notes Mapping

**Canonical Field:** `notes`

**Raw Header Variations:**

```
✅ Maps to notes:
- "Notes", "Note", "Comments"
- "Remarks", "Special Instructions"
- "Additional Info", "Information"
```

---

## Mapping Examples

### Example 1: Alaska Physician Fee Schedule

**Raw Headers:**

```
["CPT Code", "Service Description", "Allowable Fee", "Mod", "Eff Date"]
```

**Mapped:**

```json
{
  "CPT Code": "procedure_code",
  "Service Description": "description",
  "Allowable Fee": "reimbursement_rate",
  "Mod": "modifier",
  "Eff Date": "effective_date"
}
```

---

### Example 2: Florida Dental Rates

**Raw Headers:**

```
["CDT Code", "Procedure Description", "Medicaid Rate", "Begin Date", "End Date"]
```

**Mapped:**

```json
{
  "CDT Code": "procedure_code",
  "Procedure Description": "description",
  "Medicaid Rate": "reimbursement_rate",
  "Begin Date": "effective_date",
  "End Date": "end_date"
}
```

---

### Example 3: Texas Pharmacy Schedule

**Raw Headers:**

```
["NDC", "Drug Name", "Max Allowable", "Units", "Notes"]
```

**Mapped:**

```json
{
  "NDC": "procedure_code",
  "Drug Name": "description",
  "Max Allowable": "reimbursement_rate",
  "Units": "unit_type",
  "Notes": "notes"
}
```

---

## Data Quality Validation

After mapping, validate that:

### Required Fields

- ✅ `procedure_code` is not empty
- ✅ `description` has at least 3 characters
- ✅ `reimbursement_rate` > 0.00

### Optional Fields

- ⚠️ Flag if `effective_date` is missing
- ⚠️ Flag if date format is invalid

### Common Issues

- ❌ `procedure_code` is empty → Row invalid
- ❌ `reimbursement_rate` = $0.00 → Flag as issue
- ❌ Duplicate `(procedure_code + modifier)` → Flag as issue

---

## Implementation Notes

- The Extractor Agent system prompt enforces these mappings
- The `map_columns` tool structures the analysis for the LLM
- Confidence scores indicate mapping quality (0.0-1.0)
- Low confidence (<0.7) should trigger manual review

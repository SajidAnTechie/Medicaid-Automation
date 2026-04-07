# Navigator Agent — "The Researcher"

The Navigator Agent is the **entry point** of the Sentinel-State pipeline. It crawls U.S. state Medicaid portals using a headless browser, discovers all downloadable dataset links, and uses LLM reasoning to rank them by relevance to Medicaid fee schedules.

---

## Architecture

```
NavigatorInput (portal URL + state info)
        │
        ▼
┌──────────────────────────────┐
│     Navigator Agent          │
│     (Strands + Bedrock)      │
│                              │
│  1. crawl_portal   ──► Playwright headless browser
│     (discover links)         │
│                              │
│  2. filter_datasets ──► LLM relevance scoring
│     (rank results)           │
│                              │
└──────────────────────────────┘
        │
        ▼
NavigatorOutput (ranked datasets with scores)
        │
        ▼
  Downstream: Extractor Agent (downloads + parses files)
```

## File Structure

```
agents/navigator/
├── __init__.py               # Package exports
├── agent.py                  # Strands Agent definition + system prompt
├── models.py                 # Pydantic input/output schemas
├── run.py                    # CLI entry point
├── README.md                 # This file
└── tools/
    ├── __init__.py
    ├── crawl_portal.py       # Playwright-based portal crawling tool
    └── filter_datasets.py    # LLM relevance ranking prompt builder
```

---

## Prerequisites

```bash
pip install strands-agents strands-agents-tools playwright pydantic boto3
playwright install chromium
```

**AWS credentials** must be configured for Amazon Bedrock access (Claude 3.5 Sonnet).

---

## Quick Start

### CLI Runner (pre-configured portals)

```bash
# Run from the project root (new/)
cd new

# Default: Alaska
python -m agents.navigator.run

# Specify a state
python -m agents.navigator.run alaska
python -m agents.navigator.run florida
```

### Programmatic Usage

```python
from agents.navigator import run_navigator, NavigatorInput

input_data = NavigatorInput(
    portal_url="https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html",
    state_name="Alaska",
    state_code="AK",
    dataset_category="all",   # or "physician", "dental", "pharmacy", etc.
    max_depth=2,
    top_k=15,
)

result = run_navigator(input_data)

for ds in result.relevant_datasets:
    print(f"[{ds.relevance_score:.2f}] {ds.title} → {ds.url}")
```

---

## Input — `NavigatorInput`

| Field              | Type  | Default | Description                                                                |
| ------------------ | ----- | ------- | -------------------------------------------------------------------------- |
| `portal_url`       | `str` | —       | **Required.** Root URL of the state Medicaid fee schedule portal.          |
| `state_name`       | `str` | —       | **Required.** Full state name (e.g., `"Alaska"`, `"Florida"`).             |
| `state_code`       | `str` | `""`    | Two-letter state code (e.g., `"AK"`, `"FL"`).                              |
| `dataset_category` | `str` | `"all"` | Target category or `"all"`. See [Dataset Categories](#dataset-categories). |
| `max_depth`        | `int` | `3`     | Maximum crawl depth from the portal root URL.                              |
| `top_k`            | `int` | `15`    | Number of top relevant datasets to return.                                 |

---

## Output — `NavigatorOutput`

| Field                    | Type                  | Description                                                          |
| ------------------------ | --------------------- | -------------------------------------------------------------------- |
| `state_name`             | `str`                 | State name echoed from input.                                        |
| `state_code`             | `str`                 | State code echoed from input.                                        |
| `portal_url`             | `str`                 | Portal URL echoed from input.                                        |
| `total_links_discovered` | `int`                 | Total downloadable links found across all pages.                     |
| `relevant_datasets`      | `list[RankedDataset]` | Top-K datasets ranked by relevance (see below).                      |
| `crawled_pages`          | `list[str]`           | All page URLs visited during the crawl.                              |
| `portal_type`            | `str`                 | Detected portal type: `sharepoint`, `drupal`, `wordpress`, `custom`. |
| `errors`                 | `list[str]`           | Any errors encountered during crawling.                              |
| `crawl_timestamp`        | `str`                 | ISO-8601 UTC timestamp of when the crawl completed.                  |

### `RankedDataset` (each item in `relevant_datasets`)

| Field              | Type              | Description                                                     |
| ------------------ | ----------------- | --------------------------------------------------------------- |
| `url`              | `str`             | Direct download URL of the dataset.                             |
| `title`            | `str`             | Link text or filename.                                          |
| `file_type`        | `FileType`        | `"pdf"`, `"xls"`, `"xlsx"`, `"csv"`, `"zip"`, or `"unknown"`.   |
| `category`         | `DatasetCategory` | Detected fee schedule category (see below).                     |
| `relevance_score`  | `float`           | `0.0`–`1.0` — LLM-assigned relevance to Medicaid fee schedules. |
| `relevance_reason` | `str`             | One-sentence explanation of the score.                          |
| `page_source_url`  | `str`             | The page URL where this link was found.                         |
| `is_current`       | `bool`            | `true` if this appears to be the current/active version.        |
| `estimated_date`   | `str \| null`     | Estimated effective date if detectable from filename/context.   |

---

## Dataset Categories

The agent classifies each discovered dataset into one of these Medicaid service categories:

| Value               | Description                                                 |
| ------------------- | ----------------------------------------------------------- |
| `physician`         | Physician / professional services                           |
| `dental`            | Dental services (CDT codes)                                 |
| `pharmacy`          | Pharmacy / drug reimbursement (NDC codes)                   |
| `dmepos`            | Durable Medical Equipment, Prosthetics, Orthotics, Supplies |
| `outpatient`        | Outpatient facility rates                                   |
| `inpatient`         | Inpatient facility rates                                    |
| `behavioral_health` | Behavioral / mental health services                         |
| `laboratory`        | Laboratory services                                         |
| `vision`            | Vision / optical services                                   |
| `home_health`       | Home health / HCBS services                                 |
| `general`           | General or multi-category fee schedule                      |
| `unknown`           | Could not determine category                                |

---

## Example Output (JSON)

```json
{
  "state_name": "Alaska",
  "state_code": "AK",
  "portal_url": "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html",
  "total_links_discovered": 42,
  "portal_type": "sharepoint",
  "crawl_timestamp": "2026-03-30T12:34:56.789000",
  "crawled_pages": [
    "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html",
    "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/Rates.html"
  ],
  "relevant_datasets": [
    {
      "url": "https://extranet-sp.dhss.alaska.gov/.../physician_fee_schedule_2025.xlsx",
      "title": "Physician Fee Schedule FY2025",
      "file_type": "xlsx",
      "category": "physician",
      "relevance_score": 0.97,
      "relevance_reason": "Excel file titled 'Physician Fee Schedule' with current fiscal year indicator — high-confidence match.",
      "page_source_url": "https://extranet-sp.dhss.alaska.gov/.../FeeSchedule.html",
      "is_current": true,
      "estimated_date": "2025"
    },
    {
      "url": "https://extranet-sp.dhss.alaska.gov/.../dental_rates_2025.pdf",
      "title": "Dental Reimbursement Rates",
      "file_type": "pdf",
      "category": "dental",
      "relevance_score": 0.91,
      "relevance_reason": "PDF containing dental reimbursement rates with CDT code references in context.",
      "page_source_url": "https://extranet-sp.dhss.alaska.gov/.../FeeSchedule.html",
      "is_current": true,
      "estimated_date": "2025"
    }
  ],
  "errors": []
}
```

Output is saved to: `output/<state_key>_navigator_result.json`

---

## Tools

### `crawl_portal`

**Purpose:** Crawls a state Medicaid portal using Playwright (headless Chromium) and extracts all downloadable file links.

| Feature                  | Detail                                                    |
| ------------------------ | --------------------------------------------------------- |
| **JS rendering**         | Full Chromium — handles SharePoint, dynamic content       |
| **SharePoint support**   | Expands collapsed sections, detects document libraries    |
| **Metadata extraction**  | Section headers, surrounding context, last-modified dates |
| **Deduplication**        | URL-based dedup across all crawled pages                  |
| **Depth-bounded BFS**    | Follows only relevant Medicaid links up to `max_depth`    |
| **Supported file types** | `.pdf`, `.xls`, `.xlsx`, `.csv`, `.zip`                   |

### `filter_datasets`

**Purpose:** Structures the crawl results into a detailed analysis prompt and feeds it to the LLM for relevance ranking.

**Scoring criteria (in priority order):**

1. **Content match** — title/context mentions fee schedule, rates, reimbursement, HCPCS, CPT
2. **Category detection** — maps to one of 12 Medicaid service categories
3. **Recency** — prefers current/active files over archived/historical
4. **File type preference** — Excel/CSV (+0.05) > PDF (neutral) > ZIP (−0.05)
5. **Negative signals** — rejects provider manuals, forms, newsletters (score → 0.0)

---

## Navigator Agent Configuration

## File Type Policy

The Navigator agent is configured to **ONLY** discover and return flat files:

### ✅ Accepted File Types

- `.xlsx` (Excel 2007+)
- `.xls` (Excel 97-2003)
- `.csv` (Comma-Separated Values)

### ❌ Rejected File Types

- `.pdf` (too difficult to extract reliably)
- `.zip` (unknown contents, processing complexity)
- `.doc`, `.docx` (not structured data)
- All other file types

## Why Flat Files Only?

1. **Reliable Extraction**: Excel and CSV files have consistent, predictable structures
2. **No OCR Required**: Unlike PDFs, no optical character recognition needed
3. **Clean Data**: Tables are already structured in rows and columns
4. **Fast Processing**: Simple parsing with pandas/openpyxl
5. **No Hidden Content**: What you see is what you get

## What If No Flat Files Found?

If the Navigator finds NO Excel or CSV files:

- It returns an empty `relevant_datasets` array
- An error message is added: "No Excel or CSV files found"
- The pipeline should try a deeper crawl or different portal section

## Configuration

The flat-file-only policy is enforced at multiple levels:

1. **Crawl Tool**: Only extracts .xlsx, .xls, .csv links
2. **Filter Tool**: Rejects non-flat files immediately
3. **Agent Prompt**: Explicitly instructs to exclude PDFs/ZIPs
4. **Scoring**: Non-flat files get score of 0.0

This ensures NO PDF or ZIP files slip through.

---

## Pre-configured State Portals

| State   | Code | Portal URL                                                                               |
| ------- | ---- | ---------------------------------------------------------------------------------------- |
| Alaska  | AK   | `https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html` |
| Florida | FL   | `https://ahca.myflorida.com/medicaid/cost-reimbursement-and-auditing`                    |

To add a new state, edit the `STATE_PORTALS` dict in `run.py` or pass a `NavigatorInput` directly via the programmatic API.

---

## Environment Variables

| Variable        | Required | Default                                 | Description                  |
| --------------- | -------- | --------------------------------------- | ---------------------------- |
| `AWS_REGION`    | Yes      | `us-east-1`                             | AWS region for Bedrock       |
| AWS credentials | Yes      | (from `~/.aws/credentials` or IAM role) | Access to Bedrock Claude 3.5 |

---

## How It Fits in the Pipeline

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│  Navigator  │ ──► │  Extractor  │ ──► │  Analyst    │ ──► │  Archivist  │
│ (this agent)│     │ (download + │     │ (column     │     │ (Bronze /   │
│             │     │  parse)     │     │  mapping)   │     │  Silver /   │
│ Finds URLs  │     │ Raw DataFr. │     │ Canonical   │     │  Gold)      │
└────────────┘     └────────────┘     └────────────┘     └────────────┘
```

The Navigator's `relevant_datasets[].url` becomes the input for the Extractor Agent, which downloads and parses the actual file content.

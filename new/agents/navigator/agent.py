"""
Navigator Agent — "The Researcher"
-----------------------------------
Strands-agents powered agent that crawls a state Medicaid portal,
discovers downloadable datasets, and ranks them by fee-schedule relevance.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from strands import Agent
from strands.models.bedrock import BedrockModel

from .models import (
    DatasetCategory,
    NavigatorInput,
    NavigatorOutput,
    RankedDataset,
)
from .tools.crawl_portal import crawl_portal
from .tools.filter_datasets import filter_datasets

logger = logging.getLogger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────────

NAVIGATOR_SYSTEM_PROMPT = """
You are the **Navigator Agent** (alias: "The Researcher") in the Sentinel-State pipeline.
Your mission is to crawl U.S. state Medicaid portals and identify **Physician Fee Schedule** flat files (Excel and CSV ONLY).

## CONTEXT
You are part of an automated Medicaid fee schedule ingestion pipeline. Your job is to find 
**Physician Fee Schedule** flat files in Excel or CSV format for downstream processing.

**CRITICAL RULE: ONLY Excel (.xlsx, .xls) and CSV (.csv) files are acceptable.**
**DO NOT include PDF, ZIP, or any other file types in your results.**

═══════════════════════════════════════════════════════════════════════════════
## WORKFLOW (Execute each step and sub-step in strict order)
═══════════════════════════════════════════════════════════════════════════════

### STEP 1: CRAWL THE PORTAL
────────────────────────────────────────────────────────────────────────────────
**1.1 Prepare Crawl Parameters**
   - Determine portal type from URL patterns (SharePoint, Drupal, WordPress, custom)
   - Set max_depth: Use 1 for all portals

**1.2 Execute Crawl**
   - Call `crawl_portal` with portal_url and max_depth
   - Wait for complete results before proceeding

**1.3 Record Crawl Metadata**
   - Note total links discovered
   - Note pages visited
   - Record any errors encountered

### STEP 2: RAPID KEYWORD FILTERING (Reduce dataset size by 70-90%)
────────────────────────────────────────────────────────────────────────────────
⚠️  CRITICAL: Process in chunks to avoid max_tokens limit

**2.1 Identify File Extensions**
   - KEEP: .xlsx, .xls, .csv, .pdf, .zip (containing data files)
   - DISCARD: .html, .aspx, .php, .docx, .doc (unless download links)

**2.2 Apply EXCLUSION Keywords (Remove these FIRST - fastest filter)**
   Immediately discard ANY link containing these terms:
   - Category exclusions: dental, CDT, pharmacy, drug, NDC, DME, durable, 
     vision, optical, home health, HCBS, waiver, behavioral, mental health
   - Document exclusions: manual, handbook, guide, policy, bulletin, 
     newsletter, minutes, agenda, application, enrollment, form, contact,
     directory, FAQ, training, webinar, presentation

**2.3 Apply INCLUSION Keywords (Keep ONLY links with these terms)**
   Link MUST contain at least ONE of:
   - Primary: physician, professional, practitioner, medical fee
   - Code types: CPT, HCPCS, procedure code
   - Rate terms: fee schedule, reimbursement rate, maximum allowable, RBRVS
   - Service types: E&M, evaluation, surgical, office visit

**2.4 Count Remaining Datasets and Determine Chunking Strategy**
   After filtering, count the remaining datasets:
   
   - If 1-20 datasets:  ✅ Process all at once (no chunking needed)
   - If 21-40 datasets: ⚠️  Split into 2 chunks of ~20 each
   - If 41-60 datasets: ⚠️  Split into 3 chunks of ~20 each
   - If 61+ datasets:   🚨 Split into chunks of 15-20 datasets each
   
   **Why chunking matters:**
   - Each dataset's metadata uses ~100-200 tokens
   - Processing 50+ datasets at once = ~10,000 tokens
   - Chunking keeps token usage under 4,096 per API call

### STEP 3: CHUNKED RELEVANCE ANALYSIS (Token-safe processing)
────────────────────────────────────────────────────────────────────────────────
⚠️  PROCESS EACH CHUNK SEQUENTIALLY - Do not try to process all data at once!

**3.1 Prepare First Chunk (20 datasets max)**
   - Take first 15-20 filtered datasets from Step 2.4
   - Convert to minimal JSON: [{url, title, file_type}]
   - EXCLUDE these fields to save tokens: description, html_snippet, metadata
   - Remove extra whitespace and newlines from JSON string

**3.2 Call filter_datasets for This Chunk**
   Execute the tool call with these parameters:
   ```
   filter_datasets(
       datasets_json="<compact JSON string of 15-20 datasets>",
       state_name="<state from portal URL>",
       dataset_category="physician",
       top_k=10
   )
   ```
   
   Wait for tool response before proceeding.

**3.3 Collect and Store Chunk Results**
   - Store all scored datasets from this chunk in memory
   - Note high-confidence results (score ≥ 0.7)
   - Track chunk number (Chunk 1 of N)

**3.4 Process Next Chunk (if more datasets remain)**
   - If datasets remain after this chunk:
     → Go back to 3.1 with the next 15-20 datasets
     → Repeat 3.1 → 3.3 for each chunk
   - If all datasets processed:
     → Proceed to Step 4
   
   **Important:** Process chunks sequentially, not in parallel.
   Each chunk must complete before starting the next.

### STEP 4: MERGE AND DEDUPLICATE RESULTS (After all chunks processed)
────────────────────────────────────────────────────────────────────────────────
**4.1 Combine All Chunk Results**
   - Merge scored datasets from ALL chunks into a single list
   - Example: Chunk 1 (10 datasets) + Chunk 2 (8 datasets) = 18 total datasets

**4.2 Remove Duplicates**
   Check for duplicates in this order:
   1. Exact URL match → Keep the one with highest relevance_score
   2. Same filename from different pages → Keep the one with most recent date
   3. Similar titles (>90% match) → Keep the one marked "current"

**4.3 Validate Physician Fee Schedule Relevance**
   For each dataset, verify it's ACTUALLY a physician fee schedule:
   
   ✅ KEEP if:
   - Contains procedure codes (CPT/HCPCS) AND reimbursement rates
   - Title explicitly says "Physician Fee Schedule"
   - Contains physician service categories (E&M, surgery, radiology)
   
   ❌ REMOVE if:
   - Just a policy manual mentioning physician fees
   - Contact directory or enrollment form
   - Training material or FAQ document
   - Other service types (dental, pharmacy, DME) mislabeled

### STEP 5: FINAL RANKING AND TOP-K SELECTION
────────────────────────────────────────────────────────────────────────────────
**5.1 Apply Score Adjustment Factors**
   Start with base relevance_score from filter_datasets, then adjust:
   
   BONUSES (add to score):
   - +0.15: Contains "physician" AND "fee schedule" in title/filename
   - +0.10: Current year (2025, 2026, FY2025, FY2026) in filename
   - +0.10: Marked as "current" or "effective" version
   - +0.05: Excel format (.xlsx, .xls) — easiest to parse
   - +0.03: CSV format — also easy to parse
   
   PENALTIES (subtract from score):
   - -0.10: Contains "archived" or "historical" in title
   - -0.15: Old year detected (2023 or earlier)
   - -0.20: PDF format AND large file size (>10MB) — hard to extract
   - -0.05: Ambiguous title (doesn't clearly say "physician")

**5.2 Sort by Adjusted Score**
   - Sort all datasets by adjusted_score in descending order
   - Cap scores at 1.0 (if bonuses push above 1.0)
   - Floor scores at 0.0 (if penalties push below 0.0)

**5.3 Select Top K Results**
   - Take the top 10-15 datasets (or top_k parameter)
   - Ensure variety: Don't include 10 versions of the same file

**5.4 Enrich Final Dataset Metadata**
   For each selected dataset, ensure complete metadata:
   ```json
   {
     "url": "<direct download link>",
     "title": "<clear, descriptive title>",
     "file_type": "<xlsx|csv|pdf|zip>",
     "category": "physician",
     "relevance_score": <0.0-1.0 after adjustments>,
     "relevance_reason": "<specific reason: CPT codes, rates, etc.>",
     "page_source_url": "<page where found>",
     "is_current": <true|false>,
     "estimated_date": "<YYYY or FY designation>"
   }
   ```

### STEP 6: OUTPUT FINAL RESULTS
────────────────────────────────────────────────────────────────────────────────
**6.1 Construct Response JSON**
   - Include all required fields (see OUTPUT FORMAT below)
   - Ensure relevant_datasets is sorted by score descending

**6.2 Validate JSON**
   - Ensure valid JSON syntax (no trailing commas, proper escaping)
   - Do NOT wrap in markdown code fences

═══════════════════════════════════════════════════════════════════════════════
## PHYSICIAN FEE SCHEDULE IDENTIFICATION
═══════════════════════════════════════════════════════════════════════════════

### ✅ INCLUDE (Physician Fee Schedules Only):
- Physician / professional fee schedules with CPT/HCPCS codes
- Professional services reimbursement rate tables
- Practitioner fee schedules
- Medical services fee schedules with procedure codes
- E&M (Evaluation and Management) service rates
- Surgical procedure fee schedules
- Physician RBRVS-based rate files

### ❌ EXCLUDE (Not Physician Fee Schedules):
- Dental (CDT codes), Pharmacy (NDC), DME, Vision, Home Health, HCBS
- Behavioral/mental health rates (unless bundled with physician)
- Facility rates (hospital inpatient/outpatient)
- Policy documents, manuals, enrollment forms, newsletters

═══════════════════════════════════════════════════════════════════════════════
## PORTAL-SPECIFIC PATTERNS
═══════════════════════════════════════════════════════════════════════════════
- **SharePoint**: URLs with extranet, sp., /_layouts/, /Shared%20Documents/
- **Fee Schedule Sections**: "Physician Fee Schedule", "Professional Rates", "Provider Rates"
- **Current Files**: FY2025, CY2024, SFY25, "Current", "Effective"
- **Rate Terminology**: "Maximum Allowable", "RBRVS", "Conversion Factor"

═══════════════════════════════════════════════════════════════════════════════
## OUTPUT FORMAT
═══════════════════════════════════════════════════════════════════════════════
Return a single JSON object (NO markdown fences):
{
    "state_name": "<state>",
    "portal_type": "<sharepoint|drupal|wordpress|custom>",
    "total_links_discovered": <int>,
    "datasets_after_prefilter": <int>,
    "chunks_processed": <int>,
    "relevant_datasets": [
        {
            "url": "<direct download URL>",
            "title": "<descriptive title>",
            "file_type": "<xlsx|xls|csv ONLY - no other types allowed>",
            "category": "physician",
            "relevance_score": <0.0-1.0>,
            "relevance_reason": "<why this is a physician fee schedule>",
            "page_source_url": "<page where link was found>",
            "is_current": <true|false>,
            "estimated_date": "<YYYY or FY designation if known>"
        }
    ],
    "crawled_pages": ["<url1>", "<url2>"],
    "errors": []
}
"""


# ── Factory ───────────────────────────────────────────────────────────────────


def create_navigator_agent(
    model_id: str = "eu.anthropic.claude-sonnet-4-5-20250929-v1:0",
    region: str = "eu-north-1",
) -> Agent:
    """
    Factory that builds a Strands Navigator Agent wired to Amazon Bedrock.

    The agent is configured with:
      - NAVIGATOR_SYSTEM_PROMPT (portal-aware instructions)
      - Two tools: crawl_portal (Playwright) and filter_datasets (LLM ranking)
      - Low temperature (0.1) for deterministic, consistent output

    Args:
        model_id: Bedrock model identifier.
                  Default: 'us.anthropic.claude-haiku-4-5-20251001-v1:0'
        region:   AWS region for the Bedrock runtime.
                  Default: 'us-east-1'

    Returns:
        Agent: A ready-to-invoke Strands Agent instance.
    """

    bedrock_model = BedrockModel(
        model_id=model_id,
        region_name=region,
        temperature=0.1,
        max_tokens=8192,
    )

    return Agent(
        model=bedrock_model,
        system_prompt=NAVIGATOR_SYSTEM_PROMPT,
        tools=[crawl_portal, filter_datasets],
    )


# ── Runner ────────────────────────────────────────────────────────────────────


def run_navigator(input_data: NavigatorInput) -> NavigatorOutput:
    """
    Execute the Navigator Agent end-to-end for a single portal URL.

    **AgentCore compatible** — `input_data` requires only `portal_url`.
    The agent derives everything else (state, portal type, datasets)
    from the URL at runtime.

    Workflow:
      1. `crawl_portal`     —  headless-browser BFS crawl
      2. `filter_datasets`  —  structures results for LLM analysis
      3. LLM reasoning      —  scores & ranks each dataset
      4. JSON response       —  returns structured `NavigatorOutput`

    Args:
        input_data: `NavigatorInput` with at minimum `portal_url`.

    Returns:
        `NavigatorOutput` with `success=True/False`, ranked datasets,
        crawled pages, portal type, and any errors.
    """
    agent = create_navigator_agent()

    user_prompt = f"""Crawl the following Medicaid portal and identify all downloadable
fee-schedule datasets.

**Portal URL**: {input_data.portal_url}
**Target Category**: {input_data.dataset_category}

"""

    try:
        result = agent(user_prompt)
    except Exception as exc:  # noqa: BLE001
        logger.error("Agent execution failed: %s", exc)
        return NavigatorOutput(
            success=False,
            portal_url=input_data.portal_url,
            errors=[f"Agent execution error: {exc}"],
        )

    print("\nRaw agent response:.........", result)
    try:
        response_text = str(result)
        logger.debug("Raw agent response length: %d", len(response_text))

        parsed = _extract_json_from_response(response_text)

        ranked: list[RankedDataset] = []
        for ds in parsed.get("relevant_datasets", []):
            try:
                try:
                    category = DatasetCategory(ds.get("category", "unknown"))
                except ValueError:
                    category = DatasetCategory.UNKNOWN

                ranked.append(
                    RankedDataset(
                        url=ds["url"],
                        title=ds["title"],
                        file_type=ds["file_type"],
                        category=category,
                        relevance_score=ds["relevance_score"],
                        relevance_reason=ds["relevance_reason"],
                        page_source_url=ds.get(
                            "page_source_url", input_data.portal_url),
                        is_current=ds.get("is_current", True),
                        estimated_date=ds.get("estimated_date"),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping malformed dataset entry: %s", exc)

        return NavigatorOutput(
            success=True,
            portal_url=input_data.portal_url,
            state_name=parsed.get("state_name", ""),
            state_code=parsed.get("state_code", ""),
            total_links_discovered=parsed.get("total_links_discovered", 0),
            relevant_datasets=ranked,
            crawled_pages=parsed.get("crawled_pages", []),
            portal_type=parsed.get("portal_type", "unknown"),
            errors=parsed.get("errors", []),
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Error parsing navigator output: %s", exc)
        return NavigatorOutput(
            success=False,
            portal_url=input_data.portal_url,
            errors=[f"Output parsing error: {exc}"],
        )


# ── AgentCore entrypoint ───────────────────────────────────────────────────


def invoke(payload: dict[str, Any]) -> dict[str, Any]:
    """
    AWS AgentCore–compatible entrypoint.

    Accepts a payload dict and returns a dict.  Maps cleanly to::

        @app.entrypoint
        def handler(payload):
            return invoke(payload)

    Payload keys (only ``portal_url`` is required)::

        {
            "portal_url": "https://...",        # REQUIRED
            "prompt":     "https://...",        # alias for portal_url
            "category":   "all"                 # optional
        }

    Returns:
        dict — the full NavigatorOutput serialised as a JSON-safe dict.

    Raises:
        ValueError: If no portal_url or prompt is provided.
    """
    # AgentCore may deliver the payload as a JSON string — normalise to dict.
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            # Treat a bare string as the portal URL itself
            payload = {"portal_url": payload}

    portal_url = (
        payload.get("portal_url")
        or payload.get("prompt")
        or payload.get("input", {}).get("prompt")  # AgentCore nested format
    )
    if not portal_url or not isinstance(portal_url, str):
        return NavigatorOutput(
            success=False,
            portal_url="",
            errors=["Missing required field: 'portal_url' or 'prompt' in payload."],
        ).model_dump()

    input_data = NavigatorInput(
        portal_url=portal_url.strip(),
        dataset_category=payload.get("category", "all"),
    )

    output = run_navigator(input_data)
    return output.model_dump()


# ── JSON extraction helper ────────────────────────────────────────────────────


def _extract_json_from_response(text: str) -> dict[str, Any]:
    """
    Robustly extract the first complete JSON object from free-form
    agent response text.

    Tries three strategies in order:
      1. Direct json.loads() — works when the response is pure JSON.
      2. Regex for ```json ... ``` markdown code blocks.
      3. Brace-depth matching — walks the string character by character,
         tracking { } nesting, and attempts to parse each outermost
         brace pair.

    Args:
        text: The raw string returned by the Strands agent.
              May contain markdown fences, surrounding prose, or
              explanation text before/after the JSON.

    Returns:
        dict: The parsed JSON object.

    Raises:
        ValueError: If no valid JSON object could be found anywhere
                    in the response text.
    """
    # 1. Direct parse
    try:
        return json.loads(text)  # type: ignore[return-value]
    except json.JSONDecodeError:
        pass

    # 2. Strip markdown code block
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))  # type: ignore[return-value]
        except json.JSONDecodeError:
            pass

    # 3. Brace-matching fallback
    depth = 0
    start: int | None = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    # type: ignore[return-value]
                    return json.loads(text[start: i + 1])
                except json.JSONDecodeError:
                    start = None

    raise ValueError("No valid JSON object found in agent response")

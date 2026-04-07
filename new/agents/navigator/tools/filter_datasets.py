"""
filter_datasets tool
--------------------
Structures crawl results into a detailed LLM analysis prompt so the
Strands agent can rank discovered datasets by Medicaid fee-schedule
relevance.

Includes **duplicate detection**: when the same dataset is offered in
multiple file formats (e.g. .xlsx *and* .pdf), only the best format is
kept.  Priority order: xlsx > xls > csv > pdf > zip > other.

**AgentCore note**: The `state_name` parameter is optional.  In stateless
execution the agent infers the state from the portal URL / page content
and passes it here.  If unavailable, the default "the portal" is used.
"""

from __future__ import annotations

import json
import logging
import re
from difflib import SequenceMatcher
from urllib.parse import unquote, urlparse

from strands import tool

logger = logging.getLogger(__name__)


# ── Format priority (lower = better) ─────────────────────────────────────────

_FORMAT_PRIORITY: dict[str, int] = {
    "xlsx": 0,
    "xls": 1,
    "csv": 2,
    "pdf": 3,
    "zip": 4,
}
_WORST_PRIORITY = 99


def _format_rank(file_type: str) -> int:
    """Return integer rank for *file_type* (lower is better)."""
    return _FORMAT_PRIORITY.get(file_type.lower().strip("."), _WORST_PRIORITY)


# ── Stem-key normalisation ────────────────────────────────────────────────────

# Extensions to strip when computing the stem
_STRIP_EXTS = re.compile(
    r"\.(xlsx|xls|csv|pdf|zip|txt|html|htm|doc|docx)$", re.IGNORECASE
)

# Tokens to collapse during normalisation
_NOISE_RE = re.compile(r"[^a-z0-9]+")


def _stem_from_url(url: str) -> str:
    """
    Extract a normalised stem from a URL by:
      1. Taking the path component
      2. URL-decoding
      3. Stripping the file extension
      4. Lower-casing and collapsing non-alphanumeric chars

    Examples::

        https://portal.gov/fees/Physician_Fee_Schedule_2025.xlsx
        → "physician fee schedule 2025"

        https://portal.gov/fees/Physician_Fee_Schedule_2025.pdf
        → "physician fee schedule 2025"   (same stem → duplicate)
    """
    path = unquote(urlparse(url).path)
    # Remove extension
    path = _STRIP_EXTS.sub("", path)
    # Take only the filename (last segment)
    filename = path.rsplit("/", 1)[-1] if "/" in path else path
    return _NOISE_RE.sub(" ", filename.lower()).strip()


def _stem_from_title(title: str) -> str:
    """
    Normalise a human-readable link title the same way as URL stems
    so they can be compared for similarity.
    """
    # Strip common format suffixes that sometimes appear in link text
    title = _STRIP_EXTS.sub("", title)
    title = re.sub(r"\(?\.(xlsx|xls|csv|pdf|zip)\)?", "", title, flags=re.I)
    return _NOISE_RE.sub(" ", title.lower()).strip()


def _stems_similar(a: str, b: str, threshold: float = 0.80) -> bool:
    """
    True when two stem strings are "similar enough" to be considered
    the same dataset.  Uses SequenceMatcher ratio with a default
    threshold of 0.80.

    Also returns True when one stem is a *prefix* of the other (common
    when one version has an extra date suffix, e.g.
    ``physician fee schedule`` vs ``physician fee schedule 2025``).
    """
    if not a or not b:
        return False
    if a == b:
        return True
    # Prefix match (at least 10 chars to avoid tiny false positives)
    if len(a) >= 10 and (a.startswith(b) or b.startswith(a)):
        return True
    return SequenceMatcher(None, a, b).ratio() >= threshold


# ── Deduplication engine ──────────────────────────────────────────────────────


def _deduplicate_datasets(datasets: list[dict]) -> list[dict]:
    """
    Group datasets that represent the same content in different formats
    and keep only the best format per group.

    **Grouping signals** (in priority order):
      1. Identical URL stem (path minus extension)
      2. High title similarity (SequenceMatcher ≥ 0.80) AND same source page

    **Format priority** (best first): xlsx → xls → csv → pdf → zip → other

    Each returned dataset gains an ``available_formats`` key listing all
    formats that were available before dedup, e.g.::

        {"available_formats": ["xlsx", "pdf"]}

    Returns:
        De-duplicated list of dataset dicts.
    """
    if len(datasets) <= 1:
        return datasets

    # ── Phase 1: assign a group key to every dataset ──────────────────────
    #
    # We use Union-Find so that transitive matches (A≈B, B≈C → A,B,C in
    # same group) are handled correctly.

    n = len(datasets)
    parent: list[int] = list(range(n))

    def _find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def _union(i: int, j: int) -> None:
        ri, rj = _find(i), _find(j)
        if ri != rj:
            parent[ri] = rj

    # Pre-compute stems
    url_stems: list[str] = [_stem_from_url(
        ds.get("url", "")) for ds in datasets]
    title_stems: list[str] = [_stem_from_title(
        ds.get("title", "")) for ds in datasets]

    for i in range(n):
        for j in range(i + 1, n):
            # Signal 1: identical URL stem
            if url_stems[i] and url_stems[j] and url_stems[i] == url_stems[j]:
                _union(i, j)
                continue

            # Signal 2: similar title + same source page
            same_page = (
                datasets[i].get("page_source_url", "") ==
                datasets[j].get("page_source_url", "")
            )
            if same_page and _stems_similar(title_stems[i], title_stems[j]):
                _union(i, j)
                continue

            # Signal 3: similar URL stem (fuzzy) + same source page
            if same_page and _stems_similar(url_stems[i], url_stems[j]):
                _union(i, j)

    # ── Phase 2: pick the best format per group ──────────────────────────

    groups: dict[int, list[int]] = {}
    for i in range(n):
        root = _find(i)
        groups.setdefault(root, []).append(i)

    result: list[dict] = []
    for members in groups.values():
        if len(members) == 1:
            ds = datasets[members[0]].copy()
            ds["available_formats"] = [ds.get("file_type", "unknown")]
            result.append(ds)
            continue

        # Collect all formats in this group
        formats_in_group = [
            datasets[m].get("file_type", "unknown") for m in members
        ]

        # Sort members by format priority (best first)
        ranked = sorted(members, key=lambda m: _format_rank(
            datasets[m].get("file_type", "unknown")
        ))

        best = datasets[ranked[0]].copy()
        best["available_formats"] = sorted(
            set(formats_in_group),
            key=_format_rank,
        )

        # Merge the richest metadata from the group (some formats have
        # better titles / context than others)
        for m in ranked[1:]:
            alt = datasets[m]
            if len(alt.get("title", "")) > len(best.get("title", "")):
                best["title"] = alt["title"]
            if len(alt.get("context_text", "")) > len(best.get("context_text", "")):
                best["context_text"] = alt["context_text"]
            if len(alt.get("parent_section", "")) > len(best.get("parent_section", "")):
                best["parent_section"] = alt["parent_section"]
            if alt.get("last_modified") and not best.get("last_modified"):
                best["last_modified"] = alt["last_modified"]

        result.append(best)

    logger.info(
        "Dedup: %d datasets → %d unique (removed %d format duplicates)",
        len(datasets), len(result), len(datasets) - len(result),
    )
    return result


@tool
def filter_datasets(
    datasets_json: str,
    state_name: str = "the target state",
    dataset_category: str = "all",
) -> str:
    """
    Build a structured LLM analysis prompt from raw crawl results so the
    Navigator Agent can score and rank each dataset by Medicaid fee-schedule
    relevance.

    This is NOT the ranking logic itself — it prepares the input for the
    agent's reasoning step. The agent reads the returned prompt, applies
    the five-tier scoring criteria, and produces a ranked JSON array.

    The agent returns **all** datasets that score above the relevance
    threshold — there is no top-k cap.

    Args:
        datasets_json:    JSON string — the 'datasets' list returned by
                          crawl_portal.
        state_name:       Full state name, e.g. 'Alaska'. Optional — defaults
                          to 'the target state' when the caller has only a URL.
        dataset_category: 'all' to rank everything, or a specific category.

    Returns:
        str: A multi-section analysis prompt, or a JSON error message
             if datasets_json is empty.
    """
    try:
        datasets: list[dict] = json.loads(datasets_json)
    except (json.JSONDecodeError, TypeError) as exc:
        return json.dumps({
            "message": f"Invalid datasets_json input: {exc}",
            "datasets": [],
            "recommendation": "Ensure the datasets list is a valid JSON string.",
        })

    if not datasets:
        return json.dumps({
            "message": "No downloadable datasets were discovered on the portal.",
            "datasets": [],
            "recommendation": (
                "The portal may require authentication, use dynamic loading "
                "that wasn't captured, or the URL may be incorrect."
            ),
        })

    # ── Deduplicate: keep best format per logical dataset ─────────────
    raw_count = len(datasets)
    datasets = _deduplicate_datasets(datasets)
    dedup_removed = raw_count - len(datasets)
    dedup_note = ""
    if dedup_removed:
        dedup_note = (
            f"\nNOTE — DUPLICATE HANDLING: {dedup_removed} format-duplicate(s) "
            f"were removed during pre-processing. When the same dataset was "
            f"offered in multiple file formats, only the best format was kept "
            f"(xlsx > xls > csv > pdf > zip > other). The 'Available Formats' "
            f"field for each dataset lists all formats that were originally "
            f"available.\n"
        )

    category_filter = ""
    if dataset_category != "all":
        category_filter = f"""
CATEGORY FILTER: Focus specifically on '{dataset_category}' fee schedules.
Datasets matching this category should receive a significant score boost.
However, still include other fee schedule types if they score high on general relevance.
"""

    analysis_prompt = f"""
TASK: Analyze and rank {len(datasets)} downloadable files discovered on the {state_name} Medicaid portal.

OBJECTIVE: Identify which files are actual Medicaid fee schedule / reimbursement rate datasets.
{category_filter}{dedup_note}

RELEVANCE SCORING CRITERIA (apply in this order):

1. **CONTENT MATCH (highest weight)**:
   - File title/context mentions: fee schedule, rates, reimbursement, allowable fees,
     maximum allowance, payment schedule, rate table, fee-for-service
   - File contains procedure code references: HCPCS, CPT, CDT, NDC, procedure code
   - Score: 0.8 – 1.0 for strong matches

2. **CATEGORY DETECTION**:
   - Physician / Professional services fee schedules
   - Dental fee schedules (CDT codes)
   - Pharmacy / Drug reimbursement (NDC codes)
   - DMEPOS (Durable Medical Equipment)
   - Outpatient / Inpatient facility rates
   - Behavioral health / Mental health rates
   - Laboratory fee schedules
   - Vision / Optical services
   - Home health / HCBS rate tables
   - Assign the appropriate DatasetCategory enum value

3. **RECENCY (important)**:
   - Current/active files score higher than archived/historical
   - Look for year indicators in filenames: 2025, 2024, FY2025, CY2024, SFY25
   - Files with "effective", "current", "active" in context score higher
   - Files with "archive", "historical", "prior", "old" score lower

4. **FILE TYPE PREFERENCE**:
   - Excel (.xlsx, .xls) and CSV (.csv): +0.05 bonus (structured, easier to parse)
   - PDF (.pdf): neutral (may need OCR)
   - ZIP (.zip): −0.05 (requires extraction, may contain multiple files)

5. **NEGATIVE SIGNALS (reduce score significantly)**:
   - Provider manuals, policy documents, enrollment forms → score 0.0–0.2
   - Meeting minutes, newsletters, presentations → score 0.0
   - Prior authorization forms, claim forms → score 0.0–0.1
   - General informational pages with no data → score 0.0

DATASETS DISCOVERED:
"""

    for idx, ds in enumerate(datasets, 1):
        available = ds.get("available_formats")
        formats_line = ""
        if available and len(available) > 1:
            formats_line = (
                f"\nAvailable Formats: {', '.join(available)} "
                f"(best format '{available[0]}' selected)"
            )
        analysis_prompt += f"""
--- Dataset {idx} of {len(datasets)} ---
URL: {ds['url']}
Title: {ds['title']}
File Type: {ds['file_type']}
Source Page: {ds['page_source_url']}
Section Header: {ds.get('parent_section', 'N/A')}
Last Modified: {ds.get('last_modified', 'N/A')}
Surrounding Context: {ds.get('context_text', 'N/A')[:300]}{formats_line}
"""

    analysis_prompt += """

RESPONSE FORMAT:
Return a valid JSON array of ALL relevant datasets.
Each object must have these exact fields:

{
    "url": "<direct download URL>",
    "title": "<descriptive title>",
    "file_type": "<pdf|xls|xlsx|csv|zip>",
    "category": "<physician|dental|pharmacy|dmepos|outpatient|inpatient|behavioral_health|laboratory|vision|home_health|general|unknown>",
    "relevance_score": <float 0.0 to 1.0>,
    "relevance_reason": "<one sentence explaining the score>",
    "page_source_url": "<page where link was found>",
    "is_current": <true if this appears to be the current/active version>,
    "estimated_date": "<estimated effective date or null>"
}

RULES:
- Sort by relevance_score descending.
- Only include datasets with relevance_score >= 0.3.
- Do NOT include provider manuals, policy docs, or forms.
- Return ONLY the JSON array — no markdown, no explanation, no wrapper object.
"""

    # Filter to only flat files (Excel and CSV) FIRST
    flat_file_datasets = [
        ds for ds in datasets
        if ds.get("file_type", "").lower() in ["xlsx", "xls", "csv"]
    ]

    if not flat_file_datasets:
        return {
            "filtered_datasets": [],
            "total_filtered": 0,
            "note": "No Excel or CSV files found. PDF and ZIP files are not supported."
        }

    # Continue with only flat files
    datasets = flat_file_datasets

    # Quick filter: remove obviously irrelevant datasets
    # ...existing code...

    return analysis_prompt

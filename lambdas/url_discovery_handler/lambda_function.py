"""
AWS Lambda boilerplate for Medicaid URL discovery.

This file is intentionally minimal and structured for custom implementation.
"""

import json
import logging
import os
from typing import Any, TypeVar
from urllib.parse import urljoin, urlparse

_T = TypeVar("_T")

import boto3

logger = logging.getLogger("[url_discovery_handler]")
logger.setLevel(logging.INFO)


def _log_stage(stage: str, message: str, **fields: Any) -> None:
    meta = " ".join([f"{k}={v}" for k, v in fields.items()])
    logger.info(f"[{stage}] {message}" + (f" | {meta}" if meta else ""))


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _json_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    _log_stage(
        "response", "Returning lambda response", status_code=status_code, body=body
    )
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "data": body,
    }


def _env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        _log_stage("config", "Required env var missing", env_var=name)
        raise RuntimeError(f"Missing required env var: {name}")
    _log_stage("config", "Loaded required env var", env_var=name)
    return value


def _validate_input_url(raw_url: Any) -> str:
    """Validate and normalize input URL before any crawling/extraction."""
    value = str(raw_url or "").strip()
    if not value:
        raise ValueError("state_url is required")

    parsed = urlparse(value)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("Invalid URL: scheme must be http or https")
    if not parsed.netloc:
        raise ValueError("Invalid URL: missing host")

    return value


# Environment variable names
# DATABASE_URL = _env_required("DATABASE_URL")
LLM_PROVIDER = _env_required("LLM_PROVIDER")
if LLM_PROVIDER == "bedrock":
    LLM_MODEL = _env_required("BEDROCK_MODEL")
else:
    raise RuntimeError("LLM_PROVIDER must be one of: bedrock")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1").strip() or "us-east-1"
_log_stage(
    "config",
    "Resolved runtime configuration",
    llm_provider=LLM_PROVIDER,
    llm_model=LLM_MODEL,
    aws_region=AWS_REGION,
)


# def _parse_event_body(event: dict[str, Any]) -> dict[str, Any]:
#     _log_stage("input", "Parsing event body", event_type=type(event).__name__)
#     body = event.get("body", {})
#     if isinstance(body, str):
#         body = body.strip()
#         if not body:
#             _log_stage("input", "Event body string is empty")
#             return {}
#         parsed = json.loads(body)
#         _log_stage("input", "Parsed body from string", parsed_type=type(parsed).__name__)
#         return parsed
#     if isinstance(body, dict):
#         _log_stage("input", "Event body already dict", keys=list(body.keys()))
#         return body
#     _log_stage("input", "Unsupported body type; using empty body", body_type=type(body).__name__)
#     return {}


def _bedrock_client():
    _log_stage("llm", "Initializing Bedrock client", region=AWS_REGION, model=LLM_MODEL)
    return boto3.client("bedrock-runtime", region_name=AWS_REGION)


def _resolve_provider_from_llm_model():
    _log_stage("llm", "Resolving LLM provider", provider=LLM_PROVIDER)
    if LLM_PROVIDER == "bedrock":
        return _bedrock_client()
    else:
        raise ValueError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")


# ======================
# Database initialization
# ======================
# ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)
# SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False)
# _log_stage("db", "Database engine initialized")


# def _get_state_info(state_id: int) -> dict[str, Any] | None:
#     _log_stage("db", "Fetching active state", state_id=state_id)
#     with SessionLocal() as session:
#         row = (
#             session.execute(
#                 text(
#                     """
# 				SELECT id, state_name, state_home_link
# 				FROM state_registry
# 				WHERE id = :state_id AND is_active = TRUE
# 				LIMIT 1
# 				"""
#                 ),
#                 {"state_id": state_id},
#             )
#             .mappings()
#             .first()
#         )
#         _log_stage("db", "State lookup complete", state_found=bool(row))
#         return dict(row) if row else None


def _build_website_metadata(
    requested_url: str,
    final_url: str,
    status_code: int,
    headers: dict[str, Any],
    html_content: str,
) -> dict[str, Any]:
    """Build metadata to validate whether fetched page content is usable HTML."""
    from bs4 import BeautifulSoup  # type: ignore

    content_type = str(headers.get("Content-Type", "") or "")
    soup = BeautifulSoup(html_content or "", "html.parser")

    title = ""
    if soup.title and soup.title.get_text(strip=True):
        title = soup.title.get_text(strip=True)[:200]

    anchor_count = len(soup.find_all("a", href=True))
    body_text = soup.get_text(separator=" ", strip=True)
    body_text_length = len(body_text)

    html_lower = (html_content or "").lower()
    has_html_markers = any(
        m in html_lower for m in ("<html", "<!doctype html", "<body", "<a ")
    )
    content_type_ok = (
        ("text/html" in content_type.lower())
        or ("application/xhtml+xml" in content_type.lower())
        or (not content_type.strip())
    )
    status_ok = 200 <= int(status_code) < 300

    invalid_reasons: list[str] = []
    if not status_ok:
        invalid_reasons.append(f"unexpected_status:{status_code}")
    if not content_type_ok:
        invalid_reasons.append(f"unexpected_content_type:{content_type}")
    if not has_html_markers:
        invalid_reasons.append("html_markers_missing")
    if anchor_count == 0:
        invalid_reasons.append("no_anchor_links_found")
    if body_text_length < 80:
        invalid_reasons.append("insufficient_text_content")

    is_valid_html_data = len(invalid_reasons) == 0

    return {
        "requested_url": requested_url,
        "final_url": final_url,
        "status_code": status_code,
        "content_type": content_type,
        "title": title,
        "anchor_count": anchor_count,
        "body_text_length": body_text_length,
        "is_valid_html_data": is_valid_html_data,
        "invalid_reasons": invalid_reasons,
    }


def download_html_content(url: str) -> dict[str, Any]:
    """Download HTML content and return payload + website validation metadata."""
    import requests

    _log_stage("navigator", "Downloading HTML", url=url)
    response = requests.get(
        url,
        timeout=15,
        allow_redirects=True,
    )
    response.raise_for_status()
    metadata = _build_website_metadata(
        requested_url=url,
        final_url=str(getattr(response, "url", url)),
        status_code=int(response.status_code),
        headers=dict(response.headers),
        html_content=response.text,
    )
    _log_stage(
        "navigator",
        "Downloaded HTML",
        status_code=response.status_code,
        content_length=len(response.text),
        final_url=getattr(response, "url", url),
        content_type=response.headers.get("Content-Type", ""),
        is_valid_html_data=metadata.get("is_valid_html_data"),
    )
    return {
        "html_content": response.text,
        "website_metadata": metadata,
    }


# Block-level tags that provide meaningful surrounding context for a link
_CONTEXT_BLOCK_TAGS = {
    "li",
    "td",
    "th",
    "p",
    "dt",
    "dd",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "caption",
    "figcaption",
    "article",
    "section",
}


def _anchor_context(anchor, max_len: int = 200) -> str:
    """
    Return the text of the minimal enclosing block-level parent of *anchor*
    (e.g. <li>, <td>, <p>), with leading/trailing whitespace collapsed.
    Falls back to the anchor's own text + title attribute when no block parent exists.
    """
    import re

    _ws = re.compile(r"\s+")

    node = anchor.parent
    while node is not None and getattr(node, "name", None) not in _CONTEXT_BLOCK_TAGS:
        node = getattr(node, "parent", None)

    if node is not None and getattr(node, "name", None) in _CONTEXT_BLOCK_TAGS:
        raw = _ws.sub(" ", node.get_text(separator=" ")).strip()
    else:
        # Fallback: anchor text + optional title attribute
        parts = [_ws.sub(" ", anchor.get_text(separator=" ")).strip()]
        title = (anchor.get("title") or "").strip()
        if title:
            parts.append(title)
        raw = " | ".join(p for p in parts if p)

    return raw[:max_len]


def _extract_candidate_urls_from_html(
    base_url: str, html_content: str
) -> list[dict[str, str]]:
    """
    Extract candidate URLs from HTML anchors together with their link context.
    Each entry is a dict:
      - "url":     absolute, normalised URL
      - "context": text of the minimal enclosing block element (li, td, p, …)
                   so the LLM sees the human-readable label around the link.
    Uses BeautifulSoup for robust parsing.
    """
    from bs4 import BeautifulSoup  # type: ignore

    soup = BeautifulSoup(html_content, "html.parser")

    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    allowed_schemes = {"http", "https"}

    for anchor in soup.find_all("a", href=True):
        href_attr = anchor.get("href", "")
        if isinstance(href_attr, list):
            href = str(href_attr[0]).strip() if href_attr else ""
        else:
            href = str(href_attr).strip()
        if not href:
            continue
        absolute = urljoin(base_url, href).strip()
        if not absolute:
            continue
        parsed = urlparse(absolute)
        if parsed.scheme.lower() not in allowed_schemes:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)

        context = _anchor_context(anchor)
        candidates.append({"url": absolute, "context": context})

    max_candidates = _env_int("URL_DISCOVERY_MAX_CANDIDATE_LINKS", 250)
    _log_stage(
        "navigator",
        "Extracted candidate URLs",
        candidate_count=len(candidates),
        max_candidates=max_candidates,
    )
    return candidates[:max_candidates]


def _chunk_list(values: list[_T], chunk_size: int) -> list[list[_T]]:
    if chunk_size <= 0:
        chunk_size = 25
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def _extract_json_payload(raw_text: str) -> Any:
    """
    Best-effort JSON extraction from raw LLM text.
    Handles:
      - Clean JSON object/array
      - JSON embedded inside markdown code fences (```json ... ```)
      - JSON object/array buried in surrounding prose
    """
    import re

    text_value = (raw_text or "").strip()
    if not text_value:
        return None

    # Strip markdown code fences if present
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text_value, re.IGNORECASE)
    if fence_match:
        text_value = fence_match.group(1).strip()

    # Direct parse first
    try:
        return json.loads(text_value)
    except Exception:
        pass

    # Find first JSON object
    obj_match = re.search(r"\{[\s\S]*\}", text_value)
    if obj_match:
        try:
            return json.loads(obj_match.group(0))
        except Exception:
            pass

    # Find first JSON array
    arr_match = re.search(r"\[[\s\S]*\]", text_value)
    if arr_match:
        try:
            return json.loads(arr_match.group(0))
        except Exception:
            pass

    return None


def _extract_summary_text_from_html(html_content: str, max_chars: int = 12000) -> str:
    """Extract compact plain text from HTML for relevance summarization."""
    import re

    from bs4 import BeautifulSoup  # type: ignore

    soup = BeautifulSoup(html_content or "", "html.parser")
    text_value = soup.get_text(separator=" ", strip=True)
    text_value = re.sub(r"\s+", " ", text_value).strip()
    return text_value[:max_chars]


def _summarize_html_relevance_with_llm(
    base_url: str,
    html_content: str,
    website_metadata: dict[str, Any],
    llm: Any,
) -> dict[str, Any]:
    """Use LLM to summarize page and decide whether it is relevant for fee schedule discovery."""
    summary_chars = _env_int("HTML_SUMMARY_MAX_CHARS", 12000)
    text_sample = _extract_summary_text_from_html(html_content, max_chars=summary_chars)
    if not text_sample:
        raise ValueError("Unable to summarize HTML: empty text content")

    SYSTEM_CONTEXT = (
        "You are a Medicaid website relevance classifier. "
        "Given page metadata and text, determine if the page is relevant for Medicaid fee schedule URL discovery. "
        "Relevant pages include Medicaid fee schedules, reimbursement rates, HCPCS/CPT pricing, billing rate tables, "
        "or pages linking to those files. "
        "Irrelevant pages include login, privacy, contact, news, careers, general portal pages without fee/rate data.\n\n"
        "Return STRICT JSON ONLY:\n"
        "{\n"
        '  "is_relevant": true,\n'
        '  "summary": "short summary of page content",\n'
        '  "reasoning": "why this page is or is not relevant",\n'
        '  "confidence": 0.0\n'
        "}"
    )

    user_message = (
        f"Base URL: {base_url}\n"
        f"Final URL: {website_metadata.get('final_url', base_url)}\n"
        f"Page title: {website_metadata.get('title', '')}\n"
        f"Content-Type: {website_metadata.get('content_type', '')}\n"
        f"Anchor count: {website_metadata.get('anchor_count', 0)}\n"
        f"Body text length: {website_metadata.get('body_text_length', 0)}\n\n"
        "Page text sample:\n"
        f"{text_sample}"
    )

    _log_stage(
        "relevance",
        "Invoking LLM for HTML summary and relevance",
        sample_chars=len(text_sample),
    )

    response = llm.converse(
        modelId=LLM_MODEL,
        system=[{"text": SYSTEM_CONTEXT}],
        messages=[{"role": "user", "content": [{"text": user_message}]}],
        inferenceConfig={"temperature": 0},
    )
    parts = response.get("output", {}).get("message", {}).get("content", [])
    response_text = "\n".join(
        str(part.get("text", ""))
        for part in parts
        if isinstance(part, dict) and part.get("text")
    ).strip()

    _log_stage(
        "relevance",
        "Raw relevance response",
        response_length=len(response_text),
        preview=response_text[:300],
    )

    payload = _extract_json_payload(response_text)
    if not isinstance(payload, dict):
        raise ValueError("Unable to assess HTML relevance from LLM output")

    raw_flag = payload.get("is_relevant", False)
    if isinstance(raw_flag, bool):
        is_relevant = raw_flag
    elif isinstance(raw_flag, str):
        is_relevant = raw_flag.strip().lower() in {"true", "yes", "1", "relevant"}
    else:
        is_relevant = bool(raw_flag)

    summary = str(payload.get("summary", "")).strip()
    reasoning = str(payload.get("reasoning", "")).strip()
    confidence_raw = payload.get("confidence", 0)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0

    result = {
        "is_relevant": is_relevant,
        "summary": summary,
        "reasoning": reasoning,
        "confidence": confidence,
    }
    _log_stage(
        "relevance",
        "HTML relevance decision complete",
        is_relevant=is_relevant,
        confidence=confidence,
    )
    return result


def _normalize_selected_urls(parsed_payload: Any, candidate_set: set[str]) -> list[str]:
    """
    Normalize LLM output into a validated list of URLs.
    Accepts either:
      - ["url1", "url2", ...]
      - {"selected_urls": [...], "reasoning": "..."}
    Only keeps URLs that exist in candidate_set to reject hallucinated values.
    If none match the candidate set, returns all parsed URLs as-is (LLM may have
    cleaned/reformatted a URL slightly).
    """
    urls_raw: Any = parsed_payload
    if isinstance(parsed_payload, dict):
        reasoning = parsed_payload.get("reasoning", "")
        if reasoning:
            _log_stage("extractor", "LLM reasoning", reasoning=str(reasoning)[:300])
        urls_raw = parsed_payload.get("selected_urls", [])

    if not isinstance(urls_raw, list):
        return []

    parsed: list[str] = []
    seen: set[str] = set()
    for item in urls_raw:
        value = str(item).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        parsed.append(value)

    # Cross-check against original candidates
    validated = [u for u in parsed if u in candidate_set]
    if validated:
        return validated

    # LLM may have slightly altered URLs — return as-is if nothing matched
    return parsed


def extract_urls_with_llm(base_url: str, html_content: str, llm) -> dict[str, Any]:
    """Identify fee-schedule URLs using URL chunking + LLM classification."""
    candidates = _extract_candidate_urls_from_html(base_url, html_content)
    if not candidates:
        _log_stage("extractor", "No candidate URLs found in HTML")
        raise ValueError("No URL found based on the requirement")

    chunk_size = _env_int("URL_DISCOVERY_CHUNK_SIZE", 25)
    chunks: list[list[dict[str, str]]] = _chunk_list(candidates, chunk_size)
    candidate_set = {c["url"] for c in candidates}
    _log_stage(
        "extractor",
        "Starting LLM URL extraction with chunks",
        provider=LLM_PROVIDER,
        candidate_count=len(candidates),
        chunk_size=chunk_size,
        chunk_count=len(chunks),
    )

    SYSTEM_CONTEXT = (
        "You are a Medicaid data discovery agent. "
        "Your task is to identify which URLs from a state Medicaid agency website "
        "are likely to contain downloadable fee schedule or rate data "
        "(Excel, CSV, PDF, or HTML tables with procedure codes and payment rates).\n\n"
        "INCLUDE:\n"
        "  - URLs ending in .xlsx, .xls, .csv, .pdf\n"
        "  - URLs containing words: fee, rate, schedule, hcpcs, cpt, ndc, crosswalk, billing, reimbursement\n"
        "  - HTML pages that list or link to downloadable fee schedule files\n\n"
        "EXCLUDE:\n"
        "  - Contact, FAQ, login, form, privacy, or news pages\n"
        "  - Policy manuals, handbooks, bulletins, training pages\n"
        "  - Provider directories, maps, general information pages\n\n"
        "Return STRICT JSON ONLY — no markdown, no explanation outside JSON:\n"
        "{\n"
        '  "selected_urls": ["url1", "url2"],\n'
        '  "reasoning": "one-line summary of what was kept and why"\n'
        "}"
    )

    all_selected: list[str] = []
    reasoning_data: list[dict[str, Any]] = []
    seen: set[str] = set()

    for idx, chunk in enumerate(chunks, start=1):
        url_list = "\n".join(
            (
                f"{i + 1}. [{cand['context']}] {cand['url']}"
                if cand["context"]
                else f"{i + 1}. {cand['url']}"
            )
            for i, cand in enumerate(chunk)
        )
        user_message = (
            f"Base URL (state portal): {base_url}\n\n"
            f"Candidate URLs from this page ({len(chunk)} total):\n"
            f"{url_list}\n\n"
            "Select only the URLs that contain or directly link to Medicaid fee schedule data."
        )

        _log_stage(
            "extractor",
            "Invoking LLM",
            chunk_index=idx,
            chunk_total=len(chunks),
            chunk_len=len(chunk),
        )

        response = llm.converse(
            modelId=LLM_MODEL,
            system=[{"text": SYSTEM_CONTEXT}],
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            inferenceConfig={"temperature": 0},
        )
        parts = response.get("output", {}).get("message", {}).get("content", [])
        response_text = "\n".join(
            str(part.get("text", ""))
            for part in parts
            if isinstance(part, dict) and part.get("text")
        ).strip()

        _log_stage(
            "extractor",
            "Raw LLM response",
            chunk_index=idx,
            response_length=len(response_text),
            preview=response_text[:300],
        )

        parsed_payload = _extract_json_payload(response_text)
        selected = _normalize_selected_urls(parsed_payload, candidate_set)
        if isinstance(parsed_payload, dict):
            reasoning = str(parsed_payload.get("reasoning", "")).strip()
            if reasoning:
                reasoning_data.append(
                    {
                        "chunk_index": idx,
                        "reasoning": reasoning,
                        "selected_urls": selected,
                    }
                )
        _log_stage(
            "extractor", "Chunk complete", chunk_index=idx, selected_count=len(selected)
        )

        for url in selected:
            if url in seen:
                continue
            seen.add(url)
            all_selected.append(url)

    if all_selected:
        _log_stage(
            "extractor", "LLM selection complete", selected_total=len(all_selected)
        )
        return {
            "urls": all_selected,
            "reasoning_data": reasoning_data,
        }

    _log_stage("extractor", "No URLs found matching the fee schedule requirements")
    raise ValueError("No URL found based on the requirement")


def _discover_urls(
    state_home_link: str,
):
    """Download the HTML content of the given URL and use the specified LLM to extract potential Medicaid fee schedule URLs."""
    verified_url = _validate_input_url(state_home_link)
    _log_stage("pipeline", "Starting URL discovery", state_home_link=verified_url)

    # LLM initialization
    llm = _resolve_provider_from_llm_model()

    # Download HTML content
    page_payload = download_html_content(verified_url)
    html_content = str(page_payload.get("html_content", ""))
    website_metadata = page_payload.get("website_metadata", {})

    if not website_metadata.get("is_valid_html_data", False):
        _log_stage(
            "pipeline",
            "Website metadata validation failed",
            invalid_reasons=website_metadata.get("invalid_reasons", []),
            final_url=website_metadata.get("final_url", verified_url),
        )
        raise ValueError(
            "Invalid or non-qualifying HTML content for URL identification"
        )

    relevance_result = _summarize_html_relevance_with_llm(
        verified_url,
        html_content,
        website_metadata,
        llm,
    )
    if not relevance_result.get("is_relevant", False):
        _log_stage(
            "pipeline",
            "HTML not relevant for fee schedule discovery",
            reasoning=relevance_result.get("reasoning", ""),
            confidence=relevance_result.get("confidence", 0.0),
        )
        raise ValueError("HTML is not relevant for the fee schedule discovery use case")

    # Extract URLs using the LLM
    extraction_result = extract_urls_with_llm(verified_url, html_content, llm)
    max_links = _env_int("URL_DISCOVERY_MAX_LINKS", 100)
    urls = extraction_result["urls"][:max_links]
    reasoning_data = extraction_result.get("reasoning_data", [])
    _log_stage("pipeline", "URL discovery complete", discovered_url_count=len(urls))
    return {
        "urls": urls,
        "reasoning_data": reasoning_data,
        "website_metadata": website_metadata,
        "html_relevance": relevance_result,
    }


# def _upsert_discovered_urls(state_name: str, urls: list[str]) -> int:
#     pass


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    try:
        request_id = (
            getattr(context, "aws_request_id", "local")
            if context is not None
            else "local"
        )
        _log_stage("handler", "Invocation started", request_id=request_id)
        _log_stage("handler", "Received event", event=event)

        state_url = event.get("state_url")
        if state_url is None:
            _log_stage("handler", "Missing state_url in request")
            return _json_response(400, {"error": "missing_state_url"})

        try:
            state_url = _validate_input_url(state_url)
        except ValueError as exc:
            _log_stage(
                "handler",
                "Invalid state_url provided",
                state_url=state_url,
                error_detail=str(exc),
            )
            return _json_response(
                400, {"error": "invalid_state_url", "message": str(exc)}
            )
        # try:
        #     state_id = int(state_url)
        # except (TypeError, ValueError):
        #     _log_stage("handler", "Invalid state_id type", state_url=state_url)
        #     return _json_response(400, {"error": "invalid_state_id"})

        # # Fetch state info from DB
        # state_info = _get_state_info(state_id)
        # if not state_info:
        #     _log_stage("handler", "No active state found", state_id=state_id)
        #     return _json_response(404, {"error": "state_not_found"})

        # Fetch the HTML content of the state's home link
        # and discover potential URLs using the specified LLM provider and model.
        discovery_result = _discover_urls(state_url)
        discovered_urls = discovery_result["urls"]
        reasoning_data = discovery_result.get("reasoning_data", [])
        website_metadata = discovery_result.get("website_metadata", {})
        html_relevance = discovery_result.get("html_relevance", {})

        _log_stage(
            "handler",
            "Discovered URLs for state",
            state_url=state_url,
            discovered_url_count=len(discovered_urls),
        )

        return _json_response(
            200,
            {
                "state_url": state_url,
                "discovered_urls": discovered_urls,
                "reasoning_data": reasoning_data,
                "website_metadata": website_metadata,
                "html_relevance": html_relevance,
            },
        )
    except Exception as exc:
        logger.exception("[handler] Unhandled exception during invocation")
        return _json_response(
            500, {"error": "url_discovery_failed", "message": str(exc)}
        )

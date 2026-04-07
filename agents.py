import json
import os
import re
import hashlib
import logging
import io
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen

import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import and_, select, text

from database import AgentMemory, AgentHandoff, MappingColumn, SourceMetadata, CanonicalColumnMapping, engine, get_session

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")

PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "sentinel-state-v2")


def _runtime_mode() -> str:
    return os.getenv("RUNTIME_MODE", "local").strip().lower() or "local"


def _aws_mode_enabled() -> bool:
    return _runtime_mode() in {"aws", "hybrid"}


def _safe_slug(value: str, fallback: str = "unknown") -> str:
    clean = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(value or "")).strip("_").lower()
    return clean or fallback


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _build_storage_key(layer: str, state_name: str, source_name: str, run_id: str, file_name: str) -> str:
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return "/".join(
        [
            _safe_slug(layer),
            _safe_slug(state_name),
            _safe_slug(source_name),
            _safe_slug(run_id, fallback="manual"),
            f"{stamp}_{_safe_slug(file_name, fallback='artifact')}",
        ]
    )


def _write_storage_bytes(layer: str, key: str, payload: bytes, content_type: str) -> str:
    if _aws_mode_enabled():
        bucket = os.getenv(f"{layer.upper()}_BUCKET", "").strip()
        if bucket:
            try:
                import boto3  # type: ignore

                s3_client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
                s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=payload,
                    ContentType=content_type,
                    Metadata={"pipeline": "sentinel-state", "layer": layer, "version": PIPELINE_VERSION},
                )
                return f"s3://{bucket}/{key}"
            except Exception as exc:
                LOGGER.warning(f"[{layer}] S3 write failed; falling back to local storage: {type(exc).__name__}: {exc}")

    root = Path(os.getenv("DATA_LAKE_ROOT", "data_lake"))
    destination = root / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)
    return str(destination.resolve())


def _publish_hitl_alert(state_name: str, source_url: str, confidence: float, reason: str) -> None:
    message = {
        "state_name": state_name,
        "source_url": source_url,
        "confidence": confidence,
        "reason": reason,
        "timestamp": _utc_now_iso(),
        "pipeline_version": PIPELINE_VERSION,
    }
    if _aws_mode_enabled():
        topic_arn = os.getenv("HITL_SNS_TOPIC_ARN", "").strip()
        if topic_arn:
            try:
                import boto3  # type: ignore

                sns_client = boto3.client("sns", region_name=os.getenv("AWS_REGION", "us-east-1"))
                sns_client.publish(
                    TopicArn=topic_arn,
                    Subject=f"Sentinel-State HITL review: {state_name}",
                    Message=json.dumps(message),
                )
                LOGGER.info(f"[HITL] Published SNS alert for {state_name} (confidence={confidence})")
                return
            except Exception as exc:
                LOGGER.warning(f"[HITL] SNS publish failed: {type(exc).__name__}: {exc}")
    LOGGER.info(f"[HITL] Local alert: {json.dumps(message)}")


def _checkpoint_state_snapshot(state: dict[str, Any], node_name: str, phase: str) -> None:
    run_id = str(state.get("run_id", "manual"))
    state_name = str(state.get("state_name", "unknown"))
    snapshot = {
        "run_id": run_id,
        "state_name": state_name,
        "node": node_name,
        "phase": phase,
        "status": state.get("status", "unknown"),
        "timestamp": _utc_now_iso(),
    }

    if _aws_mode_enabled():
        ddb_table = os.getenv("CHECKPOINT_TABLE_NAME", "").strip()
        if ddb_table:
            try:
                import boto3  # type: ignore

                ddb = boto3.client("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1"))
                ddb.put_item(
                    TableName=ddb_table,
                    Item={
                        "run_id": {"S": run_id},
                        "checkpoint_key": {"S": f"{node_name}#{phase}#{snapshot['timestamp']}"},
                        "state_name": {"S": state_name},
                        "status": {"S": str(snapshot["status"])},
                        "node": {"S": node_name},
                        "phase": {"S": phase},
                        "timestamp": {"S": snapshot["timestamp"]},
                    },
                )
                return
            except Exception as exc:
                LOGGER.warning(f"[Checkpoint] DynamoDB write failed: {type(exc).__name__}: {exc}")

    checkpoint_root = Path(os.getenv("LOCAL_CHECKPOINT_DIR", "checkpoints"))
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    log_file = checkpoint_root / f"{_safe_slug(state_name)}_{_safe_slug(run_id)}.jsonl"
    with log_file.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(snapshot) + "\n")


def _write_bronze_snapshot(state: dict[str, Any]) -> str | None:
    state_name = str(state.get("state_name", "unknown"))
    source_name = str(state.get("source_name", "source"))
    source_url = str(state.get("source_url", "")).strip()
    run_id = str(state.get("run_id", "manual"))

    content: bytes | None = None
    if source_url:
        try:
            with urlopen(source_url, timeout=45) as response:
                content = response.read()
        except Exception as exc:
            LOGGER.warning(f"[Bronze] Could not download original source bytes: {type(exc).__name__}: {exc}")

    if not content:
        fallback_obj = {
            "source_url": source_url,
            "raw_records": state.get("raw_records", []),
            "captured_at": _utc_now_iso(),
        }
        content = json.dumps(fallback_obj).encode("utf-8")
        file_name = "raw_snapshot.json"
        content_type = "application/json"
    else:
        ext = os.path.splitext(urlparse(source_url).path)[1].lower().strip(".")
        file_name = f"raw_source.{ext or 'bin'}"
        content_type = "application/octet-stream"

    key = _build_storage_key("bronze", state_name, source_name, run_id, file_name)
    return _write_storage_bytes("bronze", key, content, content_type)


def _write_silver_dataset(state: dict[str, Any]) -> str | None:
    state_name = str(state.get("state_name", "unknown"))
    source_name = str(state.get("source_name", "source"))
    run_id = str(state.get("run_id", "manual"))
    records = state.get("standardized_records", []) or []

    silver_df = pd.DataFrame(records)
    if silver_df.empty:
        return None

    # Attach lineage columns for downstream audit (mirrors AWS bronze/silver/gold lineage contract).
    silver_df["source_url"] = str(state.get("source_url", ""))
    silver_df["ingestion_timestamp"] = _utc_now_iso()
    silver_df["agent_version"] = PIPELINE_VERSION

    buffer = io.BytesIO()
    file_name = "data.parquet"
    content_type = "application/octet-stream"
    try:
        silver_df.to_parquet(buffer, index=False)
    except Exception:
        csv_buffer = io.StringIO()
        silver_df.to_csv(csv_buffer, index=False)
        buffer = io.BytesIO(csv_buffer.getvalue().encode("utf-8"))
        file_name = "data.csv"
        content_type = "text/csv"

    key = _build_storage_key("silver", state_name, source_name, run_id, file_name)
    return _write_storage_bytes("silver", key, buffer.getvalue(), content_type)


def _ensure_gold_table() -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS gold_medicaid_rates (
                    id BIGSERIAL PRIMARY KEY,
                    state_id INTEGER NOT NULL,
                    state_name TEXT NOT NULL,
                    dataset_type TEXT NOT NULL,
                    procedure_code TEXT NOT NULL,
                    modifier TEXT NOT NULL DEFAULT '',
                    description TEXT,
                    fee_amount TEXT,
                    effective_date TEXT,
                    end_date DATE,
                    source_url TEXT NOT NULL,
                    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
                    agent_version TEXT NOT NULL,
                    row_hash TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_gold_rates_active_key
                ON gold_medicaid_rates (state_id, dataset_type, procedure_code, modifier)
                WHERE end_date IS NULL AND is_active = TRUE
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_rates_version
                ON gold_medicaid_rates (
                    state_id, dataset_type, procedure_code, modifier, effective_date, row_hash
                )
                """
            )
        )
        # Migration: add state_id to existing tables that predate this column
        connection.execute(text(
            "ALTER TABLE gold_medicaid_rates ADD COLUMN IF NOT EXISTS state_id INTEGER NOT NULL DEFAULT 0"
        ))


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def _rate_to_text(value: Any) -> str | None:
    token = _norm_text(value)
    if token is None:
        return None
    compact = token.replace("$", "").replace(",", "")
    try:
        return f"{float(compact):.6f}".rstrip("0").rstrip(".")
    except Exception:
        return token


def _upsert_gold_scd2_records(state: dict[str, Any]) -> dict[str, int]:
    _ensure_gold_table()

    state_id = int(state.get("state_id") or 0)
    state_name = _norm_text(state.get("state_name")) or "unknown"
    dataset_type = _norm_text(state.get("source_name")) or "unknown"
    source_url = _norm_text(state.get("source_url")) or ""

    inserted = 0
    closed = 0
    unchanged = 0

    with engine.begin() as connection:
        for record in state.get("standardized_records", []) or []:
            procedure_code = _norm_text(record.get("procedure_code"))
            if not procedure_code:
                continue

            modifier = _norm_text(record.get("modifier")) or ""
            description = _norm_text(record.get("description"))
            fee_amount = _rate_to_text(record.get("fee_amount"))
            effective_date = _norm_text(record.get("effective_date"))

            hash_payload = "|".join(
                [
                    str(state_id),
                    state_name,
                    dataset_type,
                    procedure_code,
                    modifier,
                    str(fee_amount or ""),
                    str(effective_date or ""),
                    str(description or ""),
                ]
            )
            row_hash = hashlib.sha256(hash_payload.encode("utf-8")).hexdigest()

            active = connection.execute(
                text(
                    """
                    SELECT id, description, fee_amount, effective_date
                    FROM gold_medicaid_rates
                    WHERE state_id = :state_id
                      AND dataset_type = :dataset_type
                      AND procedure_code = :procedure_code
                      AND modifier = :modifier
                      AND end_date IS NULL
                      AND is_active = TRUE
                    ORDER BY ingestion_timestamp DESC
                    LIMIT 1
                    """
                ),
                {
                    "state_id": state_id,
                    "dataset_type": dataset_type,
                    "procedure_code": procedure_code,
                    "modifier": modifier,
                },
            ).fetchone()

            if active is not None:
                existing_desc = _norm_text(active[1])
                existing_fee = _norm_text(active[2])
                existing_effective = _norm_text(active[3])
                changed = (
                    existing_desc != description
                    or existing_fee != fee_amount
                    or existing_effective != effective_date
                )
                if not changed:
                    unchanged += 1
                    continue

                connection.execute(
                    text(
                        """
                        UPDATE gold_medicaid_rates
                        SET end_date = CURRENT_DATE,
                            is_active = FALSE
                        WHERE id = :id
                        """
                    ),
                    {"id": int(active[0])},
                )
                closed += 1

            connection.execute(
                text(
                    """
                    INSERT INTO gold_medicaid_rates (
                        state_id,
                        state_name,
                        dataset_type,
                        procedure_code,
                        modifier,
                        description,
                        fee_amount,
                        effective_date,
                        end_date,
                        source_url,
                        ingestion_timestamp,
                        agent_version,
                        row_hash,
                        is_active
                    )
                    VALUES (
                        :state_id,
                        :state_name,
                        :dataset_type,
                        :procedure_code,
                        :modifier,
                        :description,
                        :fee_amount,
                        :effective_date,
                        NULL,
                        :source_url,
                        NOW(),
                        :agent_version,
                        :row_hash,
                        TRUE
                    )
                    ON CONFLICT (state_id, dataset_type, procedure_code, modifier, effective_date, row_hash)
                    DO NOTHING
                    """
                ),
                {
                    "state_id": state_id,
                    "state_name": state_name,
                    "dataset_type": dataset_type,
                    "procedure_code": procedure_code,
                    "modifier": modifier,
                    "description": description,
                    "fee_amount": fee_amount,
                    "effective_date": effective_date,
                    "source_url": source_url,
                    "agent_version": PIPELINE_VERSION,
                    "row_hash": row_hash,
                },
            )
            inserted += 1

    return {"inserted": inserted, "closed": closed, "unchanged": unchanged}

CANONICAL_SCHEMA = [
    "procedure_code",
    "modifier",
    "description",
    "fee_amount",
    "effective_date",
    "end_date",
]

# ---------------------------------------------------------------------------
# Master LLM prompts — ALL intelligence lives here, no pattern logic anywhere
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are the Analyst agent for Medicaid fee schedule normalization.

Your job: map every raw column header from a state Medicaid fee schedule table to the canonical schema.

Canonical schema (use ONLY these names):
  procedure_code  — HCPCS, CPT, NDC, or any procedure/service code column
  modifier        — billing modifier codes (1–2 char suffixes)
  description     — text description of the service or procedure
  fee_amount      — the dollar rate, allowable, payment, conversion factor, or WAC
  effective_date  — the date from which the rate/row is valid
  end_date        — the date through which the rate/row is valid (expiry/termination)

Instructions:
- Map every column you can confidently identify.
- A column not matching any canonical field should be omitted entirely.
- Multiple raw columns must NOT map to the same canonical field; pick the best one.
- Your confidence (0–100) represents certainty across the full mapping set.
- Return ONLY strict JSON. No markdown, no prose.

Output format:
{{
  "mappings": [{{"raw_column": "...", "canonical_column": "..."}}],
  "confidence": 0.0
}}
"""

BUSINESS_ANALYST_PROMPT = """You are a senior healthcare reimbursement business analyst specializing in US Medicaid fee schedules across all 50 states.

You understand HCPCS, CPT, NDC, modifier codes, and all standard Medicaid reimbursement terminology.

Your tasks for this invocation:
1. Map every raw column header to the canonical schema.
2. Detect schema drift relative to any prior approved mapping baseline (provided below).
3. Decide whether human review is warranted.
4. If cross-state reference columns are provided, use EXACTLY the same canonical names to stay consistent.

Canonical schema (use ONLY these names as canonical_column):
  procedure_code, modifier, description, fee_amount, effective_date, end_date

Drift severity definitions:
  none   — all columns map to same canonical fields as before, nothing changed
  low    — minor additions or renames that do not affect core fee fields
  medium — a core fee or code field changed mapping or is newly unmapped
  high   — multiple core fields changed or the table structure fundamentally shifted

Human review guidelines:
  needs_human_review = true  when drift is medium or high, or when a critical field
  (procedure_code, fee_amount, effective_date) changes its canonical mapping.

Output strict JSON only — no markdown, no prose:
{{
    "recommended_mappings": [{{"raw_column": "...", "canonical_column": "..."}}],
    "drift_level": "none|low|medium|high",
    "drift_summary": "one sentence explaining what changed",
    "needs_human_review": true|false,
    "confidence": 0.0,
    "naming_rationale": "why these mappings are correct given the context"
}}
"""

LINK_FILTER_PROMPT = """You are a Medicaid data discovery agent.

Given a list of URLs from a state Medicaid agency website, identify which ones are likely to contain downloadable fee schedule or rate data (Excel, CSV, or HTML tables with procedure codes and payment rates).

Exclude:
- Contact pages, FAQ pages, login pages, forms
- Policy manuals, handbooks, bulletins, news articles
- Provider directories, maps, training pages

Include:
- Any URL ending in .xlsx, .xls, or .csv
- Any URL containing words like: fee, rate, schedule, hcpcs, cpt, ndc, crosswalk, billing
- HTML pages that appear to list downloadable fee schedule files

Return strict JSON only:
{{
  "selected_urls": ["url1", "url2", ...],
  "reasoning": "brief explanation of what was kept"
}}
"""

SOURCE_CATEGORIZATION_PROMPT = """You are a Medicaid fee schedule classification agent.

Given the URL of a data file from a state Medicaid agency, determine:
1. The standard Medicaid service category (dataset_type)
2. A short clean source name (snake_case, no year/version tokens)

Standard categories:
  physician, dental, optometry, pharmacy, dmepos, therapy, transportation,
  laboratory, radiology, hospital, mental_health, home_health, hospice,
  rehabilitation, crosswalk, other

Rules:
- crosswalk: URL or filename suggests code mapping/conversion between coding systems
- Return only the best matching category
- source_name must be lowercase snake_case, max 40 chars, no years, no versions

Return strict JSON only:
{{
  "category": "...",
  "source_name": "...",
  "confidence": 0.0
}}

URL: {url}
"""

HEADER_DETECTION_PROMPT = """You are a spreadsheet structure analyst.

You are given the first {n_rows} rows of a spreadsheet (as JSON, row index → list of cell values).
Your task is to identify the row index that contains the real column headers (not title rows, not blank padding, not metadata labels).

A real header row:
- Contains text labels that describe data columns (codes, fees, dates, descriptions)
- Is followed by actual data rows
- Does NOT look like a department name, state agency title, or date annotation

Return strict JSON only:
{{
  "header_row_index": <integer, 0-based>,
  "confidence": 0.0,
  "reasoning": "brief explanation"
}}
"""

SOURCE_QUALIFICATION_PROMPT = """You are a Medicaid fee schedule source qualification agent.

Judge whether the table below is a genuine fee schedule or rate table ready for ingestion.

A genuine fee schedule must:
- Contain procedure or billing codes (HCPCS, CPT, NDC, or similar)
- Contain fee, rate, or payment amount columns
- Represent machine-readable billing data (not a PDF text dump, not a policy document)

Return strict JSON only:
{{
  "is_fee_schedule": true|false,
  "confidence": 0.0,
  "reason": "one sentence"
}}

URL: {url}
Columns: {columns}
Metadata rows above header: {metadata_rows}
Sample rows (first 6): {sample_rows}
"""

SEMANTIC_VALUE_PROMPT = """You are a Medicaid data normalization agent analyzing a tabular dataset.

For each column, determine:
- Whether it is a boolean-style field (e.g., prior authorization required, active indicator)
- If boolean: what token values mean FALSE/NO and what values mean TRUE/YES

Common FALSE tokens for authorization/indicator columns:
  x, x., n, no, false, 0, none, na, n/a, not required, -, not applicable

Common TRUE tokens:
  y, yes, true, 1, required, pa, prior auth required

For non-boolean columns, return empty token lists.

Return strict JSON only:
{{
  "rules": [
    {{
      "column": "column_name",
      "semantic_type": "boolean|non_boolean",
      "false_values": ["...", ...],
      "true_values": ["...", ...],
      "reason": "brief explanation"
    }}
  ]
}}

Columns and sample values:
{payload}
"""

DRIFT_POLICY_PROMPT = """You are a Medicaid data governance agent.

You are evaluating whether a column mapping change requires human review before ingestion.

Pipeline policy: {policy}
  strict   — any medium/high drift or critical field change triggers review
  balanced — only high drift or critical field changes trigger review
  lenient  — only high drift with critical field changes together trigger review

Critical fields (changes here are always high-risk): procedure_code, fee_amount, effective_date

Input:
  Raw columns present today: {raw_columns}
  Previously approved mapping: {approved_mapping}
  Proposed new mapping:        {proposed_mapping}

Evaluate:
- Which canonical fields changed their raw column assignment?
- Are any critical fields affected?
- Under the stated policy, does this warrant human review?

Return strict JSON only:
{{
  "drift_level": "none|low|medium|high",
  "drift_summary": "one sentence",
  "critical_changed_columns": ["..."],
  "force_review": true|false,
  "reason": "one sentence citing the policy rule that was triggered"
}}
"""


def _get_reference_state_context(dataset_type: str, current_state: str) -> str:
    """Build cross-state column context for the LLM to maintain naming consistency."""
    with get_session() as session:
        mappings = session.execute(
            select(CanonicalColumnMapping).where(
                CanonicalColumnMapping.dataset_type == dataset_type
            ).order_by(CanonicalColumnMapping.created_at)
        ).scalars().all()

        if not mappings:
            return ""

        ref_state = mappings[0].reference_state if mappings else None
        context_lines = []

        if ref_state and ref_state != current_state:
            context_lines.append(f"\n## Reference State Columns: {ref_state}")
            ref_mappings = {m.source_column_name: m.canonical_column_name
                            for m in mappings if m.state_name == ref_state}
            for raw, canonical in sorted(ref_mappings.items()):
                context_lines.append(f"  - {raw} → {canonical}")

        other_states = set(m.state_name for m in mappings if m.state_name != ref_state)
        for other_state in sorted(other_states):
            if other_state == current_state:
                continue
            state_mappings = {m.source_column_name: m.canonical_column_name
                              for m in mappings if m.state_name == other_state}
            if state_mappings:
                context_lines.append(f"\n## {other_state.title()} Columns:")
                for raw, canonical in sorted(state_mappings.items()):
                    context_lines.append(f"  - {raw} → {canonical}")

        return "\n".join(context_lines) if context_lines else ""


class MappingItem(BaseModel):
    raw_column: str
    canonical_column: str


class MappingResponse(BaseModel):
    mappings: list[MappingItem] = Field(default_factory=list)
    confidence: float = 0.0


class BusinessAnalystResponse(BaseModel):
    recommended_mappings: list[MappingItem] = Field(default_factory=list)
    drift_level: str = "none"
    drift_summary: str = ""
    needs_human_review: bool = False
    confidence: float = 0.0
    naming_rationale: str = ""


def _safe_json_load(text_value: str) -> dict[str, Any]:
    text_value = text_value.strip()
    if text_value.startswith("```"):
        text_value = text_value.strip("`")
        text_value = text_value.replace("json", "", 1).strip()
    match = re.search(r"\{[\s\S]*\}", text_value)
    payload = match.group(0) if match else text_value
    return json.loads(payload)


# ============ Agent Memory & Handoff Communication ============

def agent_recall_memory(state_name: str, agent_id: str, memory_key: str) -> Any:
    """Recall prior memory for an agent. Returns None if not found."""
    with get_session() as session:
        row = session.execute(
            select(AgentMemory).where(
                and_(
                    AgentMemory.state_name == state_name,
                    AgentMemory.agent_id == agent_id,
                    AgentMemory.memory_key == memory_key,
                )
            )
        ).scalar_one_or_none()
        if row:
            try:
                return json.loads(row.memory_value)
            except (json.JSONDecodeError, TypeError):
                return row.memory_value
        return None


def agent_store_memory(state_name: str, agent_id: str, memory_key: str, memory_value: Any, confidence: float = 1.0) -> None:
    """Store persistent memory for an agent."""
    if isinstance(memory_value, (dict, list)):
        serialized = json.dumps(memory_value)
    else:
        serialized = str(memory_value)
    
    with get_session() as session:
        session.execute(
            text(
                """
                INSERT INTO agent_memory (state_name, agent_id, memory_key, memory_value, confidence, created_at, updated_at)
                VALUES (:state_name, :agent_id, :memory_key, :memory_value, :confidence, NOW(), NOW())
                ON CONFLICT (state_name, agent_id, memory_key)
                DO UPDATE SET
                    memory_value = EXCLUDED.memory_value,
                    confidence = EXCLUDED.confidence,
                    updated_at = NOW()
                """
            ),
            {
                "state_name": state_name,
                "agent_id": agent_id,
                "memory_key": memory_key,
                "memory_value": serialized,
                "confidence": confidence,
            },
        )


def agent_send_handoff(state_name: str, from_agent: str, to_agent: str, message_type: str, message_body: dict, priority: int = 0) -> int:
    """Send a handoff message from one agent to another. Returns handoff record id."""
    with get_session() as session:
        handoff = AgentHandoff(
            state_name=state_name,
            from_agent=from_agent,
            to_agent=to_agent,
            message_type=message_type,
            message_body=json.dumps(message_body),
            priority=priority,
            acknowledged=False,
            created_at=datetime.utcnow(),
        )
        session.add(handoff)
        session.flush()
        handoff_id = handoff.id
    return handoff_id


def agent_receive_handoffs(state_name: str, to_agent: str) -> list[dict]:
    """Receive pending handoff messages for an agent."""
    with get_session() as session:
        rows = session.execute(
            select(AgentHandoff).where(
                and_(
                    AgentHandoff.state_name == state_name,
                    AgentHandoff.to_agent == to_agent,
                    AgentHandoff.acknowledged.is_(False),
                )
            ).order_by(AgentHandoff.priority.desc(), AgentHandoff.created_at.asc())
        ).scalars().all()
        
        result = []
        for row in rows:
            try:
                body = json.loads(row.message_body)
            except json.JSONDecodeError:
                body = {"raw": row.message_body}
            result.append({
                "id": row.id,
                "from_agent": row.from_agent,
                "message_type": row.message_type,
                "body": body,
                "priority": row.priority,
            })
        return result


def agent_acknowledge_handoff(handoff_id: int) -> None:
    """Mark a handoff as acknowledged."""
    with get_session() as session:
        session.execute(
            text(
                """
                UPDATE agent_handoff
                SET acknowledged = TRUE, processed_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": handoff_id},
        )


def agent_bootstrap(state_name: str, agent_id: str, memory_keys: list[str] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load memories and pending handoffs for one agent, then acknowledge the handoffs."""
    memories: dict[str, Any] = {}
    for memory_key in memory_keys or []:
        memories[memory_key] = agent_recall_memory(state_name, agent_id, memory_key)

    handoffs = agent_receive_handoffs(state_name, agent_id)
    for handoff in handoffs:
        agent_acknowledge_handoff(int(handoff["id"]))
    return memories, handoffs



def _discover_links_fallback(home_link: str) -> list[str]:
    with urlopen(home_link, timeout=30) as response:
        html = response.read().decode("utf-8", errors="ignore")
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    return [urljoin(home_link, href) for href in hrefs]


def _relevant_links(links: list[str]) -> list[str]:
    """Use the LLM to decide which URLs are likely fee schedule data sources."""
    if not links:
        return []

    llm_text = _invoke_llm_with_fallback([
        ("system", "Return strict JSON only."),
        ("human", LINK_FILTER_PROMPT + f"\n\nURLs to evaluate:\n{json.dumps(links)}"),
    ])

    if llm_text:
        try:
            parsed = _safe_json_load(llm_text)
            selected = parsed.get("selected_urls", [])
            if isinstance(selected, list) and selected:
                return [u for u in selected if isinstance(u, str) and u.strip()]
        except Exception:
            pass

    # If LLM unavailable, return all non-obviously-binary links so nothing is lost
    skip_ext = (".pdf", ".zip", ".docx", ".doc", ".pptx", ".ppt", ".7z", ".gz", ".rar")
    return [u for u in links if not any(u.lower().endswith(e) for e in skip_ext)]


_SOURCE_NAME_CACHE: dict[str, str] = {}


def _derive_source_name(source_url: str) -> str:
    """Ask the LLM to categorize and name this source URL."""
    if source_url in _SOURCE_NAME_CACHE:
        return _SOURCE_NAME_CACHE[source_url]

    prompt = SOURCE_CATEGORIZATION_PROMPT.format(url=source_url)
    llm_text = _invoke_llm_with_fallback([
        ("system", "Return strict JSON only."),
        ("human", prompt),
    ])

    name = "other"
    if llm_text:
        try:
            parsed = _safe_json_load(llm_text)
            source_name = str(parsed.get("source_name") or parsed.get("category") or "other").strip().lower()
            # Sanitize just in case the LLM returned something odd
            source_name = re.sub(r"[^a-z0-9_]+", "_", source_name).strip("_") or "other"
            name = source_name[:40]
        except Exception:
            pass

    _SOURCE_NAME_CACHE[source_url] = name
    return name


def _sanitize_identifier(value: str, prefix: str = "col") -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")
    if not cleaned:
        cleaned = prefix
    if not cleaned[0].isalpha():
        cleaned = f"{prefix}_{cleaned}"
    return cleaned


def _make_unique_columns(columns: list[Any]) -> list[str]:
    seen: dict[str, int] = {}
    unique_columns: list[str] = []
    for column in columns:
        base_name = str(column).strip() or "unnamed"
        count = seen.get(base_name, 0)
        unique_name = base_name if count == 0 else f"{base_name}_{count + 1}"
        seen[base_name] = count + 1
        unique_columns.append(unique_name)
    return unique_columns


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _build_source_table_name(state_name: str, dataset_type: str) -> str:
    """
    Build source table name with clean state-based pattern.
    Format: {state_name}_{dataset_type}
    Example: alaska_dmepos, arizona_physician, colorado_pharmacy
    """
    clean_state = _sanitize_identifier(state_name, prefix="").strip("_").lower()
    clean_type = _sanitize_identifier(dataset_type, prefix="").strip("_").lower()
    table_name = f"{clean_state}_{clean_type}"
    return table_name[:63]


_SOURCE_SIMILARITY_CACHE: dict[str, str] = {}
_PROVIDER_CHAIN_LOGGED = False


def _upsert_source_metadata(
    state_name: str,
    source_name: str,
    source_url: str,
    extraction_status: str,
) -> tuple[int, str]:
    with get_session() as session:
        row = session.execute(
            select(SourceMetadata).where(
                and_(
                    SourceMetadata.state_name == state_name,
                    SourceMetadata.source_url == source_url,
                )
            )
        ).scalar_one_or_none()

        now = datetime.utcnow()
        source_table_name = _build_source_table_name(state_name, source_name)

        # If URL changed year/version but represents the same semantic source,
        # reconcile to an existing metadata row by source_table_name.
        if row is None:
            row = session.execute(
                select(SourceMetadata).where(
                    and_(
                        SourceMetadata.state_name == state_name,
                        SourceMetadata.source_table_name == source_table_name,
                    )
                )
            ).scalar_one_or_none()

        if row is None:
            row = SourceMetadata(
                state_name=state_name,
                source_name=source_name,
                source_table_name=source_table_name,
                source_url=source_url,
                discovered_at=now,
                last_seen_at=now,
                last_extracted_at=now if extraction_status == "extracted" else None,
                extraction_status=extraction_status,
            )
            session.add(row)
            session.flush()
        else:
            row.source_name = source_name
            row.source_table_name = source_table_name
            row.source_url = source_url
            row.last_seen_at = now
            if extraction_status == "extracted":
                row.last_extracted_at = now
            row.extraction_status = extraction_status
            row.is_active = True

        return int(row.id), source_table_name


@dataclass
class ExcelSheetResult:
    df: pd.DataFrame
    header_row_index: int
    metadata_rows: list[dict[str, Any]] = field(default_factory=list)
    source_url: str = ""
    sheet_name: str = ""


def _detect_header_row_with_llm(raw_df: pd.DataFrame) -> int:
    """Ask the LLM which row index is the real column header."""
    # Build a preview: first min(20, n_rows) rows as a dict
    preview_rows = {}
    scan_limit = min(20, len(raw_df))
    for idx in range(scan_limit):
        row_values = [str(v).strip() for v in raw_df.iloc[idx].tolist() if pd.notna(v) and str(v).strip()]
        preview_rows[idx] = row_values

    prompt = HEADER_DETECTION_PROMPT.format(n_rows=scan_limit) + \
             f"\n\nRow data:\n{json.dumps(preview_rows)}"

    llm_text = _invoke_llm_with_fallback([
        ("system", "Return strict JSON only."),
        ("human", prompt),
    ])

    if llm_text:
        try:
            parsed = _safe_json_load(llm_text)
            idx = int(parsed.get("header_row_index", 0))
            if 0 <= idx < len(raw_df):
                return idx
        except Exception:
            pass

    return 0  # safe default: first row


def _smart_read_excel(url: str) -> ExcelSheetResult | None:
    """
    Read the WHOLE Excel file, use the LLM to detect the real header row in each sheet,
    capture metadata rows above it, and return the best sheet result.
    """
    import io as _io
    try:
        from urllib.request import urlopen as _urlopen
        with _urlopen(url, timeout=60) as resp:
            raw_bytes = resp.read()
    except Exception:
        return None

    try:
        excel_file = pd.ExcelFile(_io.BytesIO(raw_bytes))
    except Exception:
        return None

    best_result: ExcelSheetResult | None = None
    best_row_count = -1

    for sheet_name in excel_file.sheet_names:
        try:
            raw_df = pd.read_excel(
                _io.BytesIO(raw_bytes),
                sheet_name=sheet_name,
                header=None,
                dtype=str,
            )
        except Exception:
            continue

        if raw_df.empty:
            continue

        best_row_idx = _detect_header_row_with_llm(raw_df)

        # Capture metadata rows above the detected header
        metadata_rows: list[dict[str, Any]] = []
        for meta_idx in range(best_row_idx):
            meta_row = raw_df.iloc[meta_idx].tolist()
            non_empty = [str(v).strip() for v in meta_row if pd.notna(v) and str(v).strip()]
            if non_empty:
                metadata_rows.append({"row": meta_idx, "values": non_empty})

        # Build clean DataFrame from header row onward
        header_row = raw_df.iloc[best_row_idx].tolist()
        col_names = [str(v).strip() if pd.notna(v) else "" for v in header_row]
        data_rows = raw_df.iloc[best_row_idx + 1:].reset_index(drop=True)
        data_rows.columns = col_names

        data_rows = data_rows.dropna(how="all")
        header_set = set(c.lower() for c in col_names if c)
        data_rows = data_rows[
            ~data_rows.apply(
                lambda r: set(str(v).strip().lower() for v in r if pd.notna(v)) == header_set,
                axis=1,
            )
        ].reset_index(drop=True)

        if data_rows.empty:
            continue

        if len(data_rows) > best_row_count:
            best_row_count = len(data_rows)
            best_result = ExcelSheetResult(
                df=data_rows,
                header_row_index=best_row_idx,
                metadata_rows=metadata_rows,
                source_url=url,
                sheet_name=str(sheet_name),
            )

    return best_result


def _read_html_tables(url_or_html: str) -> list[pd.DataFrame]:
    """
    Parse HTML tables from a URL or raw HTML string.
    Tries lxml first (faster), then html5lib (more lenient), then Python's html.parser.
    Raises if all parsers fail.
    """
    for flavor in ("lxml", "html5lib", "html.parser"):
        try:
            return pd.read_html(url_or_html, flavor=flavor)
        except ImportError:
            continue  # parser not installed, try next
        except Exception:
            raise  # real parse/network error — propagate
    return []


def _read_any_table(url: str) -> pd.DataFrame | None:
    lower_url = url.lower()
    try:
        if lower_url.endswith(".csv"):
            return pd.read_csv(url)
        if lower_url.endswith(".xlsx") or lower_url.endswith(".xls"):
            result = _smart_read_excel(url)
            return result.df if result is not None else None
        tables = _read_html_tables(url)
        if tables:
            return tables[0]
    except Exception:
        return None
    return None


def _read_excel_full(url: str) -> ExcelSheetResult | None:
    """Public accessor so ExcelInspectorAgent can use the full result."""
    return _smart_read_excel(url)


def _clean_extracted_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Drop noisy columns commonly produced by Excel exports and styling artifacts."""
    if df.empty:
        return df

    cleaned = df.copy()
    cleaned.columns = _make_unique_columns(list(cleaned.columns))

    drop_columns: list[str] = []
    for column in cleaned.columns:
        col_name = str(column).strip()
        col_lower = col_name.lower()

        # Remove autogenerated empty headers and formatting helper columns.
        # Normalise spaces → underscores for comparison so "Conditional Formatting" == "conditional_formatting"
        col_normalised = col_lower.replace(" ", "_")
        _noise_names = {"nan", "none", "null", "conditional_formatting", "na", "n_a"}
        if col_normalised.startswith("unnamed") or col_normalised in _noise_names:
            drop_columns.append(column)
            continue

        # Remove columns that contain only null/blank values.
        series = cleaned[column]
        non_null = series.dropna()
        if non_null.empty:
            drop_columns.append(column)
            continue
        if non_null.astype(str).str.strip().eq("").all():
            drop_columns.append(column)

    if drop_columns:
        cleaned = cleaned.drop(columns=drop_columns, errors="ignore")

    return cleaned


def _study_source_before_ingestion(
    source_url: str,
    df: pd.DataFrame,
    metadata_rows: list[dict[str, Any]],
) -> tuple[bool, str]:
    """Ask the LLM whether this table is a genuine fee schedule ready for ingestion."""
    if df.empty or len(df.columns) < 2:
        return False, "study_failed: insufficient shape"

    columns_preview = [str(c) for c in df.columns]
    preview_rows = df.head(6).fillna("").to_dict(orient="records")
    meta_preview = metadata_rows[:5]

    prompt = SOURCE_QUALIFICATION_PROMPT.format(
        url=source_url,
        columns=json.dumps(columns_preview),
        metadata_rows=json.dumps(meta_preview),
        sample_rows=json.dumps(preview_rows),
    )

    llm_text = _invoke_llm_with_fallback([
        ("system", "Return strict JSON only."),
        ("human", prompt),
    ])

    if llm_text:
        try:
            parsed = _safe_json_load(llm_text)
            is_fee = bool(parsed.get("is_fee_schedule", False))
            confidence = float(parsed.get("confidence", 0.0) or 0.0)
            reason = str(parsed.get("reason", ""))[:240]
            return bool(is_fee and confidence >= 50), reason
        except Exception as exc:
            return False, f"llm_parse_failed: {exc}"

    # LLM unavailable — accept if at least 3 columns present (minimal safe default)
    return len(df.columns) >= 3, "llm_unavailable: accepted by column count"


def _ensure_source_table(table_name: str, mapped_canonical_columns: list[str] | None = None) -> None:
    """Create per-state curated table with mapped canonical columns only.
    
    Args:
        table_name: The curated table name (e.g., alaska_dmepos_curated)
        mapped_canonical_columns: List of canonical column names that exist in source (e.g., ["procedure_code", "fee_amount"])
                                 If None, creates with full canonical schema (backward compat).
    """
    if mapped_canonical_columns is None:
        # Backward compatibility: full schema
        mapped_canonical_columns = CANONICAL_SCHEMA
    
    # Build column definitions only for mapped canonical columns
    column_defs = []
    for canonical in mapped_canonical_columns:
        if canonical in ["procedure_code", "effective_date"]:
            # These are required fields
            column_defs.append(f"{_quote_identifier(canonical)} TEXT NOT NULL DEFAULT ''")
        elif canonical == "modifier":
            column_defs.append(f"{_quote_identifier(canonical)} TEXT NOT NULL DEFAULT ''")
        else:
            # Optional nullable fields
            column_defs.append(f"{_quote_identifier(canonical)} TEXT")
    
    # Add state_id for future multi-state merging
    column_defs.append("state_id INTEGER")
    
    # Always add timestamps
    column_defs.append("created_at TIMESTAMP NOT NULL DEFAULT NOW()")
    column_defs.append("updated_at TIMESTAMP NOT NULL DEFAULT NOW()")
    
    # Build unique constraint on core identifiers if they exist
    unique_cols = []
    if "procedure_code" in mapped_canonical_columns:
        unique_cols.append("procedure_code")
    if "modifier" in mapped_canonical_columns:
        unique_cols.append("modifier")
    if "effective_date" in mapped_canonical_columns:
        unique_cols.append("effective_date")
    
    if unique_cols:
        unique_constraint = f"UNIQUE ({', '.join(_quote_identifier(c) for c in unique_cols)})"
        column_defs.append(unique_constraint)
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} (
            {', '.join(column_defs)}
        )
    """
    with engine.begin() as connection:
        connection.execute(text(create_sql))
        # Ensure state_id column exists for backward compat
        connection.execute(text(f"ALTER TABLE {_quote_identifier(table_name)} ADD COLUMN IF NOT EXISTS state_id INTEGER"))


def _build_curated_table_name(source_table_name: str) -> str:
    return f"{source_table_name[:55]}_curated"[:63]


def _build_raw_column_map(raw_columns: list[str]) -> dict[str, str]:
    used: set[str] = set()
    result: dict[str, str] = {}
    for raw in raw_columns:
        base = _sanitize_identifier(str(raw), prefix="col")[:50]
        candidate = base
        counter = 2
        while candidate in used:
            suffix = f"_{counter}"
            candidate = f"{base[:50-len(suffix)]}{suffix}"
            counter += 1
        used.add(candidate)
        result[str(raw)] = candidate
    return result


def _normalize_semantic_token(value: Any) -> str:
    if value is None:
        return ""
    token = str(value).strip().lower()
    if not token:
        return ""
    return re.sub(r"[^a-z0-9]+", "", token)


def _apply_semantic_value_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """Ask the LLM to define boolean normalization rules for all columns, then apply them."""
    if df.empty:
        return df

    all_cols = [str(c) for c in df.columns]
    payload = []
    for col in all_cols[:80]:
        values = (
            df[col]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .head(30)
            .tolist()
        )
        payload.append({"column": col, "sample_values": values})

    prompt = SEMANTIC_VALUE_PROMPT.format(payload=json.dumps(payload))
    llm_text = _invoke_llm_with_fallback([
        ("system", "Return strict JSON only."),
        ("human", prompt),
    ])

    llm_rules: dict[str, dict[str, set[str]]] = {}
    if llm_text:
        try:
            parsed = _safe_json_load(llm_text)
            for item in parsed.get("rules", []):
                col = str(item.get("column", "")).strip()
                if not col:
                    continue
                fvals = {_normalize_semantic_token(v) for v in item.get("false_values", []) if str(v).strip()}
                tvals = {_normalize_semantic_token(v) for v in item.get("true_values", []) if str(v).strip()}
                llm_rules[col] = {"false": fvals, "true": tvals}
        except Exception:
            pass

    # Ensure every column has an entry
    for col in all_cols:
        llm_rules.setdefault(col, {"false": set(), "true": set()})

    normalized = df.copy()
    for col in normalized.columns:
        col_name = str(col)
        rule = llm_rules.get(col_name, {"false": set(), "true": set()})
        false_values = rule.get("false", set())
        true_values = rule.get("true", set())

        if not false_values and not true_values:
            continue

        def _norm(v: Any, fv: set = false_values, tv: set = true_values) -> Any:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            s = str(v).strip()
            if not s:
                return None
            sl = _normalize_semantic_token(s)
            if sl in fv:
                return "false"
            if sl in tv:
                return "true"
            return s

        normalized[col] = normalized[col].map(_norm)

    return normalized


def _ensure_raw_source_table(table_name: str, raw_column_map: dict[str, str]) -> None:
    """
    Create per-state raw table (state isolation via table name, not columns).
    Table name format: {state_name}_{dataset_type} (e.g., alaska_dmepos).
    No state_id/state_name columns needed - table name provides isolation.
    """
    column_defs = [
        "row_hash TEXT NOT NULL",
    ]
    # Add raw columns
    for sql_col in raw_column_map.values():
        column_defs.append(f"{_quote_identifier(sql_col)} TEXT")
    
    column_defs.append("created_at TIMESTAMP NOT NULL DEFAULT NOW()")
    column_defs.append("updated_at TIMESTAMP NOT NULL DEFAULT NOW()")
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {_quote_identifier(table_name)} (
            id BIGSERIAL PRIMARY KEY,
            {', '.join(column_defs)}
        )
    """
    with engine.begin() as connection:
        connection.execute(text(create_sql))
        # Add raw columns if they don't exist (backward compatibility)
        connection.execute(text(f"ALTER TABLE {_quote_identifier(table_name)} ADD COLUMN IF NOT EXISTS row_hash TEXT"))
        connection.execute(text(f"ALTER TABLE {_quote_identifier(table_name)} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW()"))
        for sql_col in raw_column_map.values():
            connection.execute(text(f"ALTER TABLE {_quote_identifier(table_name)} ADD COLUMN IF NOT EXISTS {_quote_identifier(sql_col)} TEXT"))

        # Create unique index on row_hash only (per-state isolation via table name)
        index_name = _sanitize_identifier(f"{table_name}_rowhash_uidx", prefix="idx")[:63]
        connection.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_identifier(index_name)} "
                f"ON {_quote_identifier(table_name)} (row_hash)"
            )
        )


def _replace_raw_source_rows(table_name: str, df: pd.DataFrame, state_name: str, state_id: int) -> int:
    """
    Upsert rows into per-state raw table (isolated by table name).
    Row uniqueness per row_hash content (no state columns needed).
    
    Args:
        table_name: Per-state raw table name (e.g., 'alaska_dmepos')
        df: Data frame with raw columns
        state_name: State name (for hashing context, not stored)
        state_id: State id (for hashing context, not stored)
    """
    if df.empty:
        return 0

    df = _apply_semantic_value_normalization(df)

    raw_column_map = _build_raw_column_map(list(df.columns))
    _ensure_raw_source_table(table_name, raw_column_map)

    sql_columns = [raw_column_map[str(col)] for col in df.columns]
    # No state columns in insert (table name provides isolation)
    quoted_insert_cols = ", ".join(
        [_quote_identifier(col) for col in sql_columns] + 
        ["row_hash", "created_at", "updated_at"]
    )
    placeholders = ", ".join(
        [f":{col}" for col in sql_columns] + 
        [":row_hash", "NOW()", "NOW()"]
    )
    update_assignments = ", ".join(
        [f"{_quote_identifier(col)} = EXCLUDED.{_quote_identifier(col)}" for col in sql_columns]
        + ["updated_at = NOW()"]
    )
    insert_sql = text(
        f"""
        INSERT INTO {_quote_identifier(table_name)}
        ({quoted_insert_cols})
        VALUES ({placeholders})
        ON CONFLICT (row_hash)
        DO UPDATE SET {update_assignments}
        """
    )

    rows = df.where(pd.notnull(df), None).to_dict(orient="records")

    seen_hashes: list[str] = []
    with engine.begin() as connection:
        for row in rows:
            params = {}
            for raw_col, sql_col in raw_column_map.items():
                value = row.get(raw_col)
                params[sql_col] = None if value is None else str(value)
            # Hash content only (state already isolated by table name)
            hash_payload = "|".join(
                [str(params.get(sql_col) or "") for sql_col in sql_columns]
            )
            row_hash = hashlib.sha256(hash_payload.encode("utf-8")).hexdigest()
            params["row_hash"] = row_hash
            seen_hashes.append(row_hash)
            connection.execute(insert_sql, params)

        # Remove rows no longer present in latest snapshot (all rows in table are from this state)
        if seen_hashes:
            delete_params = {f"h{i}": h for i, h in enumerate(seen_hashes)}
            placeholders = ", ".join([f":h{i}" for i in range(len(seen_hashes))])
            connection.execute(
                text(
                    f"DELETE FROM {_quote_identifier(table_name)} "
                    f"WHERE row_hash IS NOT NULL AND row_hash NOT IN ({placeholders})"
                ),
                delete_params,
            )

    return len(rows)


def _get_table_columns(table_name: str) -> list[str]:
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                ORDER BY ordinal_position
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return [str(row[0]) for row in rows]


def _load_approved_mapping(state_name: str, source_url: str) -> dict[str, str]:
    with get_session() as session:
        rows = session.execute(
            select(MappingColumn).where(
                and_(
                    MappingColumn.state_name == state_name,
                    MappingColumn.source_url == source_url,
                    MappingColumn.approved.is_(True),
                )
            )
        ).scalars()
        return {row.raw_column: row.canonical_column for row in rows}


def _llm_drift_and_policy(
    raw_columns: list[str],
    approved_mapping: dict[str, str],
    proposed_mapping: dict[str, str],
    llm_needs_review: bool,
) -> dict[str, Any]:
    """Ask the LLM to evaluate drift and decide whether human review is needed."""
    policy = os.getenv("DRIFT_POLICY", "strict").strip().lower()

    prompt = DRIFT_POLICY_PROMPT.format(
        policy=policy,
        raw_columns=json.dumps(raw_columns),
        approved_mapping=json.dumps(approved_mapping),
        proposed_mapping=json.dumps(proposed_mapping),
    )
    if llm_needs_review:
        prompt += "\nNote: The business analyst also flagged needs_human_review=true."

    llm_text = _invoke_llm_with_fallback([
        ("system", "Return strict JSON only."),
        ("human", prompt),
    ])

    if llm_text:
        try:
            parsed = _safe_json_load(llm_text)
            force_review_raw = parsed.get("force_review", llm_needs_review)
            if isinstance(force_review_raw, bool):
                force_review = force_review_raw
            elif isinstance(force_review_raw, (int, float)):
                force_review = bool(force_review_raw)
            elif isinstance(force_review_raw, str):
                force_review = force_review_raw.strip().lower() in {"1", "true", "yes", "y"}
            else:
                force_review = bool(llm_needs_review)
            return {
                "drift_level": str(parsed.get("drift_level", "none")),
                "drift_summary": str(parsed.get("drift_summary", "")),
                "critical_changed_columns": list(parsed.get("critical_changed_columns", [])),
                "force_review": force_review,
                "reason": str(parsed.get("reason", "")),
                "policy": policy,
            }
        except Exception:
            pass

    # If LLM is unavailable, fall back to safe default: escalate to review if any prior mapping exists
    return {
        "drift_level": "none" if not approved_mapping else "low",
        "drift_summary": "llm_unavailable",
        "critical_changed_columns": [],
        "force_review": llm_needs_review,
        "reason": "llm_unavailable: defaulted to safe policy",
        "policy": policy,
    }


def _is_truthy_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _provider_is_available(provider: str) -> bool:
    provider = provider.strip().lower()
    if provider == "bedrock_agent":
        return (
            _is_truthy_env(os.getenv("USE_BEDROCK_AGENT"))
            and bool(os.getenv("BEDROCK_AGENT_ID", "").strip())
            and bool(os.getenv("BEDROCK_AGENT_ALIAS_ID", "").strip())
        )
    if provider == "gemini":
        return bool(os.getenv("GEMINI_API_KEY", "").strip())
    if provider == "openai":
        return bool(os.getenv("OPENAI_API_KEY", "").strip())
    if provider == "groq":
        return bool(os.getenv("GROQ_API_KEY", "").strip())
    if provider == "ollama":
        return True
    if provider == "bedrock":
        return _is_truthy_env(os.getenv("USE_BEDROCK")) or os.getenv("LLM_PROVIDER", "").strip().lower() == "bedrock"
    return False


def _resolved_primary_provider() -> str:
    primary = os.getenv("LLM_PROVIDER", "").strip().lower()
    if primary and primary != "none":
        return primary
    if _provider_is_available("bedrock_agent"):
        return "bedrock_agent"
    if _is_truthy_env(os.getenv("USE_BEDROCK")):
        return "bedrock"
    return ""


def _log_provider_chain_once(chain: list[str]) -> None:
    global _PROVIDER_CHAIN_LOGGED
    if _PROVIDER_CHAIN_LOGGED:
        return
    _PROVIDER_CHAIN_LOGGED = True
    primary = _resolved_primary_provider() or "auto"
    LOGGER.info(f"[LLM] Provider mode primary={primary}; resolved_chain={chain}")


def _build_llm(provider: str, model: str = "") -> Any | None:
    """Construct a single LangChain chat model for the given provider. Returns None on failure."""
    try:
        if provider == "gemini":
            key = os.getenv("GEMINI_API_KEY", "").strip()
            if not key:
                return None
            from langchain_google_genai import ChatGoogleGenerativeAI  # type: ignore
            return ChatGoogleGenerativeAI(
                model=model or "gemini-2.5-pro",
                google_api_key=key,
                temperature=0,
            )

        if provider == "openai":
            key = os.getenv("OPENAI_API_KEY", "").strip()
            if not key:
                return None
            from langchain_openai import ChatOpenAI  # type: ignore
            return ChatOpenAI(
                model=model or "gpt-4.1",
                api_key=key,
                temperature=0,
            )

        if provider == "groq":
            key = os.getenv("GROQ_API_KEY", "").strip()
            if not key:
                return None
            from langchain_groq import ChatGroq  # type: ignore
            return ChatGroq(
                model=model or "llama-3.3-70b-versatile",
                api_key=key,
                temperature=0,
            )

        if provider == "ollama":
            from langchain_ollama import ChatOllama  # type: ignore
            base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
            return ChatOllama(
                model=model or "llama3.3",
                temperature=0,
                base_url=base_url,
            )

        if provider == "bedrock":
            if not _provider_is_available("bedrock"):
                return None
            from langchain_aws import ChatBedrock  # type: ignore
            return ChatBedrock(
                model_id=model or os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-7-sonnet-20250219-v1:0"),
                region_name=os.getenv("AWS_REGION", "us-east-1"),
                model_kwargs={"temperature": 0},
            )
    except Exception as exc:
        print(f"[LLM] Failed to build {provider}: {type(exc).__name__}: {exc}")
        return None
    return None


# Provider priority order — primary first, then fallbacks that have keys configured.
_ALL_PROVIDERS = ["bedrock_agent", "groq", "gemini", "openai", "ollama", "bedrock"]

# Per-provider model env var names and hardcoded defaults.
_PROVIDER_MODEL_DEFAULTS: dict[str, tuple[str, str]] = {
    "groq":   ("GROQ_MODEL",   "llama-3.3-70b-versatile"),
    "gemini": ("GEMINI_MODEL", "gemini-2.5-pro"),
    "openai": ("OPENAI_MODEL", "gpt-4.1"),
    "ollama": ("OLLAMA_MODEL", "llama3.3"),
}


def _get_model_for_provider(provider: str, is_primary: bool) -> str:
    """
    Resolve the model string for a provider.
    - Primary provider: LLM_MODEL env var wins (backward compat), then provider-specific env var, then default.
    - Fallback providers: provider-specific env var then default (never inherit primary's model name).
    """
    if is_primary:
        explicit = os.getenv("LLM_MODEL", "").strip()
        if explicit:
            return explicit
    if provider in _PROVIDER_MODEL_DEFAULTS:
        env_key, default = _PROVIDER_MODEL_DEFAULTS[provider]
        return os.getenv(env_key, "").strip() or default
    return ""


def _messages_to_agent_text(messages: list[tuple[str, str]]) -> str:
    lines: list[str] = []
    for role, content in messages:
        role_label = str(role or "user").strip().upper()
        lines.append(f"[{role_label}] {content}")
    return "\n\n".join(lines)


def _invoke_bedrock_agent_runtime(messages: list[tuple[str, str]]) -> str | None:
    if not _provider_is_available("bedrock_agent"):
        return None

    agent_id = os.getenv("BEDROCK_AGENT_ID", "").strip()
    agent_alias_id = os.getenv("BEDROCK_AGENT_ALIAS_ID", "").strip()
    region = os.getenv("AWS_REGION", "us-east-1").strip() or "us-east-1"
    session_id = os.getenv("BEDROCK_AGENT_SESSION_ID", "sentinel-session").strip() or "sentinel-session"
    prompt_text = _messages_to_agent_text(messages)

    try:
        import boto3  # type: ignore

        client = boto3.client("bedrock-agent-runtime", region_name=region)
        response = client.invoke_agent(
            agentId=agent_id,
            agentAliasId=agent_alias_id,
            sessionId=session_id,
            inputText=prompt_text,
        )

        chunks: list[str] = []
        for event in response.get("completion", []) or []:
            chunk = event.get("chunk") if isinstance(event, dict) else None
            if not chunk:
                continue
            raw_bytes = chunk.get("bytes") if isinstance(chunk, dict) else None
            if not raw_bytes:
                continue
            if isinstance(raw_bytes, bytes):
                chunks.append(raw_bytes.decode("utf-8", errors="ignore"))
            else:
                chunks.append(str(raw_bytes))

        text_value = "".join(chunks).strip()
        return text_value or None
    except Exception as exc:
        print(f"[LLM fallback] bedrock_agent failed: {type(exc).__name__}: {exc}")
        return None


def _get_provider_chain() -> list[str]:
    """
    Return ordered list of providers to try: primary first, then fallbacks.
    Order can be customised with LLM_FALLBACK_CHAIN=groq,gemini,openai (comma-separated).
    Only providers that have their API keys configured are included.
    """
    primary = _resolved_primary_provider()
    chain: list[str] = []
    if primary and primary != "none":
        chain.append(primary)

    # Respect a user-supplied fallback order if present.
    fallback_env = os.getenv("LLM_FALLBACK_CHAIN", "").strip()
    candidates = (
        [p.strip().lower() for p in fallback_env.split(",") if p.strip()]
        if fallback_env
        else _ALL_PROVIDERS
    )
    for p in candidates:
        if p not in chain and _provider_is_available(p):
            chain.append(p)

    # Last resort: ollama needs no key and can always be tried.
    if not chain and _provider_is_available("ollama"):
        chain.append("ollama")
    return chain


def _invoke_llm_with_fallback(messages: list[tuple[str, str]]) -> str | None:
    """
    Try the primary provider, then auto-fallback to the next configured provider
    on rate-limit / auth / network errors. Returns the raw text response or None.

    Model resolution per provider:
    - Primary: LLM_MODEL env var → GROQ_MODEL/GEMINI_MODEL/OPENAI_MODEL → hardcoded default
    - Fallbacks: GROQ_MODEL/GEMINI_MODEL/OPENAI_MODEL → hardcoded default (never inherits primary model)
    """
    chain = _get_provider_chain()
    _log_provider_chain_once(chain)

    if not chain:
        LOGGER.warning("[LLM] No configured providers available")
        return None

    for provider in chain:
        is_primary = provider == chain[0]

        if provider == "bedrock_agent":
            text_value = _invoke_bedrock_agent_runtime(messages)
            if text_value:
                print("[LLM] Success via bedrock_agent")
                return text_value
            continue

        model = _get_model_for_provider(provider, is_primary)
        llm = _build_llm(provider, model)
        if llm is None:
            continue
        try:
            response = llm.invoke(messages)
            content = response.content
            text_value = (
                "\n".join(
                    item.get("text", "") if isinstance(item, dict) else str(item)
                    for item in content
                )
                if isinstance(content, list)
                else str(content)
            )
            if text_value.strip():
                if not is_primary:
                    LOGGER.info(f"[LLM] Fell back to {provider} (model={model})")
                print(f"[LLM] Success via {provider} (model={model})")
                return text_value
        except Exception as exc:
            # Log so the user can see which provider failed and why.
            print(f"[LLM fallback] {provider} failed: {type(exc).__name__}: {exc}")
            continue

    print("[LLM] All providers exhausted — no LLM response available")
    return None


def _get_reference_state(dataset_type: str) -> str | None:
    """Get the first state that loaded this dataset_type (the reference standard for column names)."""
    with get_session() as session:
        first_mapping = session.execute(
            select(CanonicalColumnMapping.reference_state)
            .where(CanonicalColumnMapping.dataset_type == dataset_type)
            .order_by(CanonicalColumnMapping.created_at)
            .limit(1)
        ).scalar_one_or_none()
        return first_mapping


def _get_canonical_column_mapping(dataset_type: str, state_name: str, raw_column: str, reference_state: str | None = None) -> str | None:
    """
    Look up the canonical column name for a raw column from the cross-state mapping table.
    Returns None if no prior mapping exists — the LLM will decide during analysis.
    """
    with get_session() as session:
        existing = session.execute(
            select(CanonicalColumnMapping.canonical_column_name).where(
                and_(
                    CanonicalColumnMapping.dataset_type == dataset_type,
                    CanonicalColumnMapping.state_name == state_name,
                    CanonicalColumnMapping.source_column_name == raw_column,
                )
            )
        ).scalar_one_or_none()
        return existing


def _upsert_canonical_column_mapping(
    dataset_type: str,
    state_name: str,
    raw_column: str,
    canonical_column: str,
    confidence: float = 0.9,
) -> None:
    """Record the cross-state column mapping for future states."""
    # Determine reference state (first state for this dataset_type, or this state if first).
    reference_state = _get_reference_state(dataset_type) or state_name

    with get_session() as session:
        from sqlalchemy import insert, update
        # Upsert with conflict handling
        upsert_sql = text(
            """
            INSERT INTO canonical_column_mapping 
            (dataset_type, reference_state, state_name, source_column_name, canonical_column_name, confidence, created_at, updated_at)
            VALUES (:dataset_type, :reference_state, :state_name, :source_column_name, :canonical_column_name, :confidence, NOW(), NOW())
            ON CONFLICT (dataset_type, reference_state, state_name, source_column_name) DO UPDATE SET
                canonical_column_name = EXCLUDED.canonical_column_name,
                confidence = EXCLUDED.confidence,
                updated_at = NOW()
            """
        )
        session.execute(
            upsert_sql,
            {
                "dataset_type": dataset_type,
                "reference_state": reference_state,
                "state_name": state_name,
                "source_column_name": raw_column,
                "canonical_column_name": canonical_column,
                "confidence": confidence,
            },
        )
        session.commit()


def _save_mapping_details(
    state_name: str,
    source_name: str,
    source_url: str,
    mapping_dict: dict[str, str],
    confidence: float,
    approved: bool,
    rationale: str,
) -> None:
    """
    Save column mapping details to mapping_column table using UPSERT semantics.
    
    Uses ON CONFLICT (state_name, source_url, raw_column) DO UPDATE to ensure:
    - New mappings are inserted
    - Existing mappings (same state/source/raw_column) are updated (not duplicated)
    - Timestamps are updated to reflect latest mapping change
    
    Args:
        state_name: State abbreviation (e.g., 'alaska')
        source_name: Human-readable source name (e.g., 'fee_schedule_physician')
        source_url: URL where data was discovered
        mapping_dict: Dict[raw_column_name, canonical_column_name]
        confidence: AI confidence score (0-100) for mapping quality
        approved: Whether mapping was approved by analyst (confidence >= threshold)
        rationale: Business rationale for these mappings
    
    Note:
        - Idempotent: running twice with same input produces same DB state
        - Migration-safe: includes constraint migration for older DBs
        - Transactional: all mappings committed together or none
    """
    if not mapping_dict:
        # No mappings to save - this is valid for empty sources
        return
    
    now = datetime.utcnow()
    upserted_count = 0
    
    with get_session() as session:
        try:
            # Ensure expected unique key exists for ON CONFLICT upsert even on older DBs.
            session.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conname = 'uq_mapping_column_state_source_raw'
                        ) THEN
                            ALTER TABLE mapping_column
                            DROP CONSTRAINT IF EXISTS uq_mapping_column_state_source_raw_canonical;
                            ALTER TABLE mapping_column
                            ADD CONSTRAINT uq_mapping_column_state_source_raw
                            UNIQUE (state_name, source_url, raw_column);
                        END IF;
                    END $$;
                    """
                )
            )

            upsert_sql = text(
                """
                INSERT INTO mapping_column (
                    state_name,
                    source_name,
                    source_url,
                    raw_column,
                    canonical_column,
                    confidence,
                    rationale,
                    approved,
                    created_at,
                    updated_at
                )
                VALUES (
                    :state_name,
                    :source_name,
                    :source_url,
                    :raw_column,
                    :canonical_column,
                    :confidence,
                    :rationale,
                    :approved,
                    :created_at,
                    :updated_at
                )
                ON CONFLICT (state_name, source_url, raw_column)
                DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    canonical_column = EXCLUDED.canonical_column,
                    confidence = EXCLUDED.confidence,
                    rationale = EXCLUDED.rationale,
                    approved = EXCLUDED.approved,
                    updated_at = EXCLUDED.updated_at
                """
            )

            for raw_column, canonical_column in mapping_dict.items():
                session.execute(
                    upsert_sql,
                    {
                        "state_name": state_name,
                        "source_name": source_name,
                        "source_url": source_url,
                        "raw_column": raw_column,
                        "canonical_column": canonical_column,
                        "confidence": confidence,
                        "rationale": rationale,
                        "approved": approved,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                upserted_count += 1
            
            session.commit()
            LOGGER.info(
                f"Saved mapping details: state={state_name}, source={source_name}, "
                f"mappings={upserted_count}, confidence={confidence}, approved={approved}"
            )
        
        except Exception as e:
            LOGGER.error(f"Failed to save mapping details for {state_name}/{source_url}: {e}")
            raise


def _dedupe_canonical_mapping(mapping_dict: dict[str, str], raw_columns: list[str]) -> dict[str, str]:
    """Ensure one raw column maps to one canonical field (no duplicate canonical targets)."""
    raw_order = {raw: index for index, raw in enumerate(raw_columns)}
    chosen: dict[str, str] = {}
    canonical_used: set[str] = set()

    for raw, canonical in sorted(mapping_dict.items(), key=lambda item: raw_order.get(item[0], 10**9)):
        if canonical in canonical_used:
            continue
        chosen[raw] = canonical
        canonical_used.add(canonical)

    return chosen


def _bedrock_mapping(raw_columns: list[str]) -> MappingResponse | None:
    messages = [
        ("system", SYSTEM_PROMPT),
        ("human", "Raw columns: " + json.dumps(raw_columns) + "\nMap to canonical schema and return JSON."),
    ]
    text_value = _invoke_llm_with_fallback(messages)
    if text_value is None:
        return None
    try:
        parsed = _safe_json_load(text_value)
        mapped = MappingResponse.model_validate(parsed)
        if mapped.confidence <= 1.0:
            mapped.confidence = mapped.confidence * 100.0
        return mapped
    except (ValidationError, json.JSONDecodeError, Exception):
        return None


def navigator_node(state: dict[str, Any]) -> dict[str, Any]:
    _checkpoint_state_snapshot(state, "navigator", "start")
    home_link = state["state_home_link"]
    state_name = state["state_name"]
    state_id = int(state["state_id"])

    # Recall prior link discovery if available (agent memory)
    prior_links = agent_recall_memory(state_name, "navigator", "discovered_links")
    if prior_links:
        LOGGER.info(f"[Navigator] Recalling {len(prior_links)} prior discovered links for {state_name}")
        candidate_links = prior_links
    else:
        try:
            links = _discover_links_fallback(home_link)
        except Exception:
            links = []

        candidate_links = _relevant_links([home_link] + links)
        # Store discovered links in agent memory for future runs
        agent_store_memory(state_name, "navigator", "discovered_links", candidate_links, confidence=0.95)
        LOGGER.info(f"[Navigator] Discovered {len(candidate_links)} new links for {state_name}")

    # Send handoff to extractor with candidate links
    handoff_id = agent_send_handoff(
        state_name=state_name,
        from_agent="navigator",
        to_agent="extractor",
        message_type="LINKS_READY",
        message_body={
            "candidate_links": candidate_links,
            "source": "memory_recall" if prior_links else "fresh_discovery",
        },
        priority=0,
    )

    state["candidate_links"] = candidate_links
    state["status"] = "navigated"
    state["log"] = state.get("log", []) + [
        f"Navigator found {len(candidate_links)} links for {state_name} (handoff_id={handoff_id})"
    ]
    _checkpoint_state_snapshot(state, "navigator", "end")
    return state


@dataclass
class SourceInspectionResult:
    """Result produced by one SourceInspectorAgent worker thread."""
    link: str
    source_name: str
    source_table_name: str
    source_metadata_id: int
    df: pd.DataFrame
    header_row_index: int
    metadata_rows: list[dict[str, Any]]
    sheet_name: str
    is_fee_source: bool
    qualification_reason: str
    rows_loaded: int
    schema_added: list[str]
    schema_removed: list[str]
    error: str | None = None


def _source_inspector_agent(
    link: str,
    state_name: str,
    state_id: int,
) -> SourceInspectionResult:
    """
    SourceInspectorAgent – one agent per link, runs in its own thread.

    Responsibilities:
      1. Download + smart-read the whole file (all sheets, detect real header row).
      2. Capture metadata rows above the header (state info, effective dates, etc.).
      3. Clean the DataFrame (drop unnamed/empty columns).
      4. Classify whether this is a real fee schedule source.
      5. If qualified: write metadata row, compare schema vs previous, materialize raw table.
      6. Return a self-contained SourceInspectionResult.
    """
    source_name = _derive_source_name(link)
    lower_url = link.lower()

    # Skip binary formats we cannot read as tables.
    _SKIP_EXTENSIONS = (".pdf", ".zip", ".docx", ".doc", ".pptx", ".ppt", ".7z", ".gz", ".rar")
    if any(lower_url.endswith(ext) for ext in _SKIP_EXTENSIONS):
        return SourceInspectionResult(
            link=link, source_name=source_name, source_table_name="",
            source_metadata_id=-1, df=pd.DataFrame(),
            header_row_index=0, metadata_rows=[], sheet_name="",
            is_fee_source=False, qualification_reason="unsupported_format",
            rows_loaded=0, schema_added=[], schema_removed=[],
        )

    # --- Step 1: Read the full file intelligently ---
    excel_result: ExcelSheetResult | None = None
    df: pd.DataFrame | None = None
    header_row_index = 0
    metadata_rows: list[dict[str, Any]] = []
    sheet_name = ""

    try:
        if lower_url.endswith(".xlsx") or lower_url.endswith(".xls"):
            excel_result = _smart_read_excel(link)
            if excel_result is not None:
                df = excel_result.df
                header_row_index = excel_result.header_row_index
                metadata_rows = excel_result.metadata_rows
                sheet_name = excel_result.sheet_name
        elif lower_url.endswith(".csv"):
            df = pd.read_csv(link)
        else:
            tables = _read_html_tables(link)
            if tables:
                df = tables[0]
    except Exception as exc:
        return SourceInspectionResult(
            link=link, source_name=source_name, source_table_name="",
            source_metadata_id=-1, df=pd.DataFrame(),
            header_row_index=0, metadata_rows=[], sheet_name="",
            is_fee_source=False, qualification_reason="read_error",
            rows_loaded=0, schema_added=[], schema_removed=[],
            error=str(exc),
        )

    if df is None or df.empty:
        return SourceInspectionResult(
            link=link, source_name=source_name, source_table_name="",
            source_metadata_id=-1, df=pd.DataFrame(),
            header_row_index=0, metadata_rows=[], sheet_name="",
            is_fee_source=False, qualification_reason="empty_or_unreadable",
            rows_loaded=0, schema_added=[], schema_removed=[],
        )

    # --- Step 2: Clean columns ---
    df = _clean_extracted_dataframe(df)
    if df.empty or len(df.columns) == 0:
        return SourceInspectionResult(
            link=link, source_name=source_name, source_table_name="",
            source_metadata_id=-1, df=pd.DataFrame(),
            header_row_index=0, metadata_rows=[], sheet_name="",
            is_fee_source=False, qualification_reason="no_columns_after_clean",
            rows_loaded=0, schema_added=[], schema_removed=[],
        )

    # --- Step 3: Study source deeply before ingestion ---
    is_fee_source, qualification_reason = _study_source_before_ingestion(
        source_url=link,
        df=df,
        metadata_rows=metadata_rows,
    )

    if not is_fee_source:
        # Do NOT write non-fee sources to the database at all.
        return SourceInspectionResult(
            link=link, source_name=source_name,
            source_table_name="",
            source_metadata_id=-1, df=df,
            header_row_index=header_row_index, metadata_rows=metadata_rows,
            sheet_name=sheet_name,
            is_fee_source=False, qualification_reason=qualification_reason,
            rows_loaded=0, schema_added=[], schema_removed=[],
        )

    # Only qualified fee schedule sources get a DB row.
    source_metadata_id, source_table_name = _upsert_source_metadata(
        state_name=state_name,
        source_name=source_name,
        source_url=link,
        extraction_status="extracted",
    )

    # --- Step 4: Compare schema with previous raw table ---
    prev_columns = _get_table_columns(source_table_name)
    rows_loaded = _replace_raw_source_rows(source_table_name, df, state_name, state_id)
    next_columns = _get_table_columns(source_table_name)

    schema_added = sorted(list(set(next_columns) - set(prev_columns)))
    schema_removed = sorted(list(set(prev_columns) - set(next_columns)))

    return SourceInspectionResult(
        link=link, source_name=source_name,
        source_table_name=source_table_name,
        source_metadata_id=source_metadata_id, df=df,
        header_row_index=header_row_index, metadata_rows=metadata_rows,
        sheet_name=sheet_name,
        is_fee_source=True, qualification_reason=qualification_reason,
        rows_loaded=rows_loaded,
        schema_added=schema_added,
        schema_removed=schema_removed,
    )


def extractor_node(state: dict[str, Any]) -> dict[str, Any]:
    """
    Dispatches one SourceInspectorAgent per candidate link using a thread pool,
    collects results, selects the primary (largest) fee-schedule source, and
    populates pipeline state for downstream nodes.
    """
    _checkpoint_state_snapshot(state, "extractor", "start")
    candidate_links = state.get("candidate_links", [])
    state_name = state["state_name"]
    state_id = int(state["state_id"])
    max_workers = int(os.getenv("EXTRACTOR_THREADS", "6"))

    _, handoffs = agent_bootstrap(state_name, "extractor", memory_keys=["last_primary_source", "last_qualified_sources"])
    for handoff in handoffs:
        if handoff["message_type"] == "LINKS_READY":
            LOGGER.info(f"[Extractor] Received handoff from navigator: {handoff['body'].get('source', 'unknown')}")

    logs: list[str] = []
    results: list[SourceInspectionResult] = []

    # --- Parallel dispatch: one SourceInspectorAgent thread per link ---
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_source_inspector_agent, link, state_name, state_id): link
            for link in candidate_links
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                link = futures[future]
                logs.append(f"Inspector agent error for {link}: {exc}")

    # --- Aggregate results ---
    qualified: list[SourceInspectionResult] = []
    for r in results:
        if r.error:
            logs.append(f"Agent error [{r.link}]: {r.error}")
        elif not r.is_fee_source:
            logs.append(f"Skipped [{r.source_name}] {r.qualification_reason}")
        else:
            logs.append(
                f"Loaded [{r.source_name}] sheet={r.sheet_name or 'N/A'} "
                f"header_row={r.header_row_index} "
                f"rows={r.rows_loaded} "
                f"meta_rows_above={len(r.metadata_rows)} "
                f"schema_added={len(r.schema_added)} "
                f"schema_removed={len(r.schema_removed)} "
                f"({r.qualification_reason})"
            )
            if r.schema_added or r.schema_removed:
                logs.append(
                    f"  Schema change [{r.source_table_name}]: "
                    f"+{r.schema_added} -{r.schema_removed}"
                )
            qualified.append(r)

    if not qualified:
        state["status"] = "failed"
        state["log"] = state.get("log", []) + logs + [
            "Extractor found no qualified fee schedule sources"
        ]
        _checkpoint_state_snapshot(state, "extractor", "end")
        return state

    # --- Pick primary source (largest row count) ---
    primary = max(qualified, key=lambda r: len(r.df))

    state["source_url"] = primary.link
    state["source_name"] = primary.source_name
    state["primary_source_table_name"] = primary.source_table_name
    state["primary_source_metadata_id"] = primary.source_metadata_id
    state["raw_columns"] = list(primary.df.columns)
    state["raw_records"] = primary.df.to_dict(orient="records")
    state["source_metadata_rows"] = primary.metadata_rows
    state["status"] = "extracted"
    
    # Log for debugging
    LOGGER.info(f"[Extractor] Primary source: {primary.source_name} → table_name={primary.source_table_name}")

    qualified_summary = [
        {
            "link": result.link,
            "source_name": result.source_name,
            "rows_loaded": result.rows_loaded,
            "qualification_reason": result.qualification_reason,
        }
        for result in qualified
    ]
    agent_store_memory(state_name, "extractor", "last_primary_source", {
        "source_url": primary.link,
        "source_name": primary.source_name,
        "row_count": len(primary.df),
        "column_count": len(primary.df.columns),
    }, confidence=0.98)
    agent_store_memory(state_name, "extractor", "last_qualified_sources", qualified_summary, confidence=0.9)
    handoff_id = agent_send_handoff(
        state_name=state_name,
        from_agent="extractor",
        to_agent="business_analyst",
        message_type="SOURCE_PROFILE_READY",
        message_body={
            "source_url": primary.link,
            "source_name": primary.source_name,
            "raw_columns": list(primary.df.columns),
            "row_count": len(primary.df),
            "metadata_rows": primary.metadata_rows[:5],
        },
        priority=1,
    )
    state["log"] = state.get("log", []) + logs + [
        f"Primary source: {primary.source_name} "
        f"({len(primary.df)} rows, {len(primary.df.columns)} columns, "
        f"header_row={primary.header_row_index})",
        f"Total qualified sources extracted: {len(qualified)}",
        f"Extractor sent business_analyst handoff_id={handoff_id}",
    ]
    _checkpoint_state_snapshot(state, "extractor", "end")
    return state


def business_analyst_node(state: dict[str, Any]) -> dict[str, Any]:
    _checkpoint_state_snapshot(state, "business_analyst", "start")
    raw_columns = state.get("raw_columns", [])
    state_name = state.get("state_name", "")
    source_url = state.get("source_url", "")
    file_metadata_rows = state.get("source_metadata_rows", [])
    dataset_type = state.get("source_name", "")

    memories, handoffs = agent_bootstrap(
        state_name,
        "business_analyst",
        memory_keys=["last_drift_report", "last_recommendations", "last_source_profile"],
    )
    for handoff in handoffs:
        if handoff["message_type"] == "SOURCE_PROFILE_READY":
            LOGGER.info(f"[BusinessAnalyst] Received source profile for {handoff['body'].get('source_name', 'unknown')}")

    approved_mapping = _load_approved_mapping(state_name, source_url)

    # Build cross-state context for naming consistency
    ref_context = _get_reference_state_context(dataset_type, state_name) if dataset_type else ""

    # Fire both LLM calls in parallel: business analyst + column mapping
    from concurrent.futures import ThreadPoolExecutor as _TP, as_completed as _ac

    llm_result: BusinessAnalystResponse | None = None
    mapping_result: MappingResponse | None = None

    def _run_ba():
        human_message = (
            "Raw headers: " + json.dumps(raw_columns) +
            "\nPrior approved mappings: " + json.dumps(approved_mapping) +
            "\nFile metadata rows above header: " + json.dumps(file_metadata_rows[:5])
        )
        if ref_context:
            human_message += "\n" + ref_context
        text_value = _invoke_llm_with_fallback([
            ("system", BUSINESS_ANALYST_PROMPT),
            ("human", human_message),
        ])
        if text_value is None:
            return None
        try:
            parsed = _safe_json_load(text_value)
            obj = BusinessAnalystResponse.model_validate(parsed)
            if obj.confidence <= 1.0:
                obj.confidence = obj.confidence * 100.0
            return obj
        except Exception:
            return None

    with _TP(max_workers=2) as pool:
        ba_future = pool.submit(_run_ba)
        map_future = pool.submit(_bedrock_mapping, raw_columns)
        for future in _ac([ba_future, map_future]):
            if future is ba_future:
                llm_result = future.result()
            else:
                mapping_result = future.result()

    # Build recommendations entirely from LLM output — no keyword hints
    recommendations: dict[str, str] = {
        raw: canonical for raw, canonical in approved_mapping.items() if raw in raw_columns
    }

    if llm_result is not None:
        for item in llm_result.recommended_mappings:
            if item.raw_column in raw_columns and item.raw_column not in recommendations:
                recommendations[item.raw_column] = item.canonical_column

    recommendations = _dedupe_canonical_mapping(recommendations, raw_columns)

    llm_needs_review = llm_result.needs_human_review if llm_result is not None else False
    policy_eval = _llm_drift_and_policy(
        raw_columns=raw_columns,
        approved_mapping=approved_mapping,
        proposed_mapping=recommendations,
        llm_needs_review=llm_needs_review,
    )
    force_human_review = bool(policy_eval["force_review"])

    analyst_hint_confidence = float(llm_result.confidence) if llm_result is not None else 0.0

    drift_level = llm_result.drift_level if llm_result is not None else policy_eval["drift_level"]
    drift_summary = llm_result.drift_summary if llm_result is not None else policy_eval["drift_summary"]

    state["business_requirements"] = {
        "canonical_schema": CANONICAL_SCHEMA,
        "recommended_mappings": recommendations,
        "approved_mapping_baseline": approved_mapping,
        "drift_report": policy_eval,
        "force_human_review": force_human_review,
        "drift_policy": policy_eval["policy"],
        "drift_policy_reason": policy_eval["reason"],
        "critical_changed_columns": policy_eval["critical_changed_columns"],
        "business_confidence": analyst_hint_confidence,
        "llm_drift_level": drift_level,
        "llm_drift_summary": drift_summary,
        "note": "Pure LLM business analyst guidance applied — no keyword patterns used",
    }
    agent_store_memory(state_name, "business_analyst", "last_drift_report", policy_eval, confidence=0.92)
    agent_store_memory(state_name, "business_analyst", "last_recommendations", recommendations, confidence=0.92)
    agent_store_memory(
        state_name,
        "business_analyst",
        "last_source_profile",
        {
            "source_url": source_url,
            "raw_columns": raw_columns,
            "metadata_rows": file_metadata_rows[:5],
            "previous_drift_report": memories.get("last_drift_report"),
        },
        confidence=0.85,
    )
    if mapping_result is not None:
        state["_prefetched_mapping"] = {
            "mappings": [{"raw_column": m.raw_column, "canonical_column": m.canonical_column} for m in mapping_result.mappings],
            "confidence": mapping_result.confidence,
        }
    handoff_id = agent_send_handoff(
        state_name=state_name,
        from_agent="business_analyst",
        to_agent="analyst",
        message_type="BUSINESS_RULES_READY",
        message_body={
            "recommended_mappings": recommendations,
            "drift_report": policy_eval,
            "force_human_review": force_human_review,
            "business_confidence": analyst_hint_confidence,
        },
        priority=1,
    )
    state["force_human_review"] = force_human_review
    state["status"] = "business_analyzed"
    state["log"] = state.get("log", []) + [
        (
            f"Business Analyst proposed {len(recommendations)} mappings; "
            f"drift={drift_level}; "
            f"force_review={force_human_review}; "
            f"policy={policy_eval['policy']}; handoff_id={handoff_id}"
        )
    ]
    _checkpoint_state_snapshot(state, "business_analyst", "end")
    return state


def analyst_node(state: dict[str, Any]) -> dict[str, Any]:
    _checkpoint_state_snapshot(state, "analyst", "start")
    raw_columns = state.get("raw_columns", [])
    raw_records = state.get("raw_records", [])
    source_url = state.get("source_url", "")
    state_name = state.get("state_name", "")

    memories, handoffs = agent_bootstrap(
        state_name,
        "analyst",
        memory_keys=["last_mapping_dict", "last_approved_decision", "last_confidence"],
    )
    for handoff in handoffs:
        if handoff["message_type"] == "BUSINESS_RULES_READY":
            LOGGER.info(f"[Analyst] Received business rules with force_review={handoff['body'].get('force_human_review')}")

    approved_memory = state.get("business_requirements", {}).get("approved_mapping_baseline", {})
    if not approved_memory:
        approved_memory = _load_approved_mapping(state.get("state_name", ""), source_url)
    if not approved_memory and isinstance(memories.get("last_mapping_dict"), dict):
        approved_memory = memories["last_mapping_dict"]
    business_hints = state.get("business_requirements", {}).get("recommended_mappings", {})

    # Reuse the LLM mapping result prefetched in business_analyst_node (parallel call).
    prefetched = state.pop("_prefetched_mapping", None)
    if prefetched is not None:
        mapping_response = MappingResponse.model_validate(prefetched)
    else:
        mapping_response = _bedrock_mapping(raw_columns)
        if mapping_response is None:
            # Return zero-confidence empty response — do not fall back to pattern matching
            mapping_response = MappingResponse(mappings=[], confidence=0.0)

    mapping_dict = {raw: canon for raw, canon in approved_memory.items() if raw in raw_columns}
    for raw, canon in business_hints.items():
        if raw in raw_columns and raw not in mapping_dict:
            mapping_dict[raw] = canon
    for item in mapping_response.mappings:
        if item.raw_column in raw_columns and item.raw_column not in mapping_dict:
            mapping_dict[item.raw_column] = item.canonical_column

    mapping_dict = _dedupe_canonical_mapping(mapping_dict, raw_columns)

    raw_df = pd.DataFrame(raw_records)
    if raw_df.empty:
        # If no records, create empty DataFrame with only the mapped canonical columns
        mapped_canonicals = sorted(list(set(mapping_dict.values())))
        standardized = pd.DataFrame(columns=mapped_canonicals)
    else:
        # Rename raw columns to their mapped canonical names
        standardized = raw_df.rename(columns=mapping_dict)
        # Keep ONLY the canonical columns that were actually mapped (not all CANONICAL_SCHEMA)
        mapped_canonicals = sorted(list(set(mapping_dict.values())))
        # Select only columns that exist in standardized and are in our mapped set
        columns_to_keep = [c for c in mapped_canonicals if c in standardized.columns]
        standardized = standardized[columns_to_keep].copy()

    # Add required derived fields from workflow context for canonical Silver/Gold compatibility.
    standardized["state"] = state_name
    standardized["dataset_type"] = state.get("source_name", "")
    if "end_date" not in standardized.columns:
        standardized["end_date"] = None

    state["column_mappings"] = mapping_dict
    # Blend analyst confidence with business analyst confidence so deterministic hints are not underweighted.
    business_conf = float(state.get("business_requirements", {}).get("business_confidence", 0.0))
    state["analyst_confidence"] = round(max(float(mapping_response.confidence), business_conf), 2)
    state["standardized_records"] = standardized.to_dict(orient="records")

    threshold = float(os.getenv("ANALYST_CONFIDENCE_THRESHOLD", "85"))
    approved = bool(state["analyst_confidence"] >= threshold and not state.get("force_human_review", False))
    rationale = (
        "Business analyst + analyst mapping for Medicaid fee schedule; "
        f"drift={state.get('business_requirements', {}).get('llm_drift_level', 'unknown')}; "
        f"force_review={state.get('force_human_review', False)}"
    )
    _save_mapping_details(
        state_name=state_name,
        source_name=state.get("source_name", ""),
        source_url=source_url,
        mapping_dict=mapping_dict,
        confidence=state["analyst_confidence"],
        approved=approved,
        rationale=rationale,
    )

    # Also record in cross-state canonical mapping table so future states use same naming
    dataset_type = state.get("source_name", "")
    if dataset_type and approved:
        for raw_col, canonical_col in mapping_dict.items():
            try:
                _upsert_canonical_column_mapping(
                    dataset_type=dataset_type,
                    state_name=state_name,
                    raw_column=raw_col,
                    canonical_column=canonical_col,
                    confidence=state["analyst_confidence"] / 100.0,
                )
            except Exception as exc:
                LOGGER.warning(f"Failed to record canonical mapping for {raw_col}: {exc}")

    agent_store_memory(state_name, "analyst", "last_mapping_dict", mapping_dict, confidence=state["analyst_confidence"] / 100.0)
    agent_store_memory(state_name, "analyst", "last_approved_decision", {"approved": approved, "force_human_review": state.get("force_human_review", False)}, confidence=1.0)
    agent_store_memory(state_name, "analyst", "last_confidence", state["analyst_confidence"], confidence=1.0)

    threshold = float(os.getenv("ANALYST_CONFIDENCE_THRESHOLD", "85"))
    next_agent = "archivist"
    message_type = "CURATION_READY"
    priority = 0
    if bool(state.get("force_human_review", False)):
        block_on_review = os.getenv("BLOCK_ON_REVIEW", "true").strip().lower() == "true"
        next_agent = "auto_reject" if block_on_review else "human_review"
        message_type = "REVIEW_REQUIRED"
        priority = 2
    elif float(state.get("analyst_confidence", 0.0)) < threshold:
        next_agent = "human_review"
        message_type = "LOW_CONFIDENCE_REVIEW"
        priority = 1

    if next_agent in {"human_review", "auto_reject"}:
        _publish_hitl_alert(
            state_name=state_name,
            source_url=source_url,
            confidence=float(state.get("analyst_confidence", 0.0)),
            reason=f"next_agent={next_agent}, message_type={message_type}",
        )

    handoff_id = agent_send_handoff(
        state_name=state_name,
        from_agent="analyst",
        to_agent=next_agent,
        message_type=message_type,
        message_body={
            "mapping_dict": mapping_dict,
            "confidence": state["analyst_confidence"],
            "approved": approved,
            "source_url": source_url,
        },
        priority=priority,
    )

    state["status"] = "analyzed"
    state["log"] = state.get("log", []) + [
        (
            f"Analyst mapped {len(mapping_dict)} columns ({', '.join(sorted(set(mapping_dict.values())))}) "
            f"with {state['analyst_confidence']}% confidence; "
            f"drift={state.get('business_requirements', {}).get('llm_drift_level', 'unknown')}; handoff_id={handoff_id}"
        )
    ]
    _checkpoint_state_snapshot(state, "analyst", "end")
    return state


def archivist_node(state: dict[str, Any]) -> dict[str, Any]:
    _checkpoint_state_snapshot(state, "archivist", "start")
    state_name = state.get("state_name", "")
    state_id = state.get("state_id")
    memories, handoffs = agent_bootstrap(
        state_name,
        "archivist",
        memory_keys=["last_archive_summary", "last_inserted_rows"],
    )
    for handoff in handoffs:
        if handoff["message_type"] == "CURATION_READY":
            LOGGER.info(f"[Archivist] Received curation handoff with confidence={handoff['body'].get('confidence')}")

    source_table_name = state.get("primary_source_table_name")

    # Defensive: if source_table_name is missing, reconstruct from source_name
    if not source_table_name:
        source_name = state.get("source_name", "")
        if source_name:
            source_table_name = _build_source_table_name(state_name, source_name)
            LOGGER.info(f"[Archivist] Reconstructed source_table_name from source_name: {source_table_name}")
        
    if not source_table_name:
        state["status"] = "failed"
        state["log"] = state.get("log", []) + ["No source table resolved for archivist"]
        return state

    curated_table_name = _build_curated_table_name(source_table_name)

    # Extract canonical columns that were mapped from source plus derived canonical fields.
    column_mappings = state.get("column_mappings", {})
    mapped_canonical_columns = sorted(list(set(column_mappings.values())))
    for derived_col in ["state", "dataset_type", "end_date", "state_id"]:
        if derived_col not in mapped_canonical_columns:
            mapped_canonical_columns.append(derived_col)

    if not mapped_canonical_columns:
        state["status"] = "failed"
        state["log"] = state.get("log", []) + ["No mapped canonical columns available for insertion"]
        return state
    
    # Create table with ONLY the mapped canonical columns
    _ensure_source_table(curated_table_name, mapped_canonical_columns=mapped_canonical_columns)

    # Build INSERT and ON CONFLICT dynamically based on actual mapped columns
    insert_columns = mapped_canonical_columns + ["created_at", "updated_at"]
    placeholders = ", ".join([f":{col}" for col in mapped_canonical_columns] + ["NOW()", "NOW()"])
    insert_col_list = ", ".join([_quote_identifier(col) for col in insert_columns])

    # Build ON CONFLICT clause if we have core identifier columns
    conflict_cols = [c for c in ["procedure_code", "modifier", "effective_date"] if c in mapped_canonical_columns]
    if conflict_cols:
        update_columns = [c for c in mapped_canonical_columns if c not in conflict_cols]
        if update_columns:
            update_sql = ",\n            ".join(
                f"{_quote_identifier(c)} = EXCLUDED.{_quote_identifier(c)}" for c in update_columns
            )
            update_sql = update_sql + ",\n            updated_at = NOW()"
        else:
            update_sql = "updated_at = NOW()"
        conflict_clause = f"""
        ON CONFLICT ({', '.join(_quote_identifier(c) for c in conflict_cols)})
        DO UPDATE SET
            {update_sql}
        """
    else:
        conflict_clause = ""

    insert_sql = text(
        f"""
        INSERT INTO {_quote_identifier(curated_table_name)}
        ({insert_col_list})
        VALUES ({placeholders})
        {conflict_clause}
        """
    )

    inserted = 0
    with engine.begin() as connection:
        for record in state.get("standardized_records", []):
            # Build parameter dict with only mapped canonical columns
            params = {}
            for canonical_col in mapped_canonical_columns:
                if canonical_col == "state_id":
                    # Always use state_id from pipeline state, not from record
                    params[canonical_col] = state_id
                else:
                    value = record.get(canonical_col)
                    # Handle empty string defaults for specific fields
                    if canonical_col in ["modifier", "effective_date"] and value is None:
                        params[canonical_col] = ""
                    elif value is None:
                        params[canonical_col] = None
                    else:
                        params[canonical_col] = str(value)
            
            connection.execute(insert_sql, params)
            inserted += 1

    state["inserted_rows"] = inserted
    state["inserted_canonical_columns"] = mapped_canonical_columns
    state["status"] = "archived"
    agent_store_memory(
        state_name,
        "archivist",
        "last_archive_summary",
        {
            "source_table_name": source_table_name,
            "curated_table_name": curated_table_name,
            "mapped_columns": mapped_canonical_columns,
            "inserted_rows": inserted,
            "previous_inserted_rows": memories.get("last_inserted_rows"),
        },
        confidence=0.98,
    )
    agent_store_memory(state_name, "archivist", "last_inserted_rows", inserted, confidence=1.0)
    state["log"] = state.get("log", []) + [
        (
            f"Archivist created curated table {curated_table_name} with "
            f"{len(mapped_canonical_columns)} mapped columns: {mapped_canonical_columns}"
        )
    ]

    bronze_uri = _write_bronze_snapshot(state)
    silver_uri = _write_silver_dataset(state)
    if bronze_uri:
        state["bronze_uri"] = bronze_uri
        state["log"] = state.get("log", []) + [f"Bronze artifact saved: {bronze_uri}"]
    if silver_uri:
        state["silver_uri"] = silver_uri
        state["log"] = state.get("log", []) + [f"Silver artifact saved: {silver_uri}"]

    gold_stats = _upsert_gold_scd2_records(state)
    state["gold_stats"] = gold_stats
    state["log"] = state.get("log", []) + [
        f"Gold SCD2 upsert: inserted={gold_stats['inserted']}, closed={gold_stats['closed']}, unchanged={gold_stats['unchanged']}"
    ]

    state["log"] = state.get("log", []) + [f"Archivist upserted {inserted} rows"]
    _checkpoint_state_snapshot(state, "archivist", "end")
    return state

import os
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from agents import (
    agent_bootstrap,
    agent_store_memory,
    analyst_node,
    archivist_node,
    business_analyst_node,
    extractor_node,
    navigator_node,
)


class PipelineState(TypedDict, total=False):
    state_id: int
    state_name: str
    state_home_link: str
    candidate_links: list[str]
    source_name: str
    source_url: str
    primary_source_table_name: str
    primary_source_metadata_id: int
    raw_columns: list[str]
    raw_records: list[dict[str, Any]]
    business_requirements: dict[str, Any]
    force_human_review: bool
    column_mappings: dict[str, str]
    analyst_confidence: float
    standardized_records: list[dict[str, Any]]
    inserted_rows: int
    inserted_canonical_columns: list[str]
    gold_stats: dict[str, int]
    status: str
    log: list[str]
    source_metadata_rows: list[dict[str, Any]]


def route_after_analyst(state: PipelineState) -> str:
    if bool(state.get("force_human_review", False)):
        block_on_review = os.getenv("BLOCK_ON_REVIEW", "true").strip().lower() == "true"
        return "auto_reject" if block_on_review else "human_review"
    threshold = float(os.getenv("ANALYST_CONFIDENCE_THRESHOLD", "85"))
    confidence = float(state.get("analyst_confidence", 0.0))
    return "human_review" if confidence < threshold else "archivist"


def human_review_node(state: PipelineState) -> PipelineState:
    state_name = str(state.get("state_name", ""))
    _, handoffs = agent_bootstrap(state_name, "human_review", memory_keys=["last_review_reason"])
    latest_reason = "Human review required because analyst confidence is below threshold"
    for handoff in handoffs:
        if handoff["message_type"] in {"REVIEW_REQUIRED", "LOW_CONFIDENCE_REVIEW"}:
            latest_reason = f"Human review required from analyst handoff: {handoff['message_type']}"
    agent_store_memory(state_name, "human_review", "last_review_reason", latest_reason, confidence=1.0)
    state["status"] = "human_review"
    state["log"] = state.get("log", []) + [
        latest_reason
    ]
    return state


def auto_reject_node(state: PipelineState) -> PipelineState:
    state_name = str(state.get("state_name", ""))
    _, handoffs = agent_bootstrap(state_name, "auto_reject", memory_keys=["last_reject_reason"])
    reason = state.get("business_requirements", {}).get("drift_policy_reason", "forced review policy")
    for handoff in handoffs:
        if handoff["message_type"] == "REVIEW_REQUIRED":
            reason = f"Analyst requested auto reject: {handoff['body'].get('confidence')} confidence"
    agent_store_memory(state_name, "auto_reject", "last_reject_reason", reason, confidence=1.0)
    state["status"] = "rejected"
    state["log"] = state.get("log", []) + [f"Auto-rejected before load: {reason}"]
    return state


def build_graph():
    workflow = StateGraph(PipelineState)

    workflow.add_node("navigator", navigator_node)
    workflow.add_node("extractor", extractor_node)
    workflow.add_node("business_analyst", business_analyst_node)
    workflow.add_node("analyst", analyst_node)
    workflow.add_node("archivist", archivist_node)
    workflow.add_node("human_review", human_review_node)
    workflow.add_node("auto_reject", auto_reject_node)

    workflow.set_entry_point("navigator")
    workflow.add_edge("navigator", "extractor")
    workflow.add_edge("extractor", "business_analyst")
    workflow.add_edge("business_analyst", "analyst")
    workflow.add_conditional_edges(
        "analyst",
        route_after_analyst,
        {
            "archivist": "archivist",
            "human_review": "human_review",
            "auto_reject": "auto_reject",
        },
    )
    workflow.add_edge("archivist", END)
    workflow.add_edge("human_review", END)
    workflow.add_edge("auto_reject", END)

    return workflow.compile()

"""
Navigator Agent — Runner
-------------------------
Dual-mode entry point for the Navigator Agent.

**CLI mode** (development / testing)::

    python -m agents.navigator.run                               # Alaska (default)
    python -m agents.navigator.run alaska
    python -m agents.navigator.run --url https://example.gov/fees
    python -m agents.navigator.run --url https://... --category physician

**AgentCore mode** (production / AWS deployment)::

    # In your AgentCore app.py:
    from bedrock_agentcore import BedrockAgentCoreApp
    from agents.navigator.run import agentcore_invoke

    app = BedrockAgentCoreApp()

    @app.entrypoint
    def handler(payload, context=None):
        return agentcore_invoke(payload)

    if __name__ == "__main__":
        app.run()
"""

from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file if present

import argparse
import json
import logging
import os
import sys
from typing import Any
from bedrock_agentcore.runtime import BedrockAgentCoreApp

# Ensure the project root (new/) is on sys.path so absolute imports work
# regardless of how this script is invoked.
_PROJECT_ROOT = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from agents.navigator.agent import invoke, run_navigator  # noqa: E402
from agents.navigator.models import NavigatorInput  # noqa: E402


app = BedrockAgentCoreApp()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Pre-configured state portals for quick CLI testing
STATE_PORTALS: dict[str, str] = {
    "alaska": "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html",
    "florida": "https://ahca.myflorida.com/medicaid/cost-reimbursement-and-auditing",
}


# ──────────────────────────────────────────────────────────────────────────────
# AgentCore-compatible entrypoint
# ──────────────────────────────────────────────────────────────────────────────

@app.entrypoint
def agentcore_invoke(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Thin wrapper around ``agents.navigator.agent.invoke`` for use with
    ``BedrockAgentCoreApp``.

    Usage::

        @app.entrypoint
        def handler(payload, context=None):
            return agentcore_invoke(payload)

    Args:
        payload: Dict with at minimum ``portal_url`` or ``prompt``.

    Returns:
        dict — full ``NavigatorOutput`` as a JSON-safe dict.
    """
    return invoke(payload)


# ──────────────────────────────────────────────────────────────────────────────
# CLI mode
# ──────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments.

    Supports two modes:
      1. Preset state key:  ``python -m agents.navigator.run alaska``
      2. Raw URL:           ``python -m agents.navigator.run --url https://...``

    Returns:
        Namespace with 'state', 'url', 'category'.
    """
    parser = argparse.ArgumentParser(
        description="Navigator Agent — CLI Runner")
    parser.add_argument(
        "state",
        nargs="?",
        default=None,
        help=f"Pre-configured state key. Available: {list(STATE_PORTALS.keys())}",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Arbitrary portal URL (overrides the state key).",
    )
    parser.add_argument(
        "--category",
        type=str,
        default="all",
        help="Dataset category filter (default: 'all').",
    )
    return parser.parse_args()


# def main() -> None:
    """
    CLI entry point — run the Navigator Agent for a state Medicaid portal.

    Supports two modes:
      1. Pre-configured state:  ``python -m agents.navigator.run alaska``
      2. Arbitrary URL:         ``python -m agents.navigator.run --url https://...``

    If neither is given, defaults to Alaska.

    Exit codes:
        0 — success  |  1 — unknown state key or invalid arguments
    """
    args = _parse_args()

    # Resolve portal URL  —  portal_url is the only required input
    if args.url:
        portal_url = args.url
        output_key = "custom"
    else:
        state_key = (args.state or "alaska").lower()
        if state_key not in STATE_PORTALS:
            logger.error("Unknown state: %s. Available: %s",
                         state_key, list(STATE_PORTALS.keys()))
            sys.exit(1)
        portal_url = STATE_PORTALS[state_key]
        output_key = state_key

    # Construct input — only portal_url is required
    input_data = NavigatorInput(
        portal_url=portal_url,
        dataset_category=args.category,
    )

    logger.info("=" * 80)
    logger.info("NAVIGATOR AGENT — %s", input_data.portal_url)
    logger.info("Category: %s", input_data.dataset_category)
    logger.info("=" * 80)

    # ── Execute ─────────────────────────────────────────────────────────────
    result = run_navigator(input_data)

    # ── Summary ────────────────────────────────────────────────────────────
    result_label = result.state_name or result.portal_url
    status_icon = "✅" if result.success else "❌"
    print(f"\n{'=' * 80}")
    print(f"{status_icon} RESULTS: {result_label} Medicaid Fee Schedule Discovery")
    print(f"Portal Type : {result.portal_type}")
    print(f"Pages Crawled: {len(result.crawled_pages)}")
    print(f"Total Links  : {result.total_links_discovered}")
    print(f"Relevant     : {len(result.relevant_datasets)}")
    print(f"{'=' * 80}")

    if not result.success:
        print("\n❌  Agent execution failed.")
    elif not result.relevant_datasets:
        print("\n⚠️  No relevant Medicaid fee schedule datasets were found.")

    for i, ds in enumerate(result.relevant_datasets, 1):
        tag = "✅ CURRENT" if ds.is_current else "📦 ARCHIVED"
        print(
            f"\n#{i} [{ds.relevance_score:.2f}] [{ds.category.value.upper()}] {tag}")
        print(f"   Title : {ds.title}")
        print(f"   URL   : {ds.url}")
        print(f"   Type  : {ds.file_type}")
        print(f"   Date  : {ds.estimated_date or 'N/A'}")
        print(f"   Reason: {ds.relevance_reason}")

    if result.errors:
        print(f"\n⚠️  Errors ({len(result.errors)}):")
        for err in result.errors:
            print(f"   - {err}")

    # ── Persist JSON ──────────────────────────────────────────────────────────
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir, f"{output_key}_navigator_result.json")

    with open(output_path, "w") as fh:
        json.dump(result.model_dump(), fh, indent=2, default=str)

    print(f"\n📄 Full JSON saved to: {output_path}")


if __name__ == "__main__":
    app.run()

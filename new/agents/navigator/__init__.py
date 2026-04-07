"""agents.navigator
----------------
Strands-powered Navigator Agent (“The Researcher”) that crawls state
Medicaid portals, discovers downloadable fee-schedule datasets, and
ranks them by relevance.

**AWS AgentCore compatible** — use ``invoke(payload)`` as the
stateless entrypoint.
"""

from .agent import create_navigator_agent, invoke, run_navigator
from .models import NavigatorInput, NavigatorOutput, RankedDataset

__all__ = [
    "create_navigator_agent",
    "invoke",
    "run_navigator",
    "NavigatorInput",
    "NavigatorOutput",
    "RankedDataset",
]

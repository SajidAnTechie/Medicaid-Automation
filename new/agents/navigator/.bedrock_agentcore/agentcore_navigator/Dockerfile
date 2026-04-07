FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim
WORKDIR /app

# All environment variables in one layer
ENV UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_PROGRESS=1 \
    PYTHONUNBUFFERED=1 \
    DOCKER_CONTAINER=1 \
    AWS_REGION=eu-north-1 \
    AWS_DEFAULT_REGION=eu-north-1



COPY requirements.txt requirements.txt
# Install from requirements file
RUN uv pip install -r requirements.txt

# NOTE: No local Chromium install needed — crawl_portal connects to
# AgentCore's managed cloud Browser service via CDP (connect_over_cdp).
# Playwright is installed as a Python package only (for the CDP client API).

RUN uv pip install aws-opentelemetry-distro==0.12.2


# Signal that this is running in Docker for host binding logic
ENV DOCKER_CONTAINER=1

# Create non-root user
RUN useradd -m -u 1000 bedrock_agentcore

# ── Preserve Python package structure ─────────────────────────────────
# The build context is the navigator/ directory.  Copy it into
# /app/agents/navigator/ so "from agents.navigator.…" imports work
# (including relative imports inside agent.py and __init__.py).
RUN mkdir -p /app/agents && touch /app/agents/__init__.py
COPY --chown=bedrock_agentcore:bedrock_agentcore . /app/agents/navigator/

USER bedrock_agentcore

EXPOSE 9000
EXPOSE 8000
EXPOSE 8080

CMD ["opentelemetry-instrument", "python", "-m", "agents.navigator.agentcore_navigator"]

# URL Discovery Lambda

This Lambda discovers Medicaid fee schedule URLs from a given state page.

## What it does

1. Validates input `state_url`.
2. Downloads HTML and builds website metadata.
3. Uses LLM to summarize the page and decide relevance for fee schedule use case.
4. If relevant, extracts candidate links and asks LLM to select fee-schedule URLs.
5. Returns discovered URLs with reasoning and metadata.

## Request

**Method**: `POST`

**Lambda local endpoint**:

`http://localhost:9000/2015-03-31/functions/function/invocations`

**Body**:

```json
{
  "state_url": "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html"
}
```

## Run locally with Docker

From repository root:

```bash
docker compose -f docker-compose.yml -f docker-compose.lambda.local.yml up --build db lambda-url-discovery-local
```

## Required environment variables

The Lambda reads configuration from environment variables (typically from `.env` + compose override).

### Core required envs

| Variable | Required | Description |
|---|---|---|
| `LLM_PROVIDER` | Yes | LLM backend to use. Allowed values: `ollama`, `bedrock`. |
| `OLLAMA_MODEL` | Yes when `LLM_PROVIDER=ollama` | Ollama model name (example: `llama3.2`). |
| `BEDROCK_MODEL` | Yes when `LLM_PROVIDER=bedrock` | Bedrock model ID (example: `anthropic.claude-3-5-sonnet-20241022-v2:0`). |

### Optional / recommended envs

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama endpoint URL. For Docker + host/network Ollama, use reachable host/IP. |
| `AWS_REGION` | `us-east-1` | AWS region for Bedrock runtime client. |
| `URL_DISCOVERY_CHUNK_SIZE` | `25` | Number of candidate URLs sent to the LLM per chunk. |
| `URL_DISCOVERY_MAX_CANDIDATE_LINKS` | `250` | Max extracted candidate links from HTML before chunking. |
| `URL_DISCOVERY_MAX_LINKS` | `100` | Max final discovered URLs returned by Lambda. |
| `HTML_SUMMARY_MAX_CHARS` | `12000` | Max extracted plain text chars used for LLM relevance summary. |

## LLM setup

### Option 1: Ollama

Set envs:

```dotenv
LLM_PROVIDER=ollama
OLLAMA_MODEL=llama3.2
OLLAMA_BASE_URL=http://<ollama-host>:11434
```

Notes:
- If Lambda runs in Docker and Ollama runs on another machine, set `OLLAMA_BASE_URL` to that machine IP/hostname.
- Ensure the selected `OLLAMA_MODEL` is already pulled/available in the Ollama server.

### Option 2: AWS Bedrock

Set envs:

```dotenv
LLM_PROVIDER=bedrock
BEDROCK_MODEL=anthropic.claude-3-5-sonnet-20241022-v2:0
AWS_REGION=us-east-1
```

Notes:
- The runtime environment/container must have AWS credentials with `bedrock:InvokeModel` permissions.
- The model ID must be available in the configured AWS account/region.

### Required Bedrock configuration checklist

1. **Enable model access in Bedrock console**
  - In the target AWS account and `AWS_REGION`, request/enable access to the model used in `BEDROCK_MODEL`.

2. **Provide AWS credentials to the Lambda runtime**
  - **In AWS Lambda**: attach an execution role.
  - **In local Docker**: pass credentials using one of these approaches:
    - mount `~/.aws` into container and set `AWS_PROFILE`, or
    - set env vars: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`.

3. **IAM permissions (minimum)**
  - `bedrock:InvokeModel`
  - `bedrock:ListFoundationModels` (optional but useful for diagnostics)

4. **Region/model alignment**
  - `AWS_REGION` must match a region where the selected `BEDROCK_MODEL` is available and enabled for your account.

### Example local Bedrock envs

```dotenv
LLM_PROVIDER=bedrock
BEDROCK_MODEL=anthropic.claude-3-5-sonnet-20241022-v2:0
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
# AWS_SESSION_TOKEN=...   # only if using temporary credentials
# AWS_PROFILE=default     # if using shared credentials file
```

## LLM flow used by this Lambda

1. LLM relevance pass: summarize HTML and classify whether page is relevant for fee schedule discovery.
2. LLM extraction pass: classify chunked candidate URLs and return `selected_urls` + `reasoning`.
3. No fallback mode: if no qualifying URL is returned, Lambda raises `No URL found based on the requirement`.

## Testing approach (Thunder Client / Postman)

### Option A: Thunder Client
- Method: `POST`
- URL: `http://localhost:9000/2015-03-31/functions/function/invocations`
- Body type: JSON
- Body:

```json
{
  "state_url": "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html"
}
```

### Option B: Postman
- Method: `POST`
- URL: `http://localhost:9000/2015-03-31/functions/function/invocations`
- Header: `Content-Type: application/json`
- Body → `raw` → `JSON`:

```json
{
  "state_url": "https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html"
}
```

## Expected response fields

- `state_url`
- `discovered_urls`
- `reasoning_data`
- `website_metadata`
- `html_relevance`

## Notes

- If no qualifying URLs are found, the function raises: `No URL found based on the requirement`.
- If HTML is not relevant for the use case, it raises a relevance validation error.
- LLM provider is controlled by environment variables (`LLM_PROVIDER`, `OLLAMA_MODEL`/`BEDROCK_MODEL`, etc.).

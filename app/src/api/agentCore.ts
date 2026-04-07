import type { AgentName } from "../types";
import { AGENTS } from "../types";
import { AGENT_ENDPOINTS } from "./endpoints";

/**
 * Call an AgentCore endpoint with JWT auth, timeout, and auto-retry on 401.
 */
export async function callAgentCore(
    agentName: AgentName,
    payload: object,
    token: string,
    onTokenRefresh?: () => Promise<string | null>,
): Promise<unknown> {
    const arn = AGENT_ENDPOINTS[agentName];

    if (!arn) {
        // Dev-mode stub — return a mock success after a short delay
        return devStub(agentName);
    }

    const timeoutMs = AGENTS[agentName].timeoutMs;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    const encodedArn = encodeURIComponent(arn);
    const url = "https://bedrock-agentcore.eu-north-1.amazonaws.com/runtimes/" + encodedArn + "/invocations";
    try {
        const response = await fetch(url, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${token}`,
            },
            body: JSON.stringify(payload),
            signal: controller.signal,
        });
        clearTimeout(timer);

        // 401 → try one silent token refresh
        if (response.status === 401 && onTokenRefresh) {
            const newToken = await onTokenRefresh();
            if (newToken) {
                return callAgentCore(agentName, payload, newToken);
            }
            throw new Error("Authentication expired. Please log in again.");
        }

        if (!response.ok) {
            const body = await response.text().catch(() => "");
            throw new Error(
                `Agent ${agentName} returned HTTP ${response.status}: ${body}`,
            );
        }

        return await response.json();
    } catch (err: unknown) {
        clearTimeout(timer);
        if (err instanceof DOMException && err.name === "AbortError") {
            throw new Error(
                `Agent ${agentName} timed out after ${timeoutMs / 1000}s`,
            );
        }
        throw err;
    }
}

// ── Dev stub ─────────────────────────────────────────────────────────────────

async function devStub(
    agentName: AgentName,
): Promise<unknown> {
    await new Promise((r) => setTimeout(r, 800 + Math.random() * 1200));

    const stubs: Record<AgentName, unknown> = {
        navigator: {
            success: true,
            portal_url: "https://example.state.gov/medicaid",
            portal_type: "SharePoint",
            relevant_datasets: [
                {
                    url: "https://example.state.gov/medicaid/fees.xlsx",
                    title: "Fee Schedule 2025",
                    file_type: "xlsx",
                },
            ],
            total_links_discovered: 24,
            crawled_pages: ["/medicaid", "/medicaid/provider"],
        },
        extractor: {
            success: true,
            extracted_tables: [
                {
                    sheet_name: "Fee Schedule",
                    row_count: 150,
                    column_count: 9,
                    mapping_confidence: 0.92,
                    data: Array.from({ length: 150 }, (_, i) => ({
                        procedure_code: `9920${i}`,
                        modifier: "",
                        description: `Procedure ${i}`,
                        rate: (10 + Math.random() * 200).toFixed(2),
                    })),
                },
            ],
            total_rows_extracted: 150,
            data_quality_issues: [],
            schema_drift_detected: false,
            file_size_bytes: 58900,
            processing_time_seconds: 2.3,
        },
        csv_exporter: {
            success: true,
            rows_exported: 151,
            columns: [
                "Procedure Code",
                "Modifier",
                "Description",
                "Rate",
                "Effective Date",
            ],
            output_path: "s3://medicaid-fee-raw/raw_exports/example/fees.csv",
        },
        analysis: {
            success: true,
            output_filepath:
                "s3://medicaid-fee-stage/cleaned/example/fees_cleaned.csv",
        },
    };

    return stubs[agentName];
}

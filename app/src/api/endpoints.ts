import type { AgentName } from "../types";

/**
 * Endpoint URLs for each AgentCore agent.
 * Set via VITE_AGENT_*_URL env vars; falls back to empty string (dev mode).
 */
export const AGENT_ENDPOINTS: Record<AgentName, string> = {
    navigator: import.meta.env.VITE_AGENT_NAVIGATOR_URL ?? "",
    extractor: import.meta.env.VITE_AGENT_EXTRACTOR_URL ?? "",
    csv_exporter: import.meta.env.VITE_AGENT_CSV_EXPORTER_URL ?? "",
    analysis: import.meta.env.VITE_AGENT_ANALYSIS_URL ?? "",
};

// Agent names as a union type
export type AgentName = "navigator" | "extractor" | "csv_exporter" | "analysis";

// Display metadata for each agent
export interface AgentMeta {
    name: AgentName;
    displayName: string;
    alias: string;
    icon: string;
    order: number;
    timeoutMs: number;
}

export const AGENTS: Record<AgentName, AgentMeta> = {
    navigator: {
        name: "navigator",
        displayName: "Navigator",
        alias: "The Researcher",
        icon: "🧭",
        order: 0,
        timeoutMs: Number(import.meta.env.VITE_AGENT_TIMEOUT_NAVIGATOR ?? 120_000),
    },
    extractor: {
        name: "extractor",
        displayName: "Extractor",
        alias: "The Parser",
        icon: "📄",
        order: 1,
        timeoutMs: Number(import.meta.env.VITE_AGENT_TIMEOUT_EXTRACTOR ?? 120_000),
    },
    csv_exporter: {
        name: "csv_exporter",
        displayName: "CSV Exporter",
        alias: "The Exporter",
        icon: "📊",
        order: 2,
        timeoutMs: Number(import.meta.env.VITE_AGENT_TIMEOUT_CSV_EXPORTER ?? 60_000),
    },
    analysis: {
        name: "analysis",
        displayName: "Analysis",
        alias: "The Mapper",
        icon: "🔬",
        order: 3,
        timeoutMs: Number(import.meta.env.VITE_AGENT_TIMEOUT_ANALYSIS ?? 120_000),
    },
};

// Ordered list for iteration
export const AGENT_ORDER: AgentName[] = [
    "navigator",
    "extractor",
    "csv_exporter",
    "analysis",
];

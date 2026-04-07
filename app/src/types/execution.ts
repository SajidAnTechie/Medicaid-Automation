import type { AgentName } from "./agents";

// ── Execution ────────────────────────────────────────────────────────────────

export type ExecutionStatus = "running" | "completed" | "failed" | "cancelled";

export interface Execution {
    id: string;
    portalUrl: string;
    stateName: string;
    status: ExecutionStatus;
    createdAt: string;
    completedAt: string | null;
    totalDurationMs: number | null;
    error: string | null;
    currentAgent: AgentName | null;
}

// ── Agent Step ───────────────────────────────────────────────────────────────

export type StepStatus = "pending" | "running" | "success" | "failed" | "skipped";

export interface AgentStep {
    id: string;
    executionId: string;
    agentName: AgentName;
    order: number;
    status: StepStatus;
    startedAt: string | null;
    completedAt: string | null;
    durationMs: number | null;
    input: unknown;
    output: unknown;
    error: string | null;
    insights: AgentInsights | null;
}

// ── Agent Insights (union) ───────────────────────────────────────────────────

export type AgentInsights =
    | NavigatorInsights
    | ExtractorInsights
    | CsvExporterInsights
    | AnalysisInsights;

export interface NavigatorInsights {
    kind: "navigator";
    totalLinksDiscovered: number;
    relevantDatasets: number;
    portalType: string;
    crawledPages: number;
}

export interface ExtractorInsights {
    kind: "extractor";
    tablesExtracted: number;
    totalRowsExtracted: number;
    mappingConfidence: number;
    dataQualityIssues: string[];
    schemaDriftDetected: boolean;
    fileSizeBytes: number;
    processingTimeSec: number;
}

export interface CsvExporterInsights {
    kind: "csv_exporter";
    rowsExported: number;
    columns: string[];
    s3OutputPath: string;
}

export interface AnalysisInsights {
    kind: "analysis";
    s3OutputPath: string;
    presignedUrl: string | null;
}

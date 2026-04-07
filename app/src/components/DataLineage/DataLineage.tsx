import type { AgentStep } from "../../types";
import { AGENTS } from "../../types";

/**
 * Side-by-side view showing how data transforms across agents.
 */
export function DataLineage({ steps }: { steps: AgentStep[] }) {
    const sorted = [...steps].sort((a, b) => a.order - b.order);

    return (
        <div className="overflow-x-auto">
            <div className="grid grid-cols-4 gap-4 min-w-160 p-4">
                {sorted.map((step) => {
                    const meta = AGENTS[step.agentName];
                    const output = step.output as Record<string, unknown> | null;

                    return (
                        <div
                            key={step.id}
                            className="rounded-lg border border-gray-200 bg-white"
                        >
                            {/* Header */}
                            <div className="flex items-center gap-2 px-3 py-2 border-b border-gray-100 bg-gray-50 rounded-t-lg">
                                <span>{meta.icon}</span>
                                <span className="text-xs font-semibold text-gray-700">
                                    {meta.displayName} Output
                                </span>
                            </div>

                            {/* Summary */}
                            <div className="p-3 text-xs text-gray-600 space-y-2">
                                {step.status === "success" && output ? (
                                    <SummaryForAgent agentName={step.agentName} output={output} />
                                ) : step.status === "pending" || step.status === "skipped" ? (
                                    <p className="text-gray-400 italic">Not reached</p>
                                ) : step.status === "running" ? (
                                    <p className="text-blue-500 animate-pulse">Running…</p>
                                ) : (
                                    <p className="text-red-500">Failed</p>
                                )}
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

function SummaryForAgent({
    agentName,
    output,
}: {
    agentName: string;
    output: Record<string, unknown>;
}) {
    switch (agentName) {
        case "navigator": {
            const datasets = Array.isArray(output.relevant_datasets)
                ? output.relevant_datasets
                : [];
            return (
                <>
                    <KeyVal label="Discovered" value={`${output.total_links_discovered ?? 0} links`} />
                    <KeyVal label="Datasets" value={`${datasets.length} relevant`} />
                    <KeyVal label="Type" value={String(output.portal_type ?? "unknown")} />
                </>
            );
        }
        case "extractor": {
            const tables = Array.isArray(output.extracted_tables)
                ? output.extracted_tables
                : [];
            return (
                <>
                    <KeyVal label="Tables" value={String(tables.length)} />
                    <KeyVal label="Rows" value={String(output.total_rows_extracted ?? 0)} />
                    <KeyVal label="Confidence" value={`${Math.round(Number((tables[0] as Record<string, unknown>)?.mapping_confidence ?? 0) * 100)}%`} />
                </>
            );
        }
        case "csv_exporter":
            return (
                <>
                    <KeyVal label="Exported" value={`${output.rows_exported ?? 0} rows`} />
                    <KeyVal label="To" value={String(output.output_path ?? "")} />
                </>
            );
        case "analysis":
            return (
                <>
                    <KeyVal label="Output" value={String(output.output_filepath ?? "")} />
                    <KeyVal label="Status" value="Cleaned ✅" />
                </>
            );
        default:
            return <p className="italic text-gray-400">Unknown agent</p>;
    }
}

function KeyVal({ label, value }: { label: string; value: string }) {
    return (
        <div>
            <span className="font-medium text-gray-500">{label}: </span>
            <span className="text-gray-700 break-all">{value}</span>
        </div>
    );
}

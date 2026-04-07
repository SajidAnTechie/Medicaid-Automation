import { useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "../db/database";
import { getStepsForExecution } from "../db/stepStore";
import { PipelineVisualizer } from "../components/PipelineVisualizer";
import { PayloadInspector } from "../components/PayloadInspector";
import { InsightsPanel } from "../components/InsightsPanel";
import { DataLineage } from "../components/DataLineage";
import { ErrorBanner } from "../components/common/ErrorBanner";
import { StatusBadge } from "../components/common/StatusBadge";
import { DurationLabel } from "../components/common/DurationLabel";
import type { AgentName, AgentStep, Execution } from "../types";
import { deleteExecution } from "../db/executionStore";

type DetailTab = "input" | "output" | "insights" | "lineage";

export function ExecutionDetailPage() {
    const { executionId } = useParams<{ executionId: string }>();
    const navigate = useNavigate();

    const [selectedAgent, setSelectedAgent] = useState<AgentName | null>("navigator");
    const [activeTab, setActiveTab] = useState<DetailTab>("output");

    const execution = useLiveQuery(
        () => (executionId ? db.executions.get(executionId) : undefined),
        [executionId],
    ) as Execution | undefined;

    const steps = useLiveQuery(
        () => (executionId ? getStepsForExecution(executionId) : []),
        [executionId],
    ) as AgentStep[] | undefined;

    const selectedStep = steps?.find((s) => s.agentName === selectedAgent) ?? null;

    if (!execution) {
        return (
            <div className="flex items-center justify-center h-full text-gray-400">
                <div className="text-center">
                    <p className="text-lg">Execution not found</p>
                    <button
                        onClick={() => navigate("/history")}
                        className="mt-3 text-indigo-600 hover:underline text-sm"
                    >
                        ← Back to history
                    </button>
                </div>
            </div>
        );
    }

    return (
        <div className="flex flex-col h-full">
            {/* Header */}
            <div className="border-b border-gray-200 bg-white px-6 py-4">
                <div className="flex items-center justify-between">
                    <div>
                        <button
                            onClick={() => navigate("/history")}
                            className="text-sm text-indigo-600 hover:underline mb-1"
                        >
                            ← Back to history
                        </button>
                        <h1 className="text-xl font-bold text-gray-900">
                            {execution.stateName || "Execution"}{" "}
                            <span className="text-sm font-normal text-gray-400">
                                {execution.id.slice(0, 8)}
                            </span>
                        </h1>
                        <div className="flex items-center gap-3 mt-1">
                            <StatusBadge status={execution.status} />
                            <DurationLabel ms={execution.totalDurationMs} className="text-sm text-gray-500" />
                            <span className="text-xs text-gray-400">
                                {new Date(execution.createdAt).toLocaleString()}
                            </span>
                        </div>
                        <p className="text-sm text-gray-500 mt-1 break-all">
                            {execution.portalUrl}
                        </p>
                    </div>
                    <button
                        onClick={async () => {
                            await deleteExecution(execution.id);
                            navigate("/history");
                        }}
                        className="rounded-lg border border-red-200 px-3 py-1.5 text-xs text-red-600 hover:bg-red-50 transition-colors"
                    >
                        Delete
                    </button>
                </div>

                {execution.error && (
                    <div className="mt-3">
                        <ErrorBanner message={execution.error} />
                    </div>
                )}
            </div>

            {/* Pipeline vis */}
            {steps && steps.length > 0 && (
                <div className="border-b border-gray-200 bg-white">
                    <PipelineVisualizer
                        steps={steps}
                        selectedAgent={selectedAgent}
                        onSelectAgent={setSelectedAgent}
                    />
                </div>
            )}

            {/* Detail tabs */}
            {selectedStep && (
                <div className="flex-1 flex flex-col overflow-hidden bg-gray-50">
                    <div className="flex border-b border-gray-200 bg-white px-6">
                        {(["input", "output", "insights", "lineage"] as DetailTab[]).map(
                            (tab) => (
                                <button
                                    key={tab}
                                    onClick={() => setActiveTab(tab)}
                                    className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors capitalize ${activeTab === tab
                                            ? "border-indigo-500 text-indigo-600"
                                            : "border-transparent text-gray-500 hover:text-gray-700"
                                        }`}
                                >
                                    {tab}
                                </button>
                            ),
                        )}
                    </div>

                    <div className="flex-1 overflow-auto">
                        {activeTab === "input" && (
                            <PayloadInspector data={selectedStep.input} title={`${selectedStep.agentName} Input`} />
                        )}
                        {activeTab === "output" && (
                            <PayloadInspector data={selectedStep.output} title={`${selectedStep.agentName} Output`} />
                        )}
                        {activeTab === "insights" && <InsightsPanel step={selectedStep} />}
                        {activeTab === "lineage" && steps && <DataLineage steps={steps} />}
                    </div>
                </div>
            )}
        </div>
    );
}

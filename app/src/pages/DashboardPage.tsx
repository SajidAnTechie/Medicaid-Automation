import { useCallback, useState } from "react";
import { useLiveQuery } from "dexie-react-hooks";
import { useAuth } from "../auth/useAuth";
import { db } from "../db/database";
import { getStepsForExecution } from "../db/stepStore";
import { runPipeline, retryFromAgent } from "../orchestrator/PipelineOrchestrator";
import { PipelineVisualizer } from "../components/PipelineVisualizer";
import { PayloadInspector } from "../components/PayloadInspector";
import { InsightsPanel } from "../components/InsightsPanel";
import { DataLineage } from "../components/DataLineage";
import { ErrorBanner } from "../components/common/ErrorBanner";
import { StatusBadge } from "../components/common/StatusBadge";
import { DurationLabel } from "../components/common/DurationLabel";
import type { AgentName, AgentStep, Execution } from "../types";

type DetailTab = "input" | "output" | "insights" | "lineage";

export function DashboardPage() {
    const { accessToken } = useAuth();
    const [portalUrl, setPortalUrl] = useState("");
    const [activeExecutionId, setActiveExecutionId] = useState<string | null>(null);
    const [selectedAgent, setSelectedAgent] = useState<AgentName | null>(null);
    const [activeTab, setActiveTab] = useState<DetailTab>("output");
    const [isRunning, setIsRunning] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Live query for current execution
    const execution = useLiveQuery(
        () => (activeExecutionId ? db.executions.get(activeExecutionId) : undefined),
        [activeExecutionId],
    ) as Execution | undefined;

    // Live query for steps
    const steps = useLiveQuery(
        () => (activeExecutionId ? getStepsForExecution(activeExecutionId) : []),
        [activeExecutionId],
    ) as AgentStep[] | undefined;

    const selectedStep = steps?.find((s) => s.agentName === selectedAgent) ?? null;

    const handleRun = useCallback(async () => {
        if (!portalUrl.trim() || !accessToken) return;
        setIsRunning(true);
        setError(null);

        try {
            await runPipeline(portalUrl.trim(), accessToken, {
                onExecutionCreated: (id) => {
                    // Set immediately so useLiveQuery starts tracking progress
                    setActiveExecutionId(id);
                    setSelectedAgent("navigator");
                },
            });
        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
        } finally {
            setIsRunning(false);
        }
    }, [portalUrl, accessToken]);

    const handleRetry = useCallback(async (agentName: AgentName) => {
        if (!activeExecutionId || !accessToken) return;
        setIsRunning(true);
        setError(null);
        setSelectedAgent(agentName);

        try {
            await retryFromAgent(activeExecutionId, agentName, accessToken);
        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
        } finally {
            setIsRunning(false);
        }
    }, [activeExecutionId, accessToken]);

    return (
        <div className="flex flex-col h-full">
            {/* Input Bar */}
            <div className="border-b border-gray-200 bg-white px-6 py-4">
                <div className="flex items-center gap-3 max-w-4xl mx-auto">
                    <input
                        type="url"
                        placeholder="Enter state Medicaid portal URL…"
                        value={portalUrl}
                        onChange={(e) => setPortalUrl(e.target.value)}
                        onKeyDown={(e) => e.key === "Enter" && handleRun()}
                        disabled={isRunning}
                        className="flex-1 rounded-lg border border-gray-300 px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 focus:border-transparent disabled:opacity-50"
                    />
                    <button
                        onClick={handleRun}
                        disabled={isRunning || !portalUrl.trim()}
                        className="rounded-lg bg-indigo-600 px-6 py-2.5 text-sm font-semibold text-white shadow hover:bg-indigo-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
                    >
                        {isRunning ? "Running…" : "▶ Run Pipeline"}
                    </button>
                </div>
            </div>

            {error && (
                <div className="px-6 pt-4">
                    <ErrorBanner message={error} onDismiss={() => setError(null)} />
                </div>
            )}

            {/* Pipeline Visualizer */}
            {steps && steps.length > 0 && (
                <div className="border-b border-gray-200 bg-white">
                    <div className="flex items-center justify-between px-6 py-2">
                        <div className="flex items-center gap-3">
                            {execution && (
                                <>
                                    <StatusBadge status={execution.status} />
                                    <DurationLabel
                                        ms={execution.totalDurationMs}
                                        className="text-sm text-gray-500"
                                    />
                                    {execution.stateName && (
                                        <span className="text-sm text-gray-600">
                                            — {execution.stateName}
                                        </span>
                                    )}
                                </>
                            )}
                        </div>
                    </div>
                    <PipelineVisualizer
                        steps={steps}
                        selectedAgent={selectedAgent}
                        onSelectAgent={setSelectedAgent}
                        onRetry={handleRetry}
                        isRetrying={isRunning}
                    />
                </div>
            )}

            {/* Detail Panel */}
            {selectedStep && (
                <div className="flex-1 flex flex-col overflow-hidden bg-gray-50">
                    {/* Tab bar */}
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

                    {/* Tab content */}
                    <div className="flex-1 overflow-auto">
                        {activeTab === "input" && (
                            <PayloadInspector
                                data={selectedStep.input}
                                title={`${selectedStep.agentName} Input`}
                            />
                        )}
                        {activeTab === "output" && (
                            <PayloadInspector
                                data={selectedStep.output}
                                title={`${selectedStep.agentName} Output`}
                            />
                        )}
                        {activeTab === "insights" && (
                            <InsightsPanel step={selectedStep} />
                        )}
                        {activeTab === "lineage" && steps && (
                            <DataLineage steps={steps} />
                        )}
                    </div>
                </div>
            )}

            {/* Empty state */}
            {!activeExecutionId && (
                <div className="flex-1 flex items-center justify-center text-gray-400">
                    <div className="text-center">
                        <span className="text-5xl">🚀</span>
                        <p className="mt-4 text-lg">Enter a portal URL and run the pipeline</p>
                        <p className="mt-1 text-sm">
                            Results will appear here in real-time
                        </p>
                    </div>
                </div>
            )}
        </div>
    );
}

import type { AgentStep, AgentName } from "../../types";
import { AGENT_ORDER } from "../../types";
import { AgentNode } from "./AgentNode";
import { FlowArrow } from "./FlowArrow";

export function PipelineVisualizer({
    steps,
    selectedAgent,
    onSelectAgent,
    onRetry,
    isRetrying,
}: {
    steps: AgentStep[];
    selectedAgent: AgentName | null;
    onSelectAgent: (name: AgentName) => void;
    onRetry?: (agentName: AgentName) => void;
    isRetrying?: boolean;
}) {
    // Sort steps by order
    const sorted = [...steps].sort((a, b) => a.order - b.order);

    // Map by agent name for quick lookup
    const stepMap = new Map(sorted.map((s) => [s.agentName, s]));

    return (
        <div className="flex items-center justify-center gap-1 overflow-x-auto py-6 px-4">
            {AGENT_ORDER.map((agentName, idx) => {
                const step = stepMap.get(agentName);
                if (!step) return null;

                return (
                    <div key={agentName} className="flex items-center">
                        {idx > 0 && (
                            <FlowArrow
                                prevStatus={stepMap.get(AGENT_ORDER[idx - 1])?.status ?? "pending"}
                            />
                        )}
                        <AgentNode
                            step={step}
                            isSelected={selectedAgent === agentName}
                            onClick={() => onSelectAgent(agentName)}
                            onRetry={onRetry}
                            isRetrying={isRetrying}
                        />
                    </div>
                );
            })}
        </div>
    );
}

import type { AgentStep, AgentName } from "../../types";
import { AGENTS } from "../../types";
import { StatusBadge } from "../common/StatusBadge";
import { DurationLabel } from "../common/DurationLabel";

export function AgentNode({
    step,
    isSelected,
    onClick,
    onRetry,
    isRetrying,
}: {
    step: AgentStep;
    isSelected: boolean;
    onClick: () => void;
    onRetry?: (agentName: AgentName) => void;
    isRetrying?: boolean;
}) {
    const meta = AGENTS[step.agentName];

    return (
        <button
            onClick={onClick}
            className={`
        flex flex-col items-center gap-2 rounded-xl border-2 p-4 transition-all
        min-w-37.5 cursor-pointer
        hover:shadow-md
        ${isSelected
                    ? "border-indigo-500 bg-indigo-50 shadow-md"
                    : "border-gray-200 bg-white hover:border-gray-300"
                }
        ${step.status === "running" ? "ring-2 ring-blue-400 ring-offset-2" : ""}
        ${step.status === "failed" ? "border-red-300 bg-red-50" : ""}
      `}
        >
            <span className="text-2xl">{meta.icon}</span>
            <span className="text-sm font-semibold text-gray-800">
                {meta.displayName}
            </span>
            <StatusBadge status={step.status} />
            <DurationLabel ms={step.durationMs} className="text-xs text-gray-500" />
            {step.status === "failed" && onRetry && (
                <span
                    role="button"
                    onClick={(e) => {
                        e.stopPropagation();
                        if (!isRetrying) onRetry(step.agentName);
                    }}
                    className={`
                        mt-1 inline-flex items-center gap-1 rounded-md px-3 py-1.5 text-xs font-semibold
                        transition-colors cursor-pointer select-none
                        ${isRetrying
                            ? "bg-gray-200 text-gray-500 cursor-not-allowed"
                            : "bg-red-600 text-white hover:bg-red-700 active:bg-red-800"
                        }
                    `}
                >
                    {isRetrying ? "⏳ Retrying…" : "🔄 Retry"}
                </span>
            )}
        </button>
    );
}

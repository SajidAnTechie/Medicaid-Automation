import type { StepStatus, ExecutionStatus } from "../../types";

const STATUS_STYLES: Record<
    StepStatus | ExecutionStatus,
    { bg: string; text: string; icon: string }
> = {
    pending: { bg: "bg-gray-100", text: "text-gray-500", icon: "⬜" },
    running: { bg: "bg-blue-100", text: "text-blue-700", icon: "🔄" },
    success: { bg: "bg-green-100", text: "text-green-700", icon: "✅" },
    completed: { bg: "bg-green-100", text: "text-green-700", icon: "✅" },
    failed: { bg: "bg-red-100", text: "text-red-700", icon: "❌" },
    skipped: { bg: "bg-gray-200", text: "text-gray-400", icon: "⏭️" },
    cancelled: { bg: "bg-yellow-100", text: "text-yellow-700", icon: "🚫" },
};

export function StatusBadge({
    status,
    className = "",
}: {
    status: StepStatus | ExecutionStatus;
    className?: string;
}) {
    const style = STATUS_STYLES[status] ?? STATUS_STYLES.pending;
    return (
        <span
            className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium ${style.bg} ${style.text} ${className} ${status === "running" ? "animate-pulse" : ""
                }`}
        >
            <span>{style.icon}</span>
            <span className="capitalize">{status}</span>
        </span>
    );
}

import type { Execution } from "../../types";
import { StatusBadge } from "../common/StatusBadge";
import { DurationLabel } from "../common/DurationLabel";

export function ExecutionRow({
    execution,
    onClick,
}: {
    execution: Execution;
    onClick: () => void;
}) {
    const date = new Date(execution.createdAt);
    const dateStr = date.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
    });
    const timeStr = date.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
    });

    return (
        <tr
            onClick={onClick}
            className="cursor-pointer hover:bg-gray-50 transition-colors border-b border-gray-100"
        >
            <td className="px-4 py-3 text-sm font-medium text-gray-800">
                {execution.stateName || "—"}
            </td>
            <td className="px-4 py-3 text-sm text-gray-500 max-w-60 truncate">
                {execution.portalUrl}
            </td>
            <td className="px-4 py-3">
                <StatusBadge status={execution.status} />
            </td>
            <td className="px-4 py-3 text-sm text-gray-600">
                <DurationLabel ms={execution.totalDurationMs} />
            </td>
            <td className="px-4 py-3 text-sm text-gray-500">
                {dateStr} {timeStr}
            </td>
        </tr>
    );
}

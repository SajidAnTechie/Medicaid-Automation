import { useState } from "react";
import type { Execution, ExecutionStatus } from "../../types";
import { ExecutionRow } from "./ExecutionRow";

export function ExecutionHistory({
    executions,
    onSelect,
}: {
    executions: Execution[];
    onSelect: (id: string) => void;
}) {
    const [search, setSearch] = useState("");
    const [statusFilter, setStatusFilter] = useState<ExecutionStatus | "all">("all");

    const filtered = executions.filter((e) => {
        if (statusFilter !== "all" && e.status !== statusFilter) return false;
        if (search) {
            const q = search.toLowerCase();
            return (
                e.stateName.toLowerCase().includes(q) ||
                e.portalUrl.toLowerCase().includes(q)
            );
        }
        return true;
    });

    return (
        <div className="flex flex-col h-full">
            {/* Filters */}
            <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-200">
                <input
                    type="text"
                    placeholder="🔍 Search state or URL…"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    className="rounded border border-gray-300 px-3 py-1.5 text-sm flex-1 focus:outline-none focus:ring-1 focus:ring-indigo-400"
                />
                <select
                    value={statusFilter}
                    onChange={(e) =>
                        setStatusFilter(e.target.value as ExecutionStatus | "all")
                    }
                    className="rounded border border-gray-300 px-3 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-indigo-400"
                >
                    <option value="all">All Statuses</option>
                    <option value="running">Running</option>
                    <option value="completed">Completed</option>
                    <option value="failed">Failed</option>
                    <option value="cancelled">Cancelled</option>
                </select>
            </div>

            {/* Table */}
            <div className="flex-1 overflow-auto">
                <table className="w-full">
                    <thead className="sticky top-0 bg-gray-50">
                        <tr className="border-b border-gray-200 text-xs text-gray-500 uppercase tracking-wide">
                            <th className="px-4 py-2 text-left font-medium">State</th>
                            <th className="px-4 py-2 text-left font-medium">Portal URL</th>
                            <th className="px-4 py-2 text-left font-medium">Status</th>
                            <th className="px-4 py-2 text-left font-medium">Duration</th>
                            <th className="px-4 py-2 text-left font-medium">Date</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filtered.map((e) => (
                            <ExecutionRow key={e.id} execution={e} onClick={() => onSelect(e.id)} />
                        ))}
                    </tbody>
                </table>

                {filtered.length === 0 && (
                    <div className="text-gray-400 text-sm text-center py-12">
                        {executions.length === 0
                            ? "No executions yet. Run your first pipeline!"
                            : "No executions match your filters."}
                    </div>
                )}
            </div>
        </div>
    );
}

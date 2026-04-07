import { useNavigate } from "react-router-dom";
import { useLiveQuery } from "dexie-react-hooks";
import { db } from "../db/database";
import { ExecutionHistory } from "../components/ExecutionHistory";
import type { Execution } from "../types";

export function HistoryPage() {
    const navigate = useNavigate();

    const executions = useLiveQuery(
        () => db.executions.orderBy("createdAt").reverse().toArray(),
        [],
    ) as Execution[] | undefined;

    return (
        <div className="flex flex-col h-full">
            <div className="border-b border-gray-200 bg-white px-6 py-4">
                <h1 className="text-xl font-bold text-gray-900">Execution History</h1>
                <p className="text-sm text-gray-500 mt-1">
                    All past pipeline runs stored locally in your browser.
                </p>
            </div>

            <div className="flex-1 overflow-hidden">
                <ExecutionHistory
                    executions={executions ?? []}
                    onSelect={(id) => navigate(`/execution/${id}`)}
                />
            </div>
        </div>
    );
}

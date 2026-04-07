import type { CsvExporterInsights as CsvInsightsType } from "../../types";

export function CsvExporterInsightsView({ data }: { data: CsvInsightsType }) {
    return (
        <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
                <Stat label="Rows Exported" value={data.rowsExported.toLocaleString()} />
                <Stat label="Columns" value={data.columns.length} />
            </div>

            {data.columns.length > 0 && (
                <div className="rounded-lg bg-gray-50 p-3">
                    <p className="text-xs text-gray-500 mb-2">Column Names</p>
                    <div className="flex flex-wrap gap-1">
                        {data.columns.map((col) => (
                            <span
                                key={col}
                                className="rounded bg-indigo-100 px-2 py-0.5 text-xs text-indigo-700"
                            >
                                {col}
                            </span>
                        ))}
                    </div>
                </div>
            )}

            <div className="rounded-lg bg-gray-50 p-3">
                <p className="text-xs text-gray-500">S3 Output Path</p>
                <p className="text-xs font-mono text-gray-700 break-all mt-1">
                    {data.s3OutputPath}
                </p>
            </div>
        </div>
    );
}

function Stat({ label, value }: { label: string; value: string | number }) {
    return (
        <div className="rounded-lg bg-gray-50 p-3">
            <p className="text-xs text-gray-500">{label}</p>
            <p className="text-lg font-semibold text-gray-800">{value}</p>
        </div>
    );
}

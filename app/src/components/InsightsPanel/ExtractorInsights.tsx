import type { ExtractorInsights as ExtInsightsType } from "../../types";

export function ExtractorInsightsView({ data }: { data: ExtInsightsType }) {
    const confidence = Math.round(data.mappingConfidence * 100);
    const confColor =
        confidence >= 80
            ? "text-green-600"
            : confidence >= 50
                ? "text-yellow-600"
                : "text-red-600";

    return (
        <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
                <Stat label="Tables Extracted" value={data.tablesExtracted} />
                <Stat label="Total Rows" value={data.totalRowsExtracted.toLocaleString()} />
                <Stat
                    label="Mapping Confidence"
                    value={`${confidence}%`}
                    valueClass={confColor}
                />
                <Stat
                    label="Schema Drift"
                    value={data.schemaDriftDetected ? "⚠️ Detected" : "None"}
                    valueClass={data.schemaDriftDetected ? "text-red-600" : "text-green-600"}
                />
                <Stat
                    label="File Size"
                    value={`${(data.fileSizeBytes / 1024).toFixed(0)} KB`}
                />
                <Stat label="Processing Time" value={`${data.processingTimeSec}s`} />
            </div>

            {data.dataQualityIssues.length > 0 && (
                <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-3">
                    <p className="text-xs font-semibold text-yellow-700 mb-2">
                        ⚠️ Data Quality Issues ({data.dataQualityIssues.length})
                    </p>
                    <ul className="list-disc list-inside text-xs text-yellow-800 space-y-1">
                        {data.dataQualityIssues.map((issue, i) => (
                            <li key={i}>{issue}</li>
                        ))}
                    </ul>
                </div>
            )}
        </div>
    );
}

function Stat({
    label,
    value,
    valueClass = "",
}: {
    label: string;
    value: string | number;
    valueClass?: string;
}) {
    return (
        <div className="rounded-lg bg-gray-50 p-3">
            <p className="text-xs text-gray-500">{label}</p>
            <p className={`text-lg font-semibold text-gray-800 ${valueClass}`}>
                {value}
            </p>
        </div>
    );
}

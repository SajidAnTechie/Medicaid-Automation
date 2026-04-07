import type { NavigatorInsights as NavInsightsType } from "../../types";

export function NavigatorInsightsView({ data }: { data: NavInsightsType }) {
    return (
        <div className="grid grid-cols-2 gap-4">
            <Stat label="Links Discovered" value={data.totalLinksDiscovered} />
            <Stat label="Relevant Datasets" value={data.relevantDatasets} />
            <Stat label="Portal Type" value={data.portalType} />
            <Stat label="Pages Crawled" value={data.crawledPages} />
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

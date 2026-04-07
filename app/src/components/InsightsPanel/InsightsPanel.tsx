import type { AgentStep } from "../../types";
import { NavigatorInsightsView } from "./NavigatorInsights";
import { ExtractorInsightsView } from "./ExtractorInsights";
import { CsvExporterInsightsView } from "./CsvExporterInsights";
import { AnalysisInsightsView } from "./AnalysisInsights";

export function InsightsPanel({ step }: { step: AgentStep }) {
    const { insights } = step;

    if (!insights) {
        return (
            <div className="text-gray-400 italic p-4 text-center">
                No insights available
            </div>
        );
    }

    return (
        <div className="p-4">
            <h3 className="text-sm font-semibold text-gray-700 mb-4">
                📊 {step.agentName.replace("_", " ")} Insights
            </h3>
            {insights.kind === "navigator" && (
                <NavigatorInsightsView data={insights} />
            )}
            {insights.kind === "extractor" && (
                <ExtractorInsightsView data={insights} />
            )}
            {insights.kind === "csv_exporter" && (
                <CsvExporterInsightsView data={insights} />
            )}
            {insights.kind === "analysis" && (
                <AnalysisInsightsView data={insights} />
            )}
        </div>
    );
}

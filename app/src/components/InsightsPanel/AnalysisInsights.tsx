import type { AnalysisInsights as AnalysisInsightsType } from "../../types";
import { DownloadButton } from "../common/DownloadButton";

export function AnalysisInsightsView({ data }: { data: AnalysisInsightsType }) {
    return (
        <div className="space-y-4">
            <div className="rounded-lg bg-gray-50 p-3">
                <p className="text-xs text-gray-500">S3 Output Path</p>
                <p className="text-xs font-mono text-gray-700 break-all mt-1">
                    {data.s3OutputPath}
                </p>
            </div>

            <div className="rounded-lg bg-green-50 border border-green-200 p-3">
                <p className="text-sm text-green-700 font-medium">
                    ✅ Cleaned & uploaded to staging bucket
                </p>
            </div>

            {/* Download section */}
            <div className="rounded-lg border border-gray-200 bg-white p-4">
                <p className="text-sm font-semibold text-gray-700 mb-3">
                    📥 Download Result
                </p>
                <DownloadButton url="" />
            </div>
        </div>
    );
}

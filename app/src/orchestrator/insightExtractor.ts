import type {
    AgentName,
    AgentInsights,
    NavigatorInsights,
    ExtractorInsights,
    CsvExporterInsights,
    AnalysisInsights,
} from "../types";

/**
 * Extract human-readable insight metrics from a decoded agent output.
 */
export function extractInsights(
    agentName: AgentName,
    output: Record<string, unknown>,
): AgentInsights | null {
    try {
        switch (agentName) {
            case "navigator":
                return extractNavigator(output);
            case "extractor":
                return extractExtractor(output);
            case "csv_exporter":
                return extractCsvExporter(output);
            case "analysis":
                return extractAnalysis(output);
            default:
                return null;
        }
    } catch {
        return null;
    }
}

function extractNavigator(o: Record<string, unknown>): NavigatorInsights {
    const datasets = Array.isArray(o.relevant_datasets)
        ? o.relevant_datasets
        : [];
    const crawled = Array.isArray(o.crawled_pages) ? o.crawled_pages : [];
    return {
        kind: "navigator",
        totalLinksDiscovered: Number(o.total_links_discovered ?? 0),
        relevantDatasets: datasets.length,
        portalType: String(o.portal_type ?? "unknown"),
        crawledPages: crawled.length,
    };
}

function extractExtractor(o: Record<string, unknown>): ExtractorInsights {
    const tables = Array.isArray(o.extracted_tables) ? o.extracted_tables : [];
    const firstTable =
        tables.length > 0 ? (tables[0] as Record<string, unknown>) : {};
    return {
        kind: "extractor",
        tablesExtracted: tables.length,
        totalRowsExtracted: Number(o.total_rows_extracted ?? 0),
        mappingConfidence: Number(firstTable.mapping_confidence ?? 0),
        dataQualityIssues: Array.isArray(o.data_quality_issues)
            ? (o.data_quality_issues as string[])
            : [],
        schemaDriftDetected: Boolean(o.schema_drift_detected),
        fileSizeBytes: Number(o.file_size_bytes ?? 0),
        processingTimeSec: Number(o.processing_time_seconds ?? 0),
    };
}

function extractCsvExporter(o: Record<string, unknown>): CsvExporterInsights {
    return {
        kind: "csv_exporter",
        rowsExported: Number(o.rows_exported ?? 0),
        columns: Array.isArray(o.columns) ? (o.columns as string[]) : [],
        s3OutputPath: String(o.output_path ?? ""),
    };
}

function extractAnalysis(o: Record<string, unknown>): AnalysisInsights {
    // The agent may return the presigned URL under various keys
    const presigned =
        o.presigned_url ?? o.download_url ?? o.presignedUrl ?? null;
    return {
        kind: "analysis",
        s3OutputPath: String(o.output_filepath ?? ""),
        presignedUrl: presigned ? String(presigned) : null,
    };
}

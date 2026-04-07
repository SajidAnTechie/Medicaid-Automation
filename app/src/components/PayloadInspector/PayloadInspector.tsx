import { useState } from "react";

/** Renders a single JSON value with syntax-highlighting colors. */
function JsonValue({ value }: { value: unknown }) {
    if (value === null)
        return <span className="italic text-gray-400">null</span>;
    if (value === undefined)
        return <span className="italic text-gray-400">undefined</span>;
    if (typeof value === "string")
        return <span className="text-green-700">&quot;{value}&quot;</span>;
    if (typeof value === "number")
        return <span className="text-blue-700">{value}</span>;
    if (typeof value === "boolean")
        return <span className="text-orange-600">{String(value)}</span>;
    return <span className="text-gray-700">{String(value)}</span>;
}

/** Recursively renders a collapsible JSON tree. */
function JsonNode({
    keyName,
    value,
    depth = 0,
}: {
    keyName?: string;
    value: unknown;
    depth?: number;
}) {
    const [expanded, setExpanded] = useState(depth < 2);

    const indent = depth * 16;

    // Primitive values
    if (value === null || typeof value !== "object") {
        return (
            <div className="flex" style={{ paddingLeft: indent }}>
                {keyName != null && (
                    <span className="text-purple-700 mr-1">"{keyName}": </span>
                )}
                <JsonValue value={value} />
            </div>
        );
    }

    const isArray = Array.isArray(value);
    const entries = isArray
        ? (value as unknown[]).map((v, i) => [String(i), v] as const)
        : Object.entries(value as Record<string, unknown>);

    const summary = isArray ? `Array(${entries.length})` : `Object(${entries.length})`;
    const bracket = isArray ? ["[", "]"] : ["{", "}"];

    return (
        <div>
            <button
                onClick={() => setExpanded(!expanded)}
                className="flex items-center gap-1 hover:bg-gray-100 rounded px-1 text-left w-full"
                style={{ paddingLeft: indent }}
            >
                <span className="text-gray-400 text-xs w-4 text-center">
                    {expanded ? "▼" : "▶"}
                </span>
                {keyName != null && (
                    <span className="text-purple-700">"{keyName}": </span>
                )}
                {!expanded && (
                    <span className="text-gray-500">
                        {bracket[0]} {summary} {bracket[1]}
                    </span>
                )}
                {expanded && <span className="text-gray-500">{bracket[0]}</span>}
            </button>

            {expanded && (
                <>
                    {entries.length > 200 ? (
                        // For very large arrays, show a truncated view
                        <>
                            {entries.slice(0, 100).map(([k, v]) => (
                                <JsonNode key={k} keyName={isArray ? undefined : k} value={v} depth={depth + 1} />
                            ))}
                            <div
                                className="text-gray-400 italic text-sm"
                                style={{ paddingLeft: indent + 16 }}
                            >
                                … {entries.length - 200} more items …
                            </div>
                            {entries.slice(-100).map(([k, v]) => (
                                <JsonNode key={k} keyName={isArray ? undefined : k} value={v} depth={depth + 1} />
                            ))}
                        </>
                    ) : (
                        entries.map(([k, v]) => (
                            <JsonNode key={k} keyName={isArray ? undefined : k} value={v} depth={depth + 1} />
                        ))
                    )}
                    <div className="text-gray-500" style={{ paddingLeft: indent }}>
                        {bracket[1]}
                    </div>
                </>
            )}
        </div>
    );
}

/** A searchable, collapsible JSON tree inspector. */
export function PayloadInspector({
    data,
    title,
}: {
    data: unknown;
    title?: string;
}) {
    const [search, setSearch] = useState("");
    const [showRaw, setShowRaw] = useState(false);

    if (data == null) {
        return (
            <div className="text-gray-400 italic p-4 text-center">
                No data available
            </div>
        );
    }

    return (
        <div className="flex flex-col h-full">
            {/* Header */}
            <div className="flex items-center justify-between px-4 py-2 border-b border-gray-200">
                {title && <h3 className="text-sm font-semibold text-gray-700">{title}</h3>}
                <div className="flex items-center gap-2">
                    <input
                        type="text"
                        placeholder="Search keys / values…"
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        className="rounded border border-gray-300 px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-indigo-400"
                    />
                    <button
                        onClick={() => setShowRaw(!showRaw)}
                        className="rounded border border-gray-300 px-2 py-1 text-xs hover:bg-gray-100"
                    >
                        {showRaw ? "Decoded" : "Raw"}
                    </button>
                    <button
                        onClick={() => {
                            navigator.clipboard.writeText(
                                typeof data === "string" ? data : JSON.stringify(data, null, 2),
                            );
                        }}
                        className="rounded border border-gray-300 px-2 py-1 text-xs hover:bg-gray-100"
                    >
                        Copy
                    </button>
                </div>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-auto p-4 font-mono text-xs leading-relaxed bg-gray-50">
                {showRaw ? (
                    <pre className="whitespace-pre-wrap break-all text-gray-700">
                        {typeof data === "string" ? data : JSON.stringify(data, null, 2)}
                    </pre>
                ) : (
                    <JsonNode value={data} />
                )}
            </div>
        </div>
    );
}

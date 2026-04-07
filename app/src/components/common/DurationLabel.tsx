export function DurationLabel({
    ms,
    className = "",
}: {
    ms: number | null;
    className?: string;
}) {
    if (ms == null) return <span className={`text-gray-400 ${className}`}>—</span>;

    const seconds = ms / 1000;
    const display =
        seconds < 60
            ? `${seconds.toFixed(1)}s`
            : `${Math.floor(seconds / 60)}m ${(seconds % 60).toFixed(0)}s`;

    return <span className={`tabular-nums ${className}`}>{display}</span>;
}

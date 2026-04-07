import type { StepStatus } from "../../types";

const ARROW_STYLES: Record<string, string> = {
    success: "text-green-500",
    failed: "text-red-400",
    default: "text-gray-300",
};

export function FlowArrow({ prevStatus }: { prevStatus: StepStatus }) {
    const color =
        prevStatus === "success"
            ? ARROW_STYLES.success
            : prevStatus === "failed"
                ? ARROW_STYLES.failed
                : ARROW_STYLES.default;

    const isDashed = prevStatus === "pending" || prevStatus === "skipped";

    return (
        <div className={`flex items-center px-1 ${color}`}>
            <svg
                width="40"
                height="20"
                viewBox="0 0 40 20"
                fill="none"
                className="shrink-0"
            >
                <line
                    x1="0"
                    y1="10"
                    x2="32"
                    y2="10"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeDasharray={isDashed ? "4 4" : "none"}
                />
                <polygon points="32,5 40,10 32,15" fill="currentColor" />
            </svg>
        </div>
    );
}

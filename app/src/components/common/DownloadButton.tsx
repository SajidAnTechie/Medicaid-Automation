import { useCallback, useState } from "react";

/** Check if an AWS presigned URL has expired by inspecting X-Amz-Date + X-Amz-Expires */
function isExpired(url: string): boolean {
    try {
        const params = new URL(url).searchParams;
        const amzDate = params.get("X-Amz-Date"); // e.g. 20260405T023319Z
        const amzExpires = params.get("X-Amz-Expires"); // seconds
        if (!amzDate || !amzExpires) return false; // can't tell — assume valid

        const year = amzDate.slice(0, 4);
        const month = amzDate.slice(4, 6);
        const day = amzDate.slice(6, 8);
        const hour = amzDate.slice(9, 11);
        const min = amzDate.slice(11, 13);
        const sec = amzDate.slice(13, 15);

        const signedAt = new Date(
            `${year}-${month}-${day}T${hour}:${min}:${sec}Z`,
        ).getTime();
        const expiresMs = Number(amzExpires) * 1000;

        return Date.now() > signedAt + expiresMs;
    } catch {
        return false;
    }
}

type DownloadState = "idle" | "downloading" | "expired" | "error";

export function DownloadButton({
    url,
    fileName,
    className = "",
}: {
    url: string | null;
    fileName?: string;
    className?: string;
}) {
    const [state, setState] = useState<DownloadState>("idle");
    const [errorMsg, setErrorMsg] = useState<string | null>(null);

    const handleDownload = useCallback(async () => {
        if (!url) return;

        // Check expiration before attempting download
        if (isExpired(url)) {
            setState("expired");
            return;
        }

        setState("downloading");
        setErrorMsg(null);

        try {
            const response = await fetch(url);

            if (response.status === 403 || response.status === 401) {
                setState("expired");
                return;
            }

            if (!response.ok) {
                throw new Error(`Download failed: HTTP ${response.status}`);
            }

            const blob = await response.blob();
            const blobUrl = URL.createObjectURL(blob);

            const anchor = document.createElement("a");
            anchor.href = blobUrl;
            anchor.download = fileName ?? guessFileName(url);
            document.body.appendChild(anchor);
            anchor.click();
            document.body.removeChild(anchor);
            URL.revokeObjectURL(blobUrl);

            setState("idle");
        } catch (err) {
            setErrorMsg(err instanceof Error ? err.message : String(err));
            setState("error");
        }
    }, [url, fileName]);

    if (!url) {
        return (
            <div className={`text-sm text-gray-400 italic ${className}`}>
                No download available
            </div>
        );
    }

    return (
        <div className={`flex flex-col gap-2 ${className}`}>
            <button
                onClick={handleDownload}
                disabled={state === "downloading"}
                className={`
                    inline-flex items-center gap-2 rounded-lg px-4 py-2.5 text-sm font-semibold
                    shadow transition-colors disabled:opacity-60 disabled:cursor-not-allowed
                    ${state === "expired"
                        ? "bg-yellow-100 text-yellow-800 border border-yellow-300 hover:bg-yellow-200"
                        : "bg-indigo-600 text-white hover:bg-indigo-700"
                    }
                `}
            >
                {state === "downloading" && (
                    <>
                        <span className="animate-spin">⏳</span>
                        Downloading…
                    </>
                )}
                {state === "idle" && (
                    <>
                        <span>📥</span>
                        Download Cleaned CSV
                    </>
                )}
                {state === "expired" && (
                    <>
                        <span>⚠️</span>
                        Link Expired — Re-run Pipeline
                    </>
                )}
                {state === "error" && (
                    <>
                        <span>❌</span>
                        Retry Download
                    </>
                )}
            </button>

            {state === "expired" && (
                <p className="text-xs text-yellow-700">
                    The presigned URL has expired. Run the pipeline again to generate a new link.
                </p>
            )}

            {state === "error" && errorMsg && (
                <p className="text-xs text-red-600">{errorMsg}</p>
            )}
        </div>
    );
}

/** Extract a reasonable filename from a presigned S3 URL */
function guessFileName(url: string): string {
    try {
        const pathname = new URL(url).pathname; // e.g. /cleaned_data/alaska/alaska_raw_data_cleaned.csv
        const segments = pathname.split("/");
        return segments[segments.length - 1] || "download.csv";
    } catch {
        return "download.csv";
    }
}

export function ErrorBanner({
    message,
    onDismiss,
}: {
    message: string;
    onDismiss?: () => void;
}) {
    return (
        <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-800">
            <div className="flex items-start gap-3">
                <span className="text-lg leading-none">❌</span>
                <div className="flex-1">
                    <p className="font-medium">Error</p>
                    <p className="mt-1 whitespace-pre-wrap">{message}</p>
                </div>
                {onDismiss && (
                    <button
                        onClick={onDismiss}
                        className="text-red-400 hover:text-red-600 transition-colors"
                        aria-label="Dismiss"
                    >
                        ✕
                    </button>
                )}
            </div>
        </div>
    );
}

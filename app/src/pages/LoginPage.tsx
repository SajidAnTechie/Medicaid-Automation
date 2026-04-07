import { useAuth } from "../auth/useAuth";

export function LoginPage() {
    const { login, isLoading } = useAuth();

    return (
        <div className="flex min-h-screen items-center justify-center bg-linear-to-br from-indigo-50 to-white">
            <div className="w-full max-w-sm rounded-2xl bg-white p-8 shadow-xl">
                <div className="text-center">
                    <span className="text-5xl">🏥</span>
                    <h1 className="mt-4 text-2xl font-bold text-gray-900">
                        Medicaid Automation
                    </h1>
                    <p className="mt-2 text-sm text-gray-500">
                        Pipeline Dashboard
                    </p>
                </div>

                <button
                    onClick={login}
                    disabled={isLoading}
                    className="mt-8 w-full rounded-lg bg-indigo-600 px-4 py-3 text-sm font-semibold text-white shadow hover:bg-indigo-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                    {isLoading ? "Authenticating…" : "Sign in with Cognito"}
                </button>

                <p className="mt-4 text-center text-xs text-gray-400">
                    Secure OAuth2 authentication via AWS Cognito
                </p>
            </div>
        </div>
    );
}

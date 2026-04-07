import {
    useCallback,
    useEffect,
    useMemo,
    useRef,
    useState,
    type ReactNode,
} from "react";
import { AuthContext, type AuthState } from "./AuthContext";
import {
    cognitoConfig,
    exchangeCodeForTokens,
    getLoginUrl,
    getLogoutUrl,
} from "./cognito";

// ── Dev-mode detection ───────────────────────────────────────────────────────

const isDev = !cognitoConfig.clientId;

/** True when there is an OAuth code in the URL that needs exchanging. */
function hasAuthCode(): boolean {
    return new URLSearchParams(window.location.search).has("code");
}

// ── Cookie persistence helpers ───────────────────────────────────────────────

const COOKIE_ACCESS_TOKEN = "medicaid_access_token";
const COOKIE_REFRESH_TOKEN = "medicaid_refresh_token";
const COOKIE_USER = "medicaid_user";

/** Max-age in seconds — 1 hour for access token, 30 days for refresh token */
const ACCESS_MAX_AGE = 3600;
const REFRESH_MAX_AGE = 30 * 24 * 3600;

function setCookie(name: string, value: string, maxAge: number): void {
    // Secure + SameSite=Strict in production; relaxed in dev (http://localhost)
    const secure = window.location.protocol === "https:" ? ";Secure" : "";
    document.cookie = `${name}=${encodeURIComponent(value)};path=/;max-age=${maxAge};SameSite=Strict${secure}`;
}

function getCookie(name: string): string | null {
    const match = document.cookie.match(
        new RegExp(`(?:^|;\\s*)${name}=([^;]*)`),
    );
    return match ? decodeURIComponent(match[1]) : null;
}

function deleteCookie(name: string): void {
    document.cookie = `${name}=;path=/;max-age=0`;
}

function loadSession(): {
    token: string | null;
    refreshToken: string | null;
    user: string | null;
} {
    return {
        token: getCookie(COOKIE_ACCESS_TOKEN),
        refreshToken: getCookie(COOKIE_REFRESH_TOKEN),
        user: getCookie(COOKIE_USER),
    };
}

function saveSession(
    accessToken: string,
    refreshToken: string,
    user: string,
): void {
    setCookie(COOKIE_ACCESS_TOKEN, accessToken, ACCESS_MAX_AGE);
    setCookie(COOKIE_REFRESH_TOKEN, refreshToken, REFRESH_MAX_AGE);
    setCookie(COOKIE_USER, user, REFRESH_MAX_AGE);
}

function clearSession(): void {
    deleteCookie(COOKIE_ACCESS_TOKEN);
    deleteCookie(COOKIE_REFRESH_TOKEN);
    deleteCookie(COOKIE_USER);
}

// ── Provider ─────────────────────────────────────────────────────────────────

export function AuthProvider({ children }: { children: ReactNode }) {
    // Rehydrate from cookies so a page refresh keeps the user logged in
    const saved = isDev ? null : loadSession();

    const [accessToken, setAccessToken] = useState<string | null>(
        isDev ? "dev-token" : saved?.token ?? null,
    );
    const [refreshToken, setRefreshToken] = useState<string | null>(
        isDev ? "dev-refresh" : saved?.refreshToken ?? null,
    );
    const [user, setUser] = useState<string | null>(
        isDev ? "dev@localhost" : saved?.user ?? null,
    );
    // Show loading only when we have a code to exchange AND no cached session
    const [isLoading, setIsLoading] = useState(
        () => !isDev && hasAuthCode() && !saved?.token,
    );
    const didRun = useRef(false);
    /** Capture whether we had a saved token at mount time (avoids stale closure) */
    const hadSavedToken = useRef(!!saved?.token);

    // On mount: exchange OAuth code for tokens (production only)
    useEffect(() => {
        if (isDev || didRun.current) return;
        didRun.current = true;

        // If we already have a valid session from cookies, skip the exchange
        // (the auth code may already have been consumed on a previous load)
        if (hadSavedToken.current) {
            // Clean up ?code= from URL if still present
            if (window.location.search.includes("code=")) {
                window.history.replaceState({}, "", window.location.pathname);
            }
            return;
        }

        const params = new URLSearchParams(window.location.search);
        const code = params.get("code");
        if (!code) return; // nothing to exchange → isLoading is already false

        // Remove code from URL
        window.history.replaceState({}, "", window.location.pathname);

        exchangeCodeForTokens(code)
            .then((tokens) => {
                setAccessToken(tokens.accessToken);
                setRefreshToken(tokens.refreshToken);
                let userName = "user";
                try {
                    const payload = JSON.parse(atob(tokens.idToken.split(".")[1]));
                    userName = payload.email ?? payload.sub ?? "user";
                } catch {
                    // keep default
                }
                setUser(userName);
                saveSession(tokens.accessToken, tokens.refreshToken, userName);
            })
            .catch((err) => {
                console.error("Auth failed:", err);
            })
            .finally(() => setIsLoading(false));
    }, []);

    const login = useCallback(() => {
        if (isDev) {
            setAccessToken("dev-token");
            setUser("dev@localhost");
            return;
        }
        window.location.href = getLoginUrl();
    }, []);

    const logout = useCallback(() => {
        setAccessToken(null);
        setRefreshToken(null);
        setUser(null);
        clearSession();
        if (!isDev) {
            window.location.href = getLogoutUrl();
        }
    }, []);

    const value = useMemo<AuthState>(
        () => ({
            isAuthenticated: !!accessToken,
            isLoading,
            user,
            accessToken,
            refreshToken,
            login,
            logout,
        }),
        [accessToken, refreshToken, isLoading, user, login, logout],
    );

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

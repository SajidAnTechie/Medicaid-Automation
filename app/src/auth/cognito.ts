// Cognito configuration — reads from Vite env variables.
// In development these can be left empty; the auth flow will be
// bypassed with a dev-mode stub token.

export const cognitoConfig = {
    userPoolId: import.meta.env.VITE_COGNITO_USER_POOL_ID ?? "",
    clientId: import.meta.env.VITE_COGNITO_CLIENT_ID ?? "",
    domain: import.meta.env.VITE_COGNITO_DOMAIN ?? "",
    redirectUri: import.meta.env.VITE_COGNITO_REDIRECT_URI ?? window.location.origin,
    logoutUri: import.meta.env.VITE_COGNITO_LOGOUT_URI ?? window.location.origin,
};

/** Build the Cognito Hosted UI login URL */
export function getLoginUrl(): string {
    const { domain, clientId, redirectUri } = cognitoConfig;
    const params = new URLSearchParams({
        response_type: "code",
        client_id: clientId,
        redirect_uri: redirectUri,
        // scope: "openid profile email",
    });
    return `${domain}/login?${params}`;
}

/** Build the Cognito logout URL */
export function getLogoutUrl(): string {
    const { domain, clientId, logoutUri } = cognitoConfig;
    const params = new URLSearchParams({
        client_id: clientId,
        logout_uri: logoutUri,
    });
    return `${domain}/logout?${params}`;
}

/** Exchange authorization code for tokens */
export async function exchangeCodeForTokens(
    code: string,
): Promise<{ accessToken: string; idToken: string; refreshToken: string }> {
    const { domain, clientId, redirectUri } = cognitoConfig;

    const response = await fetch(`${domain}/oauth2/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
            grant_type: "authorization_code",
            client_id: clientId,
            redirect_uri: redirectUri,
            code,
        }),
    });

    if (!response.ok) {
        const errorBody = await response.text().catch(() => "");
        console.error("Cognito token exchange error:", {
            status: response.status,
            body: errorBody,
            redirect_uri: redirectUri,
            client_id: clientId,
        });
        throw new Error(
            `Token exchange failed: ${response.status} — ${errorBody}`,
        );
    }

    const data = await response.json();
    return {
        accessToken: data.access_token,
        idToken: data.id_token,
        refreshToken: data.refresh_token,
    };
}

/** Refresh the access token using a refresh token */
export async function refreshAccessToken(
    refreshToken: string,
): Promise<{ accessToken: string; idToken: string }> {
    const { domain, clientId } = cognitoConfig;

    const response = await fetch(`${domain}/oauth2/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
            grant_type: "refresh_token",
            client_id: clientId,
            refresh_token: refreshToken,
        }),
    });

    if (!response.ok) {
        throw new Error(`Token refresh failed: ${response.status}`);
    }

    const data = await response.json();
    return {
        accessToken: data.access_token,
        idToken: data.id_token,
    };
}

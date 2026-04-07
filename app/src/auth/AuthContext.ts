import { createContext } from "react";

export interface AuthState {
    isAuthenticated: boolean;
    isLoading: boolean;
    user: string | null;
    accessToken: string | null;
    refreshToken: string | null;
    login: () => void;
    logout: () => void;
}

export const AuthContext = createContext<AuthState>({
    isAuthenticated: false,
    isLoading: true,
    user: null,
    accessToken: null,
    refreshToken: null,
    login: () => { },
    logout: () => { },
});

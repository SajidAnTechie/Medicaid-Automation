import { BrowserRouter, Routes, Route, NavLink, Navigate, useSearchParams } from "react-router-dom";
import { AuthProvider } from "./auth/AuthProvider";
import { ProtectedRoute } from "./auth/ProtectedRoute";
import { useAuth } from "./auth/useAuth";
import { DashboardPage } from "./pages/DashboardPage";
import { HistoryPage } from "./pages/HistoryPage";
import { ExecutionDetailPage } from "./pages/ExecutionDetailPage";
import { LoginPage } from "./pages/LoginPage";

function TopBar() {
  const { user, logout, isAuthenticated } = useAuth();

  if (!isAuthenticated) return null;

  return (
    <header className="flex items-center justify-between border-b border-gray-200 bg-white px-6 py-3 shrink-0">
      <div className="flex items-center gap-6">
        <NavLink to="/" className="flex items-center gap-2 text-lg font-bold text-gray-900">
          <span>🏥</span> Medicaid Automation
        </NavLink>
        <nav className="flex gap-1">
          <NavLink
            to="/"
            end
            className={({ isActive }) =>
              `rounded-lg px-3 py-1.5 text-sm font-medium transition-colors ${isActive
                ? "bg-indigo-100 text-indigo-700"
                : "text-gray-600 hover:bg-gray-100"
              }`
            }
          >
            Dashboard
          </NavLink>
          <NavLink
            to="/history"
            className={({ isActive }) =>
              `rounded-lg px-3 py-1.5 text-sm font-medium transition-colors ${isActive
                ? "bg-indigo-100 text-indigo-700"
                : "text-gray-600 hover:bg-gray-100"
              }`
            }
          >
            History
          </NavLink>
        </nav>
      </div>

      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-500">{user}</span>
        <button
          onClick={logout}
          className="rounded-lg border border-gray-200 px-3 py-1.5 text-xs text-gray-600 hover:bg-gray-100 transition-colors"
        >
          Sign out
        </button>
      </div>
    </header>
  );
}

const cognitoConfig = {
  userPoolId: import.meta.env.VITE_COGNITO_USER_POOL_ID ?? "",
  clientId: import.meta.env.VITE_COGNITO_CLIENT_ID ?? "",
  domain: import.meta.env.VITE_COGNITO_DOMAIN ?? "",
  redirectUri: import.meta.env.VITE_COGNITO_REDIRECT_URI ?? window.location.origin,
  logoutUri: import.meta.env.VITE_COGNITO_LOGOUT_URI ?? window.location.origin,
};

console.log("cognitoConfig:", cognitoConfig);

/**
 * If Cognito redirectUri is set to /callback, this forwards the ?code=
 * query param to the root where AuthProvider picks it up.
 */
function CallbackRedirect() {
  const [params] = useSearchParams();
  const code = params.get("code");
  return <Navigate to={code ? `/?code=${code}` : "/"} replace />;
}

function AppRoutes() {
  return (
    <div className="flex flex-col h-screen bg-gray-50">
      <TopBar />
      <main className="flex-1 overflow-hidden">
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/callback"
            element={<CallbackRedirect />}
          />
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <DashboardPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/history"
            element={
              <ProtectedRoute>
                <HistoryPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/execution/:executionId"
            element={
              <ProtectedRoute>
                <ExecutionDetailPage />
              </ProtectedRoute>
            }
          />
        </Routes>
      </main>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <AppRoutes />
      </AuthProvider>
    </BrowserRouter>
  );
}

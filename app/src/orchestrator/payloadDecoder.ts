/**
 * Recursively unwrap multi-layer JSON-encoded payloads.
 *
 * AgentCore often wraps responses in extra layers of string-encoding:
 *   '{"prompt": "{\"success\":true,...}"}'
 *
 * This function recursively parses any string value that looks like JSON.
 */
export function deepDecode(value: unknown): unknown {
    if (typeof value === "string") {
        try {
            const parsed = JSON.parse(value);
            return deepDecode(parsed);
        } catch {
            return value;
        }
    }

    if (Array.isArray(value)) {
        return value.map(deepDecode);
    }

    if (typeof value === "object" && value !== null) {
        return Object.fromEntries(
            Object.entries(value).map(([k, v]) => [k, deepDecode(v)]),
        );
    }

    return value;
}

import { v4 as uuidv4 } from "uuid";
import type { AgentName } from "../types";
import { AGENT_ORDER, AGENTS } from "../types";
import { callAgentCore } from "../api/agentCore";
import { deepDecode } from "./payloadDecoder";
import { extractInsights } from "./insightExtractor";
import {
    createExecution,
    setExecutionStatus,
    setCurrentAgent,
    updateExecution,
} from "../db/executionStore";
import {
    createStepsForExecution,
    markStepRunning,
    markStepSuccess,
    markStepFailed,
    markRemainingSkipped,
    resetStepsFrom,
    getStepsForExecution,
} from "../db/stepStore";
import { getExecution } from "../db/executionStore";

export interface PipelineCallbacks {
    /** Called immediately after the execution & steps are created in IndexedDB, with the executionId */
    onExecutionCreated?: (executionId: string) => void;
    /** Called when an agent's status changes */
    onStepChange?: () => void;
    /** Called when the overall execution status changes */
    onExecutionChange?: () => void;
}

/**
 * Build the payload to send to the given agent.
 */
function buildPayload(
    agentName: AgentName,
    portalUrl: string,
    previousOutput: Record<string, unknown> | null,
): object {
    switch (agentName) {
        case "navigator":
            return { prompt: portalUrl };
        case "extractor":
            return { prompt: JSON.stringify(previousOutput) };
        case "csv_exporter":
            return { prompt: JSON.stringify(previousOutput) };
        case "analysis":
            return { prompt: (previousOutput as Record<string, unknown>)?.output_path ?? "" };
        default:
            return { prompt: "" };
    }
}

/**
 * Run the full 4-agent pipeline sequentially.
 *
 * All state is persisted to IndexedDB via the store helpers.
 * Returns the execution ID.
 */
export async function runPipeline(
    portalUrl: string,
    accessToken: string,
    callbacks?: PipelineCallbacks,
): Promise<string> {
    const executionId = uuidv4();

    await createExecution(executionId, portalUrl);
    await createStepsForExecution(executionId);
    callbacks?.onExecutionCreated?.(executionId);
    callbacks?.onExecutionChange?.();

    let previousOutput: Record<string, unknown> | null = null;

    for (const agentName of AGENT_ORDER) {
        const order = AGENTS[agentName].order;

        // Mark step running
        const payload = buildPayload(agentName, portalUrl, previousOutput);
        await markStepRunning(executionId, agentName, payload);
        await setCurrentAgent(executionId, agentName);
        callbacks?.onStepChange?.();

        const startTime = Date.now();

        try {
            // TODO: REMOVE — test hook to simulate extractor failure

            const rawResponse = await callAgentCore(agentName, payload, accessToken);
            const duration = Date.now() - startTime;

            // Decode multi-layer JSON
            const decoded = deepDecode(rawResponse) as Record<string, unknown>;

            // Check for explicit failure
            if (decoded.success === false) {
                throw new Error(
                    String(decoded.error ?? `${agentName} returned success=false`),
                );
            }

            // Extract insights
            const insights = extractInsights(agentName, decoded);

            // Persist success
            await markStepSuccess(executionId, agentName, decoded, duration, insights);
            callbacks?.onStepChange?.();

            // Derive state name from Navigator output
            if (agentName === "navigator") {
                const stateName =
                    String(decoded.state_name ?? decoded.portal_url ?? portalUrl)
                        .replace(/https?:\/\//, "")
                        .split(/[./]/)[0];
                await updateExecution(executionId, { stateName });
            }

            previousOutput = decoded;
        } catch (err: unknown) {
            console.log("Error...........", err)
            const duration = Date.now() - startTime;
            const message = err instanceof Error ? err.message : String(err);

            await markStepFailed(executionId, agentName, message, duration);
            await markRemainingSkipped(executionId, order);
            await setExecutionStatus(executionId, "failed", message);
            callbacks?.onStepChange?.();
            callbacks?.onExecutionChange?.();
            return executionId;
        }
    }

    // All agents succeeded
    await setCurrentAgent(executionId, null);
    await setExecutionStatus(executionId, "completed");
    callbacks?.onExecutionChange?.();
    return executionId;
}

/**
 * Retry the pipeline from a specific (failed) agent.
 *
 * Resets the failed step and all subsequent steps to "pending",
 * sets the execution back to "running", then re-runs the pipeline
 * from that agent onwards using the previous agent's stored output.
 */
export async function retryFromAgent(
    executionId: string,
    fromAgent: AgentName,
    accessToken: string,
    callbacks?: PipelineCallbacks,
): Promise<void> {
    const execution = await getExecution(executionId);
    if (!execution) throw new Error(`Execution ${executionId} not found`);

    const portalUrl = execution.portalUrl;
    const fromOrder = AGENTS[fromAgent].order;

    // Reset the failed step and all subsequent ones
    await resetStepsFrom(executionId, fromOrder);

    // Set execution back to running
    await setExecutionStatus(executionId, "running");
    await setCurrentAgent(executionId, fromAgent);
    callbacks?.onExecutionChange?.();

    // Get the previous agent's output (if any) to use as input
    let previousOutput: Record<string, unknown> | null = null;
    if (fromOrder > 0) {
        const steps = await getStepsForExecution(executionId);
        const prevStep = steps.find((s) => s.order === fromOrder - 1);
        if (prevStep?.output) {
            previousOutput = prevStep.output as Record<string, unknown>;
        }
    }

    // Run from the failed agent onwards
    const agentsToRun = AGENT_ORDER.filter((_, idx) => idx >= fromOrder);

    for (const agentName of agentsToRun) {
        const order = AGENTS[agentName].order;

        const payload = buildPayload(agentName, portalUrl, previousOutput);
        await markStepRunning(executionId, agentName, payload);
        await setCurrentAgent(executionId, agentName);
        callbacks?.onStepChange?.();

        const startTime = Date.now();

        try {
            const rawResponse = await callAgentCore(agentName, payload, accessToken);
            const duration = Date.now() - startTime;

            const decoded = deepDecode(rawResponse) as Record<string, unknown>;

            if (decoded.success === false) {
                throw new Error(
                    String(decoded.error ?? `${agentName} returned success=false`),
                );
            }

            const insights = extractInsights(agentName, decoded);

            await markStepSuccess(executionId, agentName, decoded, duration, insights);
            callbacks?.onStepChange?.();

            if (agentName === "navigator") {
                const stateName =
                    String(decoded.state_name ?? decoded.portal_url ?? portalUrl)
                        .replace(/https?:\/\//, "")
                        .split(/[./]/)[0];
                await updateExecution(executionId, { stateName });
            }

            previousOutput = decoded;
        } catch (err: unknown) {
            console.log("Retry error:", err);
            const duration = Date.now() - startTime;
            const message = err instanceof Error ? err.message : String(err);

            await markStepFailed(executionId, agentName, message, duration);
            await markRemainingSkipped(executionId, order);
            await setExecutionStatus(executionId, "failed", message);
            callbacks?.onStepChange?.();
            callbacks?.onExecutionChange?.();
            return;
        }
    }

    // All retried agents succeeded
    await setCurrentAgent(executionId, null);
    await setExecutionStatus(executionId, "completed");
    callbacks?.onExecutionChange?.();
}

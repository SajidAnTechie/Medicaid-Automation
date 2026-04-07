import { db } from "./database";
import type { AgentStep, StepStatus, AgentInsights, AgentName } from "../types";
import { AGENT_ORDER } from "../types";

export async function createStepsForExecution(executionId: string): Promise<AgentStep[]> {
    const steps: AgentStep[] = AGENT_ORDER.map((agentName, idx) => ({
        id: `${executionId}-${agentName}`,
        executionId,
        agentName,
        order: idx,
        status: "pending" as StepStatus,
        startedAt: null,
        completedAt: null,
        durationMs: null,
        input: null,
        output: null,
        error: null,
        insights: null,
    }));

    await db.agentSteps.bulkAdd(steps);
    return steps;
}

export async function updateStep(
    id: string,
    updates: Partial<AgentStep>,
): Promise<void> {
    await db.agentSteps.update(id, updates);
}

export async function markStepRunning(
    executionId: string,
    agentName: AgentName,
    input: unknown,
): Promise<void> {
    const id = `${executionId}-${agentName}`;
    await db.agentSteps.update(id, {
        status: "running",
        startedAt: new Date().toISOString(),
        input,
    });
}

export async function markStepSuccess(
    executionId: string,
    agentName: AgentName,
    output: unknown,
    durationMs: number,
    insights: AgentInsights | null,
): Promise<void> {
    const id = `${executionId}-${agentName}`;
    await db.agentSteps.update(id, {
        status: "success",
        completedAt: new Date().toISOString(),
        durationMs,
        output,
        insights,
    });
}

export async function markStepFailed(
    executionId: string,
    agentName: AgentName,
    error: string,
    durationMs: number,
): Promise<void> {
    const id = `${executionId}-${agentName}`;
    await db.agentSteps.update(id, {
        status: "failed",
        completedAt: new Date().toISOString(),
        durationMs,
        error,
    });
}

export async function markRemainingSkipped(
    executionId: string,
    afterOrder: number,
): Promise<void> {
    const steps = await db.agentSteps
        .where("executionId")
        .equals(executionId)
        .toArray();

    const toSkip = steps.filter(
        (s) => s.order > afterOrder && s.status === "pending",
    );

    await Promise.all(
        toSkip.map((s) =>
            db.agentSteps.update(s.id, { status: "skipped" }),
        ),
    );
}

/**
 * Reset a step and all subsequent steps back to "pending",
 * clearing runtime fields so the pipeline can be retried.
 */
export async function resetStepsFrom(
    executionId: string,
    fromOrder: number,
): Promise<void> {
    const steps = await db.agentSteps
        .where("executionId")
        .equals(executionId)
        .toArray();

    const toReset = steps.filter((s) => s.order >= fromOrder);

    await Promise.all(
        toReset.map((s) =>
            db.agentSteps.update(s.id, {
                status: "pending",
                startedAt: null,
                completedAt: null,
                durationMs: null,
                input: null,
                output: null,
                error: null,
                insights: null,
            }),
        ),
    );
}

export async function getStepsForExecution(
    executionId: string,
): Promise<AgentStep[]> {
    return db.agentSteps
        .where("executionId")
        .equals(executionId)
        .sortBy("order");
}

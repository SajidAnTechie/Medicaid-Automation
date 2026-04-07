import { db } from "./database";
import type { Execution, ExecutionStatus, AgentName } from "../types";

export async function createExecution(
    id: string,
    portalUrl: string,
): Promise<Execution> {
    const execution: Execution = {
        id,
        portalUrl,
        stateName: "",
        status: "running",
        createdAt: new Date().toISOString(),
        completedAt: null,
        totalDurationMs: null,
        error: null,
        currentAgent: "navigator",
    };
    await db.executions.add(execution);
    return execution;
}

export async function updateExecution(
    id: string,
    updates: Partial<Execution>,
): Promise<void> {
    await db.executions.update(id, updates);
}

export async function setExecutionStatus(
    id: string,
    status: ExecutionStatus,
    error?: string,
): Promise<void> {
    const now = new Date().toISOString();
    const exec = await db.executions.get(id);
    const totalDurationMs = exec
        ? Date.now() - new Date(exec.createdAt).getTime()
        : null;

    await db.executions.update(id, {
        status,
        completedAt: status === "running" ? null : now,
        totalDurationMs: status === "running" ? null : totalDurationMs,
        error: error ?? null,
        currentAgent: status === "running" ? exec?.currentAgent : null,
    });
}

export async function setCurrentAgent(
    id: string,
    agent: AgentName | null,
): Promise<void> {
    await db.executions.update(id, { currentAgent: agent });
}

export async function getExecution(id: string): Promise<Execution | undefined> {
    return db.executions.get(id);
}

export async function getAllExecutions(): Promise<Execution[]> {
    return db.executions.orderBy("createdAt").reverse().toArray();
}

export async function deleteExecution(id: string): Promise<void> {
    await db.transaction("rw", db.executions, db.agentSteps, async () => {
        await db.agentSteps.where("executionId").equals(id).delete();
        await db.executions.delete(id);
    });
}

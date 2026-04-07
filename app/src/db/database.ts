import Dexie, { type EntityTable } from "dexie";
import type { Execution, AgentStep } from "../types";

// ── Database Schema ──────────────────────────────────────────────────────────

export class PipelineDB extends Dexie {
    executions!: EntityTable<Execution, "id">;
    agentSteps!: EntityTable<AgentStep, "id">;

    constructor() {
        super("MedicaidPipelineDB");

        this.version(1).stores({
            executions: "id, status, createdAt, stateName",
            agentSteps: "id, executionId, agentName, [executionId+order]",
        });
    }
}

export const db = new PipelineDB();

import { z } from "zod";
import { genObject } from "./run";
import type { EventBus } from "./bus";
import type { SwarmPlan } from "./types";

const planSchema = z.object({
  summary: z.string().describe("One-sentence read of the goal and the strategy to solve it."),
  tasks: z
    .array(
      z.object({
        id: z.string().describe("Short stable id, e.g. 't1'."),
        type: z
          .enum(["researcher", "analyst", "writer", "coder"])
          .describe("Which specialist agent should own this task."),
        title: z.string().describe("3-6 word task name shown on the graph node."),
        brief: z.string().describe("Clear, self-contained instruction for the worker agent."),
        dependsOn: z
          .array(z.string())
          .describe("Ids of tasks whose output this task needs. Empty for root tasks."),
      }),
    )
    .min(2)
    .max(4),
  synthesisBrief: z
    .string()
    .describe("How to fuse all worker outputs into one polished final deliverable."),
});

const SYSTEM = `You are the Planner of an autonomous agent swarm.
Decompose the user's goal into a small DAG (2-4 tasks) of specialist subtasks that
together fully solve it. STRONGLY maximize PARALLELISM: prefer independent tasks with
NO dependencies so they all run at once. Only add a dependency when a task genuinely
needs another's output; avoid long dependency chains. Each task must be self-contained and assigned to
the best-fit specialist: researcher (gather/structure knowledge), analyst (reason,
compare, evaluate trade-offs), writer (produce prose/sections), coder (produce code
or technical specs). Keep the graph tight — no redundant tasks.`;

/** Generic degrade-gracefully plan used if every planner model is rate-limited/unavailable. */
function fallbackPlan(goal: string): SwarmPlan {
  return {
    goal,
    summary: "Default research → analysis → write plan (planner models were unavailable).",
    tasks: [
      { id: "t1", type: "researcher", title: "Gather context", brief: `Research everything relevant to: ${goal}. Facts, landscape, constraints, players.`, dependsOn: [] },
      { id: "t2", type: "analyst", title: "Analyze & recommend", brief: `Reason through the key decisions and trade-offs for: ${goal}, and give concrete recommendations.`, dependsOn: [] },
    ],
    synthesisBrief: `Combine the sections into one cohesive, well-structured answer to: ${goal}.`,
  };
}

export async function plan(goal: string, bus: EventBus): Promise<SwarmPlan> {
  bus.emit({ kind: "plan.start" });

  let plan: SwarmPlan;
  try {
    const { object } = await genObject("planner", {
      schema: planSchema,
      system: SYSTEM,
      prompt: `Goal:\n${goal}`,
    });
    plan = { goal, ...object };
    bus.emit({ kind: "plan.token", delta: plan.summary });
  } catch {
    plan = fallbackPlan(goal);
    bus.emit({ kind: "plan.token", delta: plan.summary });
  }

  bus.emit({ kind: "plan.done", plan });
  return plan;
}

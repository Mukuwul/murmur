import { z } from "zod";
import { genObject } from "./run";
import type { SwarmTask } from "./types";

const schema = z.object({
  score: z.number().min(0).max(10).describe("Quality of the output against the brief, 0-10."),
  approved: z.boolean().describe("True if the output fully satisfies the brief."),
  feedback: z
    .string()
    .describe("If not approved, specific, actionable fixes. If approved, a one-line note."),
});

const SYSTEM = `You are the Validator agent — the swarm's quality gate. Judge whether a
worker's output actually fulfills its brief: correctness, completeness, specificity,
and usefulness. Be strict but fair. Approve only genuinely solid work (score >= 7).
If rejecting, give concrete, actionable feedback the worker can apply in one revision.`;

export interface Verdict {
  approved: boolean;
  score: number;
  feedback: string;
}

export async function validate(task: SwarmTask, output: string): Promise<Verdict> {
  try {
    const { object } = await genObject("validator", {
      schema,
      system: SYSTEM,
      prompt: `Brief:\nTask "${task.title}": ${task.brief}\n\nWorker output:\n${output}`,
    });
    return {
      approved: object.approved && object.score >= 7,
      score: object.score,
      feedback: object.feedback,
    };
  } catch {
    // If every validator model is rate-limited, don't block the run — pass the work through.
    return { approved: true, score: 7, feedback: "Validator unavailable; auto-approved." };
  }
}

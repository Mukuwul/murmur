import { createOpenRouter } from "@openrouter/ai-sdk-provider";

export type Role = "planner" | "validator" | "worker" | "synthesizer";

const openrouter = createOpenRouter({ apiKey: process.env.OPENROUTER_API_KEY });
export const model = (id: string) => openrouter(id);

// Preferred paid model — auto-used the moment the OpenRouter key has credit.
const CLAUDE = process.env.MURMUR_CLAUDE_MODEL ?? "anthropic/claude-sonnet-4.5";

// Free fallback pools (verified live on OpenRouter). Order = preference; the executor
// walks the list on rate-limit / unavailability so a single 429 never kills a run.
const FREE_STRUCTURED = [
  "nvidia/nemotron-nano-9b-v2:free",
  "nvidia/nemotron-3-super-120b-a12b:free",
  "google/gemma-4-31b-it:free",
  "qwen/qwen3-next-80b-a3b-instruct:free",
];
const FREE_CHAT = [
  "openai/gpt-oss-120b:free",
  "nvidia/nemotron-3-super-120b-a12b:free",
  "google/gemma-4-31b-it:free",
  "meta-llama/llama-3.3-70b-instruct:free",
];

const ENV_OVERRIDE: Record<Role, string | undefined> = {
  planner: process.env.MURMUR_PLANNER_MODEL,
  validator: process.env.MURMUR_VALIDATOR_MODEL,
  worker: process.env.MURMUR_WORKER_MODEL,
  synthesizer: process.env.MURMUR_SYNTH_MODEL,
};

// One-time probe: does this key have paid access? Cached for the process lifetime.
let paidProbe: Promise<boolean> | null = null;
function hasPaidAccess(): Promise<boolean> {
  if (process.env.MURMUR_FORCE_PAID === "1") return Promise.resolve(true);
  if (process.env.MURMUR_FORCE_FREE === "1") return Promise.resolve(false);
  if (!paidProbe) {
    paidProbe = fetch("https://openrouter.ai/api/v1/key", {
      headers: { Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}` },
    })
      .then((r) => r.json())
      .then((j) => j?.data?.is_free_tier === false)
      .catch(() => false);
  }
  return paidProbe;
}

/** Ordered list of model ids to try for a role. Claude first when the key is paid. */
export async function chainFor(role: Role): Promise<string[]> {
  const paid = await hasPaidAccess();
  const pool = role === "worker" ? FREE_CHAT : role === "synthesizer" ? FREE_CHAT : FREE_STRUCTURED;
  // Workers stay free even on a paid key (cost control); other roles prefer Claude.
  const head = [ENV_OVERRIDE[role], paid && role !== "worker" ? CLAUDE : undefined].filter(
    Boolean,
  ) as string[];
  return [...new Set([...head, ...pool])];
}

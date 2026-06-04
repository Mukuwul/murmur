import { createOpenRouter } from "@openrouter/ai-sdk-provider";

// All models route through OpenRouter (one key, many providers).
// "Mixed" setup: structured-output roles (planner, validator) run on a capable PAID
// Claude model; plain-text roles (worker, synthesizer) run on FREE models.
// Every slug is env-overridable — verify exact availability at https://openrouter.ai/models.
const PLANNER = process.env.MURMUR_PLANNER_MODEL ?? "anthropic/claude-sonnet-4.5";
const VALIDATOR = process.env.MURMUR_VALIDATOR_MODEL ?? "anthropic/claude-sonnet-4.5";
const WORKER = process.env.MURMUR_WORKER_MODEL ?? "meta-llama/llama-3.3-70b-instruct:free";
const SYNTH = process.env.MURMUR_SYNTH_MODEL ?? "meta-llama/llama-3.3-70b-instruct:free";

const openrouter = createOpenRouter({ apiKey: process.env.OPENROUTER_API_KEY });

export const models = {
  planner: openrouter(PLANNER),
  worker: openrouter(WORKER),
  validator: openrouter(VALIDATOR),
  synthesizer: openrouter(SYNTH),
};

export const MODEL_NAMES = { PLANNER, WORKER, VALIDATOR, SYNTH };

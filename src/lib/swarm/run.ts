import { generateObject, streamObject, streamText } from "ai";
import type { z } from "zod";
import { chainFor, model, type Role } from "./models";

// Per-attempt wall-clock budget. A model that accepts the request but never returns
// (common on congested free tiers) is aborted so the executor falls through.
const TIMEOUT_MS = Number(process.env.MURMUR_ATTEMPT_TIMEOUT_MS ?? 40000);

// Treat rate-limit (429), payment-required (402, i.e. paid model on a free key),
// timeout (408), and 5xx as "try the next model in the chain".
function shouldFallback(e: unknown): boolean {
  const err = e as { statusCode?: number; status?: number; message?: string };
  const code = err?.statusCode ?? err?.status;
  if (code && (code === 429 || code === 402 || code === 408 || code >= 500)) return true;
  const m = (err?.message ?? "").toLowerCase();
  return m.includes("rate-limit") || m.includes("rate limit") || m.includes("429") || m.includes("quota");
}

class AllModelsFailed extends Error {
  constructor(role: string, last: unknown) {
    super(`All models failed for ${role}: ${(last as Error)?.message ?? last}`);
  }
}

/** Stream plain text, walking the model chain on rate-limit/unavailability. */
export async function runText(
  role: Role,
  opts: { system: string; prompt: string; onDelta: (d: string) => void },
): Promise<{ text: string; modelId: string }> {
  const chain = await chainFor(role);
  let last: unknown;
  for (const id of chain) {
    try {
      const res = streamText({
        model: model(id),
        system: opts.system,
        prompt: opts.prompt,
        maxRetries: 0,
        abortSignal: AbortSignal.timeout(TIMEOUT_MS),
      });
      let text = "";
      for await (const delta of res.textStream) {
        text += delta;
        opts.onDelta(delta);
      }
      if (!text.trim()) throw new Error("empty completion");
      return { text, modelId: id };
    } catch (e) {
      last = e;
      if (!shouldFallback(e)) continue; // try next model regardless; only bail when chain is exhausted
    }
  }
  throw new AllModelsFailed(role, last);
}

/** Stream a structured object (used by the planner), walking the chain on failure. */
export async function runObject<T>(
  role: Role,
  opts: { schema: z.ZodType<T>; system: string; prompt: string; onPartial?: (o: Partial<T>) => void },
): Promise<{ object: T; modelId: string }> {
  const chain = await chainFor(role);
  let last: unknown;
  for (const id of chain) {
    try {
      const res = streamObject({
        model: model(id),
        schema: opts.schema,
        system: opts.system,
        prompt: opts.prompt,
        maxRetries: 0,
        abortSignal: AbortSignal.timeout(TIMEOUT_MS),
      });
      if (opts.onPartial) {
        for await (const partial of res.partialObjectStream) opts.onPartial(partial as Partial<T>);
      }
      return { object: (await res.object) as T, modelId: id };
    } catch (e) {
      last = e;
    }
  }
  throw new AllModelsFailed(role, last);
}

/** Non-streaming structured object (used by the validator), walking the chain on failure. */
export async function genObject<T>(
  role: Role,
  opts: { schema: z.ZodType<T>; system: string; prompt: string },
): Promise<{ object: T; modelId: string }> {
  const chain = await chainFor(role);
  let last: unknown;
  for (const id of chain) {
    try {
      const { object } = await generateObject({
        model: model(id),
        schema: opts.schema,
        system: opts.system,
        prompt: opts.prompt,
        maxRetries: 0,
        abortSignal: AbortSignal.timeout(TIMEOUT_MS),
      });
      return { object: object as T, modelId: id };
    } catch (e) {
      last = e;
    }
  }
  throw new AllModelsFailed(role, last);
}

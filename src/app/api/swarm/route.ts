import { EventBus } from "@/lib/swarm/bus";
import { runSwarm } from "@/lib/swarm/orchestrator";
import { enforceRateLimit, rateLimitKey, RateLimitError, RUN_RATE_LIMIT } from "@/lib/swarm/rateLimit";

export const runtime = "nodejs";
export const maxDuration = 300;

export async function POST(req: Request) {
  // Defensive parsing: invalid JSON becomes an empty goal so validation below
  // returns a clean 400 instead of crashing the route.
  const { goal } = await req.json().catch(() => ({ goal: "" }));
  if (!goal || typeof goal !== "string" || goal.trim().length < 4) {
    return new Response(JSON.stringify({ error: "Provide a goal (min 4 chars)." }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }
  if (!process.env.OPENROUTER_API_KEY) {
    // API keys stay server-side in Next.js route handlers. Never expose this
    // key from a client component.
    return new Response(JSON.stringify({ error: "OPENROUTER_API_KEY is not set on the server." }), {
      status: 500,
      headers: { "content-type": "application/json" },
    });
  }

  const clientId =
    req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ??
    req.headers.get("x-real-ip") ??
    "local";

  try {
    await enforceRateLimit({
      key: rateLimitKey("runs", clientId),
      ...RUN_RATE_LIMIT,
    });
  } catch (e) {
    if (e instanceof RateLimitError) {
      return new Response(JSON.stringify({ error: e.message, retryAfterSeconds: e.retryAfterSeconds }), {
        status: 429,
        headers: {
          "content-type": "application/json",
          "retry-after": String(e.retryAfterSeconds),
        },
      });
    }
    return new Response(JSON.stringify({ error: "Rate limiter unavailable." }), {
      status: 503,
      headers: { "content-type": "application/json" },
    });
  }

  const runId = crypto.randomUUID();
  const bus = new EventBus(runId);
  const encoder = new TextEncoder();

  // Kick off the run; failures are surfaced as an error event then close.
  // The route returns the stream immediately while runSwarm keeps emitting.
  runSwarm(goal.trim(), bus).catch(async (e) => {
    bus.emit({ kind: "error", message: (e as Error).message });
    try {
      await bus.close("failed");
    } catch (deliveryError) {
      console.error("Failed to persist swarm failure", deliveryError);
    }
  });

  const stream = new ReadableStream({
    async start(controller) {
      try {
        for await (const event of bus) {
          // Server-Sent Events frame format. The blank line terminates one event.
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
        }
      } catch (e) {
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify({ kind: "error", message: (e as Error).message })}\n\n`),
        );
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      // text/event-stream tells the browser/client this is a long-lived SSE stream.
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      "x-murmur-run-id": runId,
    },
  });
}

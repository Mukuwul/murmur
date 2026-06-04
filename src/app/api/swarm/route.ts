import { EventBus } from "@/lib/swarm/bus";
import { runSwarm } from "@/lib/swarm/orchestrator";

export const runtime = "nodejs";
export const maxDuration = 300;

export async function POST(req: Request) {
  const { goal } = await req.json().catch(() => ({ goal: "" }));
  if (!goal || typeof goal !== "string" || goal.trim().length < 4) {
    return new Response(JSON.stringify({ error: "Provide a goal (min 4 chars)." }), {
      status: 400,
      headers: { "content-type": "application/json" },
    });
  }
  if (!process.env.OPENROUTER_API_KEY) {
    return new Response(JSON.stringify({ error: "OPENROUTER_API_KEY is not set on the server." }), {
      status: 500,
      headers: { "content-type": "application/json" },
    });
  }

  const bus = new EventBus();
  const encoder = new TextEncoder();

  // Kick off the run; failures are surfaced as an error event then close.
  runSwarm(goal.trim(), bus).catch((e) => {
    bus.emit({ kind: "error", message: (e as Error).message });
    bus.close();
  });

  const stream = new ReadableStream({
    async start(controller) {
      try {
        for await (const event of bus) {
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
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
    },
  });
}

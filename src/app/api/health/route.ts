import { getInfrastructureHealth } from "@/lib/swarm/infrastructure";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const health = await getInfrastructureHealth({ force: true });
  return Response.json(
    {
      status: health.ok ? "ready" : "not_ready",
      checkedAt: new Date(health.checkedAt).toISOString(),
      dependencies: {
        kafka: { ok: health.kafka.ok, latencyMs: health.kafka.latencyMs },
        redis: { ok: health.redis.ok, latencyMs: health.redis.latencyMs },
      },
    },
    {
      status: health.ok ? 200 : 503,
      headers: { "cache-control": "no-store" },
    },
  );
}

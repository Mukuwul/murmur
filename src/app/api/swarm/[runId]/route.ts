import { getRunEvents, getRunSession } from "@/lib/swarm/session";

export const runtime = "nodejs";

export async function GET(_req: Request, context: { params: Promise<{ runId: string }> }) {
  const { runId } = await context.params;
  if (!/^[0-9a-f-]{36}$/i.test(runId)) {
    return Response.json({ error: "Invalid run id." }, { status: 400 });
  }

  try {
    const session = await getRunSession(runId);
    if (!session) return Response.json({ error: "Run not found or expired." }, { status: 404 });
    return Response.json({ session, events: await getRunEvents(runId) });
  } catch {
    return Response.json(
      { error: "Required Redis session storage is unavailable." },
      { status: 503, headers: { "retry-after": "5" } },
    );
  }
}

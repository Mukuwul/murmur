import { getRunEvents, getRunSession } from "@/lib/swarm/session";

export const runtime = "nodejs";

export async function GET(_req: Request, context: { params: Promise<{ runId: string }> }) {
  const { runId } = await context.params;
  const session = await getRunSession(runId);
  if (!session) {
    return Response.json({ error: "Run not found or Redis session storage is disabled." }, { status: 404 });
  }

  return Response.json({ session, events: await getRunEvents(runId) });
}

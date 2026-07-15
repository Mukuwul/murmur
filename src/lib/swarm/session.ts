import type { SwarmEvent } from "./types";
import { getRedis } from "./redis";

export type RunStatus = "running" | "completed" | "failed";

export interface SwarmEventEnvelope {
  version: 1;
  id: string;
  runId: string;
  sequence: number;
  occurredAt: number;
  event: SwarmEvent;
}

export interface RunSession {
  runId: string;
  status: RunStatus;
  goal?: string;
  startedAt?: number;
  completedAt?: number;
  eventCount: number;
  final?: string;
  lastError?: string;
}

const SESSION_TTL_SECONDS = intEnv("MURMUR_RUN_SESSION_TTL_SECONDS", 86_400);
const EVENT_STREAM_MAX_LENGTH = intEnv("MURMUR_RUN_EVENT_STREAM_MAX_LENGTH", 10_000);

// One atomic Redis script keeps the session projection and append-only stream in
// sync. sequence-0 is a deterministic stream ID, so a retried event is ignored
// instead of being appended twice.
const PERSIST_EVENT = `
  local sequence = tonumber(ARGV[1])
  local current = tonumber(redis.call("HGET", KEYS[1], "eventCount") or "0")
  if current >= sequence then
    return 0
  end

  for i = 6, #ARGV, 2 do
    redis.call("HSET", KEYS[1], ARGV[i], ARGV[i + 1])
  end
  redis.call("EXPIRE", KEYS[1], ARGV[2])
  redis.call("XADD", KEYS[2], "MAXLEN", "~", ARGV[3], ARGV[4], "envelope", ARGV[5])
  redis.call("EXPIRE", KEYS[2], ARGV[2])
  return 1
`;

function intEnv(name: string, fallback: number) {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function sessionKey(runId: string) {
  return `murmur:run:${runId}`;
}

function eventStreamKey(runId: string) {
  return `murmur:run:${runId}:events`;
}

/** Stores an append-only event stream and the current run projection in Redis. */
export async function persistRunEvent(envelope: SwarmEventEnvelope) {
  const redis = await getRedis();

  const session = sessionKey(envelope.runId);
  const eventStream = eventStreamKey(envelope.runId);
  const now = String(envelope.occurredAt);
  const fields: string[] = [
    "runId",
    envelope.runId,
    "status",
    "running",
    "eventCount",
    String(envelope.sequence),
    "updatedAt",
    now,
  ];

  if (envelope.event.kind === "run.start") {
    fields.push("goal", envelope.event.goal, "startedAt", String(envelope.event.at));
  }
  if (envelope.event.kind === "run.done") {
    fields.push("status", "completed", "completedAt", now, "final", envelope.event.final);
  }
  if (envelope.event.kind === "error" && !envelope.event.agentId) {
    fields.push("lastError", envelope.event.message);
  }

  await redis.eval(
    PERSIST_EVENT,
    2,
    session,
    eventStream,
    envelope.sequence,
    SESSION_TTL_SECONDS,
    EVENT_STREAM_MAX_LENGTH,
    `${envelope.sequence}-0`,
    JSON.stringify(envelope),
    ...fields,
  );
}

export async function finishRunSession(runId: string, status: Exclude<RunStatus, "running">) {
  const redis = await getRedis();

  const now = Date.now();
  await redis
    .multi()
    .hset(sessionKey(runId), "status", status, "completedAt", String(now), "updatedAt", String(now))
    .expire(sessionKey(runId), SESSION_TTL_SECONDS)
    .expire(eventStreamKey(runId), SESSION_TTL_SECONDS)
    .exec();
}

export async function getRunSession(runId: string): Promise<RunSession | null> {
  const redis = await getRedis();

  const data = await redis.hgetall(sessionKey(runId));
  if (!data.runId) return null;
  return {
    runId: data.runId,
    status: (data.status as RunStatus) ?? "running",
    goal: data.goal,
    startedAt: numberOrUndefined(data.startedAt),
    completedAt: numberOrUndefined(data.completedAt),
    eventCount: Number(data.eventCount ?? 0),
    final: data.final,
    lastError: data.lastError,
  };
}

export async function getRunEvents(runId: string, count = 1_000): Promise<SwarmEventEnvelope[]> {
  const redis = await getRedis();

  const entries = await redis.xrange(eventStreamKey(runId), "-", "+", "COUNT", count);
  return entries.flatMap(([, values]) => {
    const index = values.indexOf("envelope");
    if (index < 0) return [];
    const value = values[index + 1];
    if (!value) return [];
    try {
      return [JSON.parse(value) as SwarmEventEnvelope];
    } catch {
      return [];
    }
  });
}

function numberOrUndefined(value: string | undefined) {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

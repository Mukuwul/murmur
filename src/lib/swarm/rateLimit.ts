import { getRedis } from "./redis";

export class RateLimitError extends Error {
  constructor(
    message: string,
    readonly retryAfterSeconds: number,
  ) {
    super(message);
    this.name = "RateLimitError";
  }
}

interface LimitOptions {
  key: string;
  limit: number;
  windowSeconds: number;
}

function intEnv(name: string, fallback: number) {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export const RUN_RATE_LIMIT = {
  limit: intEnv("MURMUR_RUNS_PER_WINDOW", 20),
  windowSeconds: intEnv("MURMUR_RUN_WINDOW_SECONDS", 3600),
};

export const MODEL_RATE_LIMIT = {
  limit: intEnv("MURMUR_MODEL_CALLS_PER_WINDOW", 120),
  windowSeconds: intEnv("MURMUR_MODEL_WINDOW_SECONDS", 3600),
};

const INCREMENT_WITH_EXPIRY = `
  local count = redis.call("INCR", KEYS[1])
  if count == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
  end
  return { count, redis.call("TTL", KEYS[1]) }
`;

export async function enforceRateLimit({ key, limit, windowSeconds }: LimitOptions) {
  const client = await getRedis();
  if (!client) return;

  try {
    const [count, ttl] = (await client.eval(INCREMENT_WITH_EXPIRY, 1, key, windowSeconds)) as [number, number];
    if (count > limit) {
      throw new RateLimitError("Rate limit exceeded.", Math.max(ttl, 1));
    }
  } catch (e) {
    if (e instanceof RateLimitError) throw e;
    if (process.env.MURMUR_STRICT_REDIS_RATE_LIMIT === "1") throw e;
  }
}

export function rateLimitKey(scope: string, id: string) {
  return `murmur:rate:${scope}:${id}`;
}

import { getInfrastructureHealthCacheMs, InfrastructureConfigError } from "./config";
import { pingKafka } from "./kafka";
import { pingRedis } from "./redis";

export interface DependencyHealth {
  ok: boolean;
  latencyMs: number;
  error?: string;
}

export interface InfrastructureHealth {
  ok: boolean;
  checkedAt: number;
  kafka: DependencyHealth;
  redis: DependencyHealth;
}

export class InfrastructureUnavailableError extends Error {
  constructor(readonly health: InfrastructureHealth) {
    super("Required Kafka or Redis infrastructure is unavailable.");
    this.name = "InfrastructureUnavailableError";
  }
}

let cached: { expiresAt: number; value: InfrastructureHealth } | null = null;
let inFlight: Promise<InfrastructureHealth> | null = null;

async function checkDependency(check: () => Promise<void>): Promise<DependencyHealth> {
  const startedAt = Date.now();
  try {
    await check();
    return { ok: true, latencyMs: Date.now() - startedAt };
  } catch (error) {
    const message = error instanceof InfrastructureConfigError ? error.message : (error as Error).message;
    return { ok: false, latencyMs: Date.now() - startedAt, error: message };
  }
}

export async function getInfrastructureHealth(options: { force?: boolean } = {}) {
  const now = Date.now();
  if (!options.force && cached && cached.expiresAt > now) return cached.value;
  // Share an active probe even when a caller asks to bypass the cached result.
  if (inFlight) return inFlight;

  inFlight = Promise.all([checkDependency(pingKafka), checkDependency(pingRedis)])
    .then(([kafka, redis]) => {
      const value: InfrastructureHealth = {
        ok: kafka.ok && redis.ok,
        checkedAt: Date.now(),
        kafka,
        redis,
      };
      cached = { expiresAt: Date.now() + getInfrastructureHealthCacheMs(), value };
      return value;
    })
    .finally(() => {
      inFlight = null;
    });

  return inFlight;
}

export async function assertInfrastructureReady() {
  const health = await getInfrastructureHealth();
  if (!health.ok) throw new InfrastructureUnavailableError(health);
  return health;
}

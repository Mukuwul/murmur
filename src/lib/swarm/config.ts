import type { SASLOptions } from "kafkajs";

export class InfrastructureConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InfrastructureConfigError";
  }
}

export interface KafkaInfrastructureConfig {
  brokers: string[];
  clientId: string;
  topic: string;
  ssl: boolean;
  sasl?: SASLOptions;
  connectionTimeoutMs: number;
  requestTimeoutMs: number;
  retryCount: number;
}

export interface RedisInfrastructureConfig {
  url: string;
  connectTimeoutMs: number;
  commandTimeoutMs: number;
}

function required(name: string) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new InfrastructureConfigError(`${name} is required. Start the infrastructure stack and configure the server.`);
  }
  return value;
}

function positiveInt(name: string, fallback: number) {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new InfrastructureConfigError(`${name} must be a positive integer.`);
  }
  return value;
}

function bool(name: string, fallback = false) {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes"].includes(raw)) return true;
  if (["0", "false", "no"].includes(raw)) return false;
  throw new InfrastructureConfigError(`${name} must be 1/0, true/false, or yes/no.`);
}

function kafkaSasl(): SASLOptions | undefined {
  const username = process.env.KAFKA_USERNAME?.trim();
  const password = process.env.KAFKA_PASSWORD?.trim();
  if (!username && !password) return undefined;
  if (!username || !password) {
    throw new InfrastructureConfigError("KAFKA_USERNAME and KAFKA_PASSWORD must be configured together.");
  }

  const mechanism = process.env.KAFKA_SASL_MECHANISM?.trim() || "plain";
  if (mechanism === "plain" || mechanism === "scram-sha-256" || mechanism === "scram-sha-512") {
    return { mechanism, username, password };
  }
  throw new InfrastructureConfigError(
    "KAFKA_SASL_MECHANISM must be plain, scram-sha-256, or scram-sha-512.",
  );
}

export function getKafkaConfig(): KafkaInfrastructureConfig {
  const brokers = required("KAFKA_BROKERS")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);
  if (brokers.length === 0) throw new InfrastructureConfigError("KAFKA_BROKERS must contain at least one broker.");

  const topic = required("KAFKA_SWARM_EVENTS_TOPIC");
  if (!/^[a-zA-Z0-9._-]+$/.test(topic)) {
    throw new InfrastructureConfigError("KAFKA_SWARM_EVENTS_TOPIC contains unsupported characters.");
  }

  return {
    brokers,
    clientId: process.env.KAFKA_CLIENT_ID?.trim() || "murmur-web",
    topic,
    ssl: bool("KAFKA_SSL"),
    sasl: kafkaSasl(),
    connectionTimeoutMs: positiveInt("KAFKA_CONNECTION_TIMEOUT_MS", 5_000),
    requestTimeoutMs: positiveInt("KAFKA_REQUEST_TIMEOUT_MS", 30_000),
    retryCount: positiveInt("KAFKA_RETRY_COUNT", 8),
  };
}

export function getRedisConfig(): RedisInfrastructureConfig {
  const url = required("REDIS_URL");
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new InfrastructureConfigError("REDIS_URL must be a valid redis:// or rediss:// URL.");
  }
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new InfrastructureConfigError("REDIS_URL must use redis:// or rediss://.");
  }

  return {
    url,
    connectTimeoutMs: positiveInt("REDIS_CONNECT_TIMEOUT_MS", 5_000),
    commandTimeoutMs: positiveInt("REDIS_COMMAND_TIMEOUT_MS", 3_000),
  };
}

export function getInfrastructureHealthCacheMs() {
  return positiveInt("MURMUR_INFRA_HEALTH_CACHE_MS", 5_000);
}

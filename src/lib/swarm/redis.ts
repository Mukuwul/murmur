import Redis from "ioredis";
import { getRedisConfig } from "./config";

let redisClient: Redis | null = null;
let redisPromise: Promise<Redis> | null = null;

/** Shares one required connection across rate limits and durable run state. */
export function getRedis(): Promise<Redis> {
  if (!redisPromise) {
    const config = getRedisConfig();
    const client = new Redis(config.url, {
      maxRetriesPerRequest: 1,
      lazyConnect: true,
      enableOfflineQueue: false,
      autoResendUnfulfilledCommands: false,
      connectTimeout: config.connectTimeoutMs,
      commandTimeout: config.commandTimeoutMs,
      keepAlive: 10_000,
      connectionName: "murmur-web",
      retryStrategy: (attempt) => Math.min(attempt * 100, 2_000),
    });
    redisClient = client;
    client.on("error", (error) => console.error("Redis connection error", error.message));
    client.on("end", () => {
      if (redisClient === client) {
        redisClient = null;
        redisPromise = null;
      }
    });
    redisPromise = client.connect().then(
      () => client,
      (error) => {
        redisClient = null;
        redisPromise = null;
        client.disconnect();
        throw error;
      },
    );
  }
  return redisPromise;
}

export async function pingRedis() {
  const redis = await getRedis();
  const response = await redis.ping();
  if (response !== "PONG") throw new Error("Redis did not return PONG.");
}

export async function disconnectRedis() {
  const client = redisClient;
  redisClient = null;
  redisPromise = null;
  if (client && client.status !== "end") client.disconnect();
}

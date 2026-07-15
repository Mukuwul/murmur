import Redis from "ioredis";

const redisUrl = process.env.REDIS_URL;
let redisPromise: Promise<Redis | null> | null = null;

/** Shares one connection across rate limits and durable run state. */
export function getRedis(): Promise<Redis | null> {
  if (!redisUrl) return Promise.resolve(null);
  if (!redisPromise) {
    const client = new Redis(redisUrl, {
      maxRetriesPerRequest: 1,
      lazyConnect: true,
      enableOfflineQueue: false,
    });
    redisPromise = client.connect().then(
      () => client,
      (error) => {
        redisPromise = null;
        client.disconnect();
        throw error;
      },
    );
  }
  return redisPromise;
}

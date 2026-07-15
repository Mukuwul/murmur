import { Kafka, type Producer } from "kafkajs";
import type { SwarmEventEnvelope } from "./session";

const brokers = process.env.KAFKA_BROKERS?.split(",")
  .map((b) => b.trim())
  .filter(Boolean);

const topic = process.env.KAFKA_SWARM_EVENTS_TOPIC ?? "murmur.swarm.events";

let producerPromise: Promise<Producer | null> | null = null;

function getProducer() {
  if (!brokers?.length) return Promise.resolve(null);
  if (!producerPromise) {
    const kafka = new Kafka({
      clientId: process.env.KAFKA_CLIENT_ID ?? "murmur-web",
      brokers,
      ssl: process.env.KAFKA_SSL === "1",
      sasl:
        process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD
          ? {
              mechanism: "plain",
              username: process.env.KAFKA_USERNAME,
              password: process.env.KAFKA_PASSWORD,
            }
          : undefined,
    });
    const producer = kafka.producer({
      allowAutoTopicCreation: process.env.KAFKA_ALLOW_AUTO_TOPIC_CREATION === "1",
      idempotent: true,
      maxInFlightRequests: 5,
    });
    producerPromise = producer.connect().then(
      () => producer,
      (error) => {
        producerPromise = null;
        producer.disconnect().catch(() => undefined);
        throw error;
      },
    );
  }
  return producerPromise;
}

export async function publishSwarmEvent(envelope: SwarmEventEnvelope) {
  const producer = await getProducer();
  if (!producer) return;

  await producer.send({
    topic,
    acks: -1,
    messages: [
      {
        key: envelope.runId,
        value: JSON.stringify(envelope),
        headers: {
          eventKind: envelope.event.kind,
          eventVersion: String(envelope.version),
        },
      },
    ],
  });
}

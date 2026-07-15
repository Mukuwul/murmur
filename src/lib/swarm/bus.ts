import type { SwarmEvent } from "./types";
import { publishSwarmEvent } from "./kafka";
import { finishRunSession, persistRunEvent, type RunStatus, type SwarmEventEnvelope } from "./session";

/**
 * Async event queue that lets many parallel agents push events while a single
 * consumer (the HTTP stream) drains them in order. This is what makes concurrent
 * agent token streams interleave live in the UI.
 */
export class EventBus implements AsyncIterable<SwarmEvent> {
  private queue: SwarmEvent[] = [];
  private waiters: ((r: IteratorResult<SwarmEvent>) => void)[] = [];
  private closing = false;
  private closed = false;
  private sequence = 0;
  private delivery = Promise.resolve();
  private deliveryFailure: unknown;
  private closePromise: Promise<void> | null = null;

  constructor(private readonly runId: string) {}

  emit(event: SwarmEvent) {
    if (this.closing || this.closed) return;
    const envelope: SwarmEventEnvelope = {
      version: 1,
      id: `${this.runId}:${++this.sequence}`,
      runId: this.runId,
      sequence: this.sequence,
      occurredAt: Date.now(),
      event,
    };
    // Parallel workers can emit simultaneously; serial delivery preserves their
    // local emission order in both Redis and Kafka.
    this.delivery = this.delivery.catch(() => undefined).then(() => this.deliver(envelope));
    // If the HTTP stream is already waiting for the next event, resolve it
    // immediately. Otherwise, buffer the event until a reader asks for it.
    const w = this.waiters.shift();
    if (w) w({ value: event, done: false });
    else this.queue.push(event);
  }

  close(status: Exclude<RunStatus, "running"> = "completed") {
    if (this.closePromise) return this.closePromise;
    this.closing = true;
    this.closePromise = this.finish(status);
    return this.closePromise;
  }

  private async finish(status: Exclude<RunStatus, "running">) {
    try {
      await this.delivery;
      if (this.deliveryFailure && process.env.MURMUR_STRICT_EVENT_DELIVERY === "1") {
        await finishRunSession(this.runId, "failed");
        throw this.deliveryFailure;
      }
      await finishRunSession(this.runId, status);
    } catch (error) {
      if (process.env.MURMUR_STRICT_EVENT_DELIVERY === "1") throw error;
      console.error("Failed to finish durable swarm event delivery", error);
    } finally {
      this.closed = true;
      // Keep SSE open until persistence and Kafka acknowledgement complete, then
      // wake any pending readers so the response can terminate safely.
      let waiter;
      while ((waiter = this.waiters.shift())) waiter({ value: undefined as never, done: true });
    }
  }

  private async deliver(envelope: SwarmEventEnvelope) {
    try {
      await Promise.all([persistRunEvent(envelope), publishSwarmEvent(envelope)]);
    } catch (error) {
      this.deliveryFailure ??= error;
      console.error("Failed to deliver swarm event", error);
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<SwarmEvent> {
    return {
      next: () => {
        // Drain already-buffered events first.
        if (this.queue.length) return Promise.resolve({ value: this.queue.shift()!, done: false });
        if (this.closed) return Promise.resolve({ value: undefined as never, done: true });
        // No event yet: suspend the reader until emit() resolves this promise.
        return new Promise((resolve) => this.waiters.push(resolve));
      },
    };
  }
}

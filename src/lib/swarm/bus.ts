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
  private waiters: {
    resolve: (result: IteratorResult<SwarmEvent>) => void;
    reject: (reason: Error) => void;
  }[] = [];
  private closing = false;
  private closed = false;
  private terminalError: Error | null = null;
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
    const waiter = this.waiters.shift();
    if (waiter) waiter.resolve({ value: event, done: false });
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
      if (this.deliveryFailure) {
        await finishRunSession(this.runId, "failed");
        throw this.deliveryFailure;
      }
      await finishRunSession(this.runId, status);
    } catch (error) {
      this.terminalError = error instanceof Error ? error : new Error(String(error));
      console.error("Required durable swarm event delivery failed", this.terminalError);
    } finally {
      this.closed = true;
      // Keep SSE open until persistence and Kafka acknowledgement complete, then
      // wake pending readers. A durability error rejects the iterator so the API
      // can send a visible terminal error to the browser.
      let waiter;
      while ((waiter = this.waiters.shift())) {
        if (this.terminalError) waiter.reject(this.terminalError);
        else waiter.resolve({ value: undefined as never, done: true });
      }
    }
    if (this.terminalError) throw this.terminalError;
  }

  private async deliver(envelope: SwarmEventEnvelope) {
    try {
      // Redis is the canonical recoverable record. Publish to Kafka only after
      // Redis accepts the event so a Kafka failure never loses the source event.
      await persistRunEvent(envelope);
      await publishSwarmEvent(envelope);
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
        if (this.closed) {
          if (this.terminalError) return Promise.reject(this.terminalError);
          return Promise.resolve({ value: undefined as never, done: true });
        }
        // No event yet: suspend the reader until emit() resolves this promise.
        return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
      },
    };
  }
}

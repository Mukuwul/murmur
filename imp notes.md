# Important Notes: Murmur API Call, SSE, and Live Streaming Flow

> Current production correction: Kafka and Redis are now required, not optional. Older snippets later in this learning file that show optional infrastructure describe the earlier implementation.

## Example used in this lesson

```text
User prompt: "Generate research on AI agents in healthcare"
User clicks: Swarm
```

## Full high-level flowchart

```text
GoalBar button click
  ↓
useRunSwarm()
  ↓
fetch("/api/swarm")
  ↓
Next.js route.ts
  ↓
runSwarm(goal, bus)
  ↓
EventBus emits events
  ↓
ReadableStream sends SSE chunks
  ↓
browser reads chunks
  ↓
Zustand store updates
  ↓
React UI rerenders live
```

## Full file-level flowchart

```text
src/components/GoalBar.tsx
  User enters goal and clicks Swarm.
  Calls the run function returned by useRunSwarm().
  ↓

src/lib/useRunSwarm.ts
  reset(goal)
  fetch("/api/swarm", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ goal }),
    signal: ctrl.signal,
  })
  Then reads the response stream with:
    res.body.getReader()
    TextDecoder()
    buffer.split("\n\n")
    JSON.parse(...)
    apply(event)
  ↓

src/app/api/swarm/route.ts
  export async function POST(req: Request)
  Reads request JSON:
    const { goal } = await req.json().catch(() => ({ goal: "" }));
  Validates goal.
  Checks OPENROUTER_API_KEY.
  Creates:
    const bus = new EventBus();
    const encoder = new TextEncoder();
  Starts:
    runSwarm(goal.trim(), bus)
  Returns:
    new Response(stream, { headers: { "content-type": "text/event-stream" } })
  ↓

src/lib/swarm/orchestrator.ts
  runSwarm(goal, bus)
  Emits:
    run.start
  Calls planner.
  Runs worker DAG waves.
  Calls validator.
  Runs synthesizer.
  Emits:
    agent.spawn
    agent.status
    agent.token
    validate.result
    run.done
  ↓

src/lib/swarm/bus.ts
  EventBus receives emitted events.
  Stores early events in queue.
  Resolves waiting stream readers when events arrive.
  Lets route.ts consume events with:
    for await (const event of bus)
  ↓

src/app/api/swarm/route.ts
  Converts each event:
    JS object
      → JSON.stringify(event)
      → `data: ${JSON.stringify(event)}\n\n`
      → encoder.encode(...)
      → controller.enqueue(...)
  Sends each event as an SSE chunk.
  ↓

Browser network layer
  Receives byte chunks over one open HTTP connection.
  ↓

src/lib/useRunSwarm.ts
  reader.read()
  decoder.decode(value, { stream: true })
  buffer += decoded text
  chunks = buffer.split("\n\n")
  line.slice(5).trim()
  JSON.parse(...)
  apply(event)
  ↓

src/lib/store.ts
  Zustand apply(event) updates state:
    agents
    edges
    statuses
    streamed output
    validator score/feedback
    final answer
  ↓

React components rerender:
  src/components/SwarmGraph.tsx
  src/components/AgentFlowNode.tsx
  src/components/SidePanel.tsx
```

## Mandatory Kafka + Redis production flow

```text
POST /api/swarm
  ↓
assertInfrastructureReady()
  ├─ Kafka admin checks that murmur.swarm.events exists
  └─ Redis returns PONG
  ↓
Redis distributed run-rate limit
  ↓
runSwarm() emits SwarmEvent
  ↓
EventBus creates versioned envelope
  {
    version,
    id,
    runId,
    sequence,
    occurredAt,
    event
  }
  ├─ Fast local branch:
  │    EventBus queue → SSE → browser immediately
  │
  └─ Ordered durable branch:
       Redis Lua script atomically stores:
         ├─ current run projection (hash)
         └─ append-only event history (Redis Stream)
       ↓
       Kafka publishes the same envelope
         key = runId
         acks = -1
       ↓
       run completion waits for all durable deliveries
```

Why Redis comes before Kafka:

```text
If Redis succeeds and Kafka fails:
  the canonical event still exists and the run fails visibly.

If Kafka ran first and Redis failed:
  downstream Kafka consumers could see an event that the application's
  recoverable run history does not contain.
```

Important tradeoff: Redis and Kafka cannot participate in one shared transaction. For strict no-gap publication at larger scale, a dedicated outbox worker should read unpublished durable Redis events, publish them to Kafka, and mark them delivered.

Local infrastructure commands:

```bash
pnpm infra:up
pnpm infra:ps
pnpm infra:topic
pnpm infra:logs
pnpm infra:down
```

Interview answer:

> "Kafka and Redis are required infrastructure in the hardened Murmur flow. Before accepting a run, the API verifies the Kafka topic and Redis connectivity. Every swarm event is wrapped in a versioned envelope with a run ID and sequence. EventBus sends a local branch to SSE immediately for low-latency UI, while an ordered durable branch runs in the background. A Redis Lua script atomically updates the run projection and append-only stream using a deterministic sequence ID, then the idempotent Kafka producer publishes that envelope keyed by run ID with all in-sync replica acknowledgement. The key preserves per-run partition order, while separate runs can scale across partitions. The run cannot complete until durable delivery catches up; if it fails, the client receives a terminal error."

## 1. How the API is called

In the browser, code calls:

```ts
fetch("/api/swarm", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ goal }),
  signal: ctrl.signal,
});
```

Cheatsheet:

```text
┌──────────────────────┬──────────────────────────────────────┐
│ Term                 │ Meaning                              │
├──────────────────────┼──────────────────────────────────────┤
│ fetch                │ Browser function for HTTP requests    │
├──────────────────────┼──────────────────────────────────────┤
│ "/api/swarm"          │ Backend endpoint inside Next.js       │
├──────────────────────┼──────────────────────────────────────┤
│ POST                 │ HTTP method for sending data          │
├──────────────────────┼──────────────────────────────────────┤
│ headers              │ Metadata about request body           │
├──────────────────────┼──────────────────────────────────────┤
│ content-type         │ Tells server body format is JSON      │
├──────────────────────┼──────────────────────────────────────┤
│ JSON.stringify       │ Converts JS object to JSON string     │
├──────────────────────┼──────────────────────────────────────┤
│ signal               │ Lets browser cancel the request       │
└──────────────────────┴──────────────────────────────────────┘
```

If `goal` is:

```text
"Generate research on AI agents in healthcare"
```

Then:

```ts
JSON.stringify({ goal })
```

becomes:

```json
{"goal":"Generate research on AI agents in healthcare"}
```

So the browser sends:

```http
POST /api/swarm
content-type: application/json

{"goal":"Generate research on AI agents in healthcare"}
```

## 2. Where `/api/swarm` goes

In Next.js App Router:

```text
src/app/api/swarm/route.ts
```

maps to:

```text
/api/swarm
```

This function handles the request:

```ts
export async function POST(req: Request) {
```

So:

```ts
fetch("/api/swarm", { method: "POST" })
```

calls:

```ts
POST(req)
```

inside `route.ts`.

## 3. What the route does first

It reads the body:

```ts
const { goal } = await req.json().catch(() => ({ goal: "" }));
```

Meaning:

```text
Take JSON sent by browser.
Extract goal.
If JSON is broken, use empty goal.
```

Then validates:

```ts
if (!goal || typeof goal !== "string" || goal.trim().length < 4) {
  return new Response(JSON.stringify({ error: "Provide a goal (min 4 chars)." }), {
    status: 400,
    headers: { "content-type": "application/json" },
  });
}
```

Meaning:

```text
If goal is missing or invalid, return error immediately.
Do not waste AI tokens.
```

Then checks API key:

```ts
if (!process.env.OPENROUTER_API_KEY) {
  return new Response(JSON.stringify({ error: "OPENROUTER_API_KEY is not set on the server." }), {
    status: 500,
    headers: { "content-type": "application/json" },
  });
}
```

Important:

```text
API key is checked on backend, not frontend.
```

Why:

```text
Frontend code is visible to users.
Backend environment variables are private.
```

## 4. What streaming is

Normal API:

```text
Browser asks question.
Server does all work.
Server returns final answer only.
```

Timeline:

```text
0s request sent
5s planner running
15s workers running
25s final ready
25s browser receives response
```

User sees nothing until 25s.

Streaming API:

```text
Browser asks question.
Server starts work.
Server sends updates while work is happening.
```

Timeline:

```text
0s request sent
1s browser receives run.start
2s browser receives plan.start
4s browser receives agent.spawn
5s browser receives agent.token
6s browser receives agent.token
12s browser receives validate.result
25s browser receives run.done
```

User sees progress live.

## 5. What SSE does

SSE means Server-Sent Events.

It is a simple format for server-to-browser live updates over one HTTP connection.

SSE is one-way:

```text
server → browser
```

In Murmur, that is enough because:

```text
browser sends goal once
server sends many updates back
```

SSE message format:

```text
data: {"kind":"agent.token","delta":"AI agents"}

```

Important:

```text
data:
```

means:

```text
this line contains event data
```

```text
\n\n
```

means:

```text
blank line; this event is finished
```

## 6. How Murmur creates SSE

In `route.ts`:

```ts
controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
```

Break it into 4 conversions:

### Step A: backend event object

```ts
const event = {
  kind: "agent.token",
  id: "agent-t1",
  delta: "AI agents"
};
```

### Step B: JSON.stringify

```ts
JSON.stringify(event)
```

becomes:

```json
{"kind":"agent.token","id":"agent-t1","delta":"AI agents"}
```

### Step C: wrap as SSE

```ts
`data: ${JSON.stringify(event)}\n\n`
```

becomes:

```text
data: {"kind":"agent.token","id":"agent-t1","delta":"AI agents"}

```

### Step D: encode to bytes

```ts
encoder.encode(...)
```

becomes bytes that can travel over HTTP.

Why encoding exists:

```text
Network streams send bytes.
JavaScript strings must be converted into bytes before sending.
```

## 7. What `ReadableStream` does

Code:

```ts
const stream = new ReadableStream({
  async start(controller) {
    try {
      for await (const event of bus) {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
      }
    } finally {
      controller.close();
    }
  },
});
```

Plain English:

```text
Create a response body that stays open.
Whenever EventBus gives an event:
  convert event to SSE bytes
  push it to browser immediately.
When EventBus closes:
  close the response.
```

`controller.enqueue(...)` means:

```text
send this chunk now
```

So each event becomes one live chunk.

## 8. Why EventBus is needed here

Because `runSwarm` and the API stream are separate.

`runSwarm` emits events:

```ts
bus.emit({ kind: "plan.start" });
bus.emit({ kind: "agent.spawn", ... });
bus.emit({ kind: "agent.token", ... });
```

The API route reads events:

```ts
for await (const event of bus) {
  controller.enqueue(...);
}
```

So the bus connects them:

```text
runSwarm emits → EventBus stores/delivers → route streams
```

## 9. What the browser receives

The browser receives text chunks like:

```text
data: {"kind":"run.start","goal":"Generate research...","at":123}

data: {"kind":"plan.start"}

data: {"kind":"agent.spawn","id":"agent-t1","agentType":"researcher"}

data: {"kind":"agent.token","id":"agent-t1","delta":"AI agents"}

data: {"kind":"agent.token","id":"agent-t1","delta":" in healthcare"}

data: {"kind":"validate.result","id":"agent-t1","score":8}

data: {"kind":"run.done","final":"...","tokensIn":1200,"tokensOut":2300,"ms":18000}

```

It does not receive one final JSON blob. It receives many small events.

## 10. How the browser reads the stream

In `useRunSwarm.ts`:

```ts
const reader = res.body.getReader();
const decoder = new TextDecoder();
let buffer = "";
```

Meaning:

```text
reader = reads byte chunks from response
decoder = converts bytes back to text
buffer = holds incomplete event text
```

Loop:

```ts
while (true) {
  const { done, value } = await reader.read();
  if (done) break;

  buffer += decoder.decode(value, { stream: true });
  const chunks = buffer.split("\n\n");
  buffer = chunks.pop() ?? "";

  for (const chunk of chunks) {
    const line = chunk.trim();
    if (!line.startsWith("data:")) continue;
    apply(JSON.parse(line.slice(5).trim()) as SwarmEvent);
  }
}
```

Plain English:

```text
1. Read bytes from response.
2. Convert bytes to text.
3. Add text to buffer.
4. Split complete SSE events by blank line.
5. Keep unfinished leftover.
6. For each complete event:
   - remove "data:"
   - parse JSON
   - call apply(event)
```

## 11. Why buffer is needed

Network chunks can split randomly.

Server sends:

```text
data: {"kind":"agent.token","delta":"AI agents"}\n\n
```

Browser might receive it as two pieces:

Chunk 1:

```text
data: {"kind":"agent.tok
```

Chunk 2:

```text
en","delta":"AI agents"}\n\n
```

If we parse chunk 1 immediately, it crashes.

So we use `buffer`:

```text
store incomplete text
wait for \n\n
then parse complete event
```

This is a key interview detail.

## 12. After parsing, what happens

Client gets JS object:

```ts
{
  kind: "agent.token",
  id: "agent-t1",
  delta: "AI agents"
}
```

Then:

```ts
apply(event)
```

goes to Zustand store.

In `store.ts`:

```ts
case "agent.token": {
  const a = s.agents[e.id];
  if (!a) return {};
  return { agents: { ...s.agents, [e.id]: { ...a, output: a.output + e.delta } } };
}
```

Meaning:

```text
Find that agent.
Append token text to its output.
Update state.
React rerenders UI.
```

That is why text appears live.

## Full live streaming pipeline

```text
Worker creates token
  ↓
bus.emit({ kind: "agent.token", delta })
  ↓
EventBus gives event to route
  ↓
route JSON.stringify(event)
  ↓
route wraps as SSE: data: ...\n\n
  ↓
TextEncoder converts text to bytes
  ↓
controller.enqueue sends chunk
  ↓
browser reader.read receives bytes
  ↓
TextDecoder converts bytes to text
  ↓
client splits by \n\n
  ↓
JSON.parse gives event object
  ↓
Zustand apply(event)
  ↓
React UI rerenders
```

## Interview answer: how API is called

> The frontend calls `/api/swarm` using `fetch` with method `POST` and a JSON body containing the user goal. In Next.js App Router, that maps to `src/app/api/swarm/route.ts`, specifically the exported `POST(req)` function. The route validates the input, creates an EventBus, starts the swarm, and returns an SSE stream.

## Interview answer: how streaming works

> Streaming works by returning a `ReadableStream` instead of a normal JSON response. The route keeps the response open. Every time the EventBus yields a swarm event, the route encodes that event as an SSE frame and pushes it into the stream with `controller.enqueue`. The browser reads the response body chunk by chunk and updates the UI as each event arrives.

## Interview answer: what SSE does

> SSE is the wire format and protocol style for server-to-client events. Each event is sent as text beginning with `data:` and ending with a blank line. In Murmur, SSE lets the backend send live planner, worker, validator, and final-output events over one HTTP connection without needing WebSockets.

## Common misconception

Wrong:

```text
SSE is the AI model streaming.
```

Correct:

```text
The AI model streams tokens to backend.
Backend converts those tokens into Murmur events.
Murmur sends those events to browser using SSE.
```

So there are two streaming layers:

```text
Model provider → backend
backend → browser
```

Murmur controls the second one.

## Next session

Next session should be `useRunSwarm.ts` deep dive.

It should cover:

```text
1. AbortController
2. fetch POST request
3. response.body.getReader()
4. TextDecoder
5. buffer handling
6. splitting SSE frames
7. JSON.parse
8. apply(event)
9. common stream bugs
10. interview questions
```

## Checkpoint

Answer in your own words:

```text
Why do we need both TextEncoder on the server and TextDecoder on the browser?
```

---

# Production Skills: Kafka, Redis Sessions, and Rate Limiting

## Why this upgrade exists

The original EventBus was only in memory:

```text
runSwarm emits event
  ↓
EventBus queue in server RAM
  ↓
SSE stream to current browser
```

That is good for a demo, but weak for production.

Production problems:

```text
1. Server crash loses events.
2. Browser reconnect cannot replay old events.
3. Multiple app servers do not share memory.
4. No distributed rate limit across servers.
5. No audit trail for swarm runs.
```

Kafka and Redis improve this:

```text
Kafka = distributed event publishing for observability/audit/downstream consumers
Redis = shared state for rate limits, run sessions, and event replay
```

## Production flowchart

```text
Browser POST /api/swarm
  ↓
route.ts validates goal + API key
  ↓
route.ts computes clientId from headers
  ↓
Redis rate limiter checks:
  murmur:rate:runs:<clientId>
  ↓
If over limit:
  return 429 + retry-after
  ↓
If allowed:
  create runId = crypto.randomUUID()
  ↓
create EventBus(runId)
  ↓
runSwarm(goal, bus)
  ↓
bus.emit(event)
  ↓
EventBus wraps event in envelope:
  { version, id, runId, sequence, occurredAt, event }
  ↓
EventBus sends event to 3 places:
  1. local SSE queue for current browser
  2. Redis stream/session storage
  3. Kafka topic
  ↓
route.ts streams raw SwarmEvent to browser as SSE
  ↓
browser updates UI live
```

## Skill vocabulary

```text
┌──────────────────────────┬──────────────────────────────────────┐
│ Term                     │ Meaning                              │
├──────────────────────────┼──────────────────────────────────────┤
│ Kafka                    │ Distributed event log / message bus   │
├──────────────────────────┼──────────────────────────────────────┤
│ topic                    │ Named stream of Kafka messages        │
├──────────────────────────┼──────────────────────────────────────┤
│ producer                 │ Code that writes messages to Kafka    │
├──────────────────────────┼──────────────────────────────────────┤
│ message key              │ Value Kafka uses for partition/order  │
├──────────────────────────┼──────────────────────────────────────┤
│ acks: -1                 │ Wait for all in-sync replicas         │
├──────────────────────────┼──────────────────────────────────────┤
│ idempotent producer      │ Reduces duplicate/ordering risks      │
├──────────────────────────┼──────────────────────────────────────┤
│ Redis                    │ Fast shared in-memory data store      │
├──────────────────────────┼──────────────────────────────────────┤
│ INCR                     │ Atomically increment a Redis counter  │
├──────────────────────────┼──────────────────────────────────────┤
│ EXPIRE                   │ Set automatic key deletion time       │
├──────────────────────────┼──────────────────────────────────────┤
│ TTL                      │ Time left before key expires          │
├──────────────────────────┼──────────────────────────────────────┤
│ fixed window limit       │ Count requests inside time window     │
├──────────────────────────┼──────────────────────────────────────┤
│ Redis Stream             │ Append-only event list in Redis       │
├──────────────────────────┼──────────────────────────────────────┤
│ runId                    │ Unique id for one swarm run           │
├──────────────────────────┼──────────────────────────────────────┤
│ sequence                 │ Increasing event number per run       │
└──────────────────────────┴──────────────────────────────────────┘
```

## Kafka in this project

File:

```text
src/lib/swarm/kafka.ts
```

Important code:

```ts
const brokers = process.env.KAFKA_BROKERS?.split(",")
  .map((b) => b.trim())
  .filter(Boolean);
```

Meaning:

```text
Read comma-separated Kafka broker addresses from env.
If no brokers are configured, Kafka is disabled.
```

Producer setup:

```ts
const producer = kafka.producer({
  allowAutoTopicCreation: process.env.KAFKA_ALLOW_AUTO_TOPIC_CREATION === "1",
  idempotent: true,
  maxInFlightRequests: 5,
});
```

Meaning:

```text
Create a Kafka producer.
idempotent: true improves reliable delivery semantics.
maxInFlightRequests limits simultaneous sends.
```

Publishing:

```ts
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
```

Meaning:

```text
Send every swarm event to Kafka.
Use runId as the message key.
Store the full event envelope as JSON.
Add headers for event type and version.
acks: -1 waits for all in-sync replicas.
```

Why `runId` as key matters:

```text
Kafka keeps messages with the same key in the same partition.
That helps preserve event order within one swarm run.
```

Interview answer:

> Kafka is used to mirror swarm events into a distributed event log. The local SSE stream updates the current browser, while Kafka enables audit trails, observability, replay pipelines, analytics, and downstream consumers. Events are keyed by `runId`, so all events for one run stay ordered within a Kafka partition.

## Redis rate limiting in this project

File:

```text
src/lib/swarm/rateLimit.ts
```

Limits:

```ts
export const RUN_RATE_LIMIT = {
  limit: intEnv("MURMUR_RUNS_PER_WINDOW", 20),
  windowSeconds: intEnv("MURMUR_RUN_WINDOW_SECONDS", 3600),
};

export const MODEL_RATE_LIMIT = {
  limit: intEnv("MURMUR_MODEL_CALLS_PER_WINDOW", 120),
  windowSeconds: intEnv("MURMUR_MODEL_WINDOW_SECONDS", 3600),
};
```

Meaning:

```text
RUN_RATE_LIMIT limits new swarm runs.
MODEL_RATE_LIMIT limits model calls.
Defaults:
  20 runs per hour
  120 model calls per hour
```

Atomic Redis script:

```lua
local count = redis.call("INCR", KEYS[1])
if count == 1 then
  redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return { count, redis.call("TTL", KEYS[1]) }
```

Meaning:

```text
Increment request counter.
If this is the first request in the window, set expiry.
Return current count and remaining TTL.
```

Why Lua script:

```text
INCR and EXPIRE need to behave atomically.
If they were separate commands and the server crashed between them,
the counter might never expire.
```

Route usage:

```ts
await enforceRateLimit({
  key: rateLimitKey("runs", clientId),
  ...RUN_RATE_LIMIT,
});
```

If over limit:

```ts
return new Response(JSON.stringify({ error: e.message, retryAfterSeconds: e.retryAfterSeconds }), {
  status: 429,
  headers: {
    "content-type": "application/json",
    "retry-after": String(e.retryAfterSeconds),
  },
});
```

Meaning:

```text
Reject request with HTTP 429.
Tell client how many seconds to wait before retrying.
```

Interview answer:

> Redis is used for shared distributed rate limits. Because every app instance talks to the same Redis, limits work even when the app is horizontally scaled. The implementation uses an atomic `INCR` + `EXPIRE` Lua script to implement a fixed-window counter and returns HTTP 429 with `retry-after` when the limit is exceeded.

## Redis run sessions and replay

File:

```text
src/lib/swarm/session.ts
```

Event envelope:

```ts
export interface SwarmEventEnvelope {
  version: 1;
  id: string;
  runId: string;
  sequence: number;
  occurredAt: number;
  event: SwarmEvent;
}
```

Meaning:

```text
Raw event gets wrapped with production metadata:
version      protocol version
id           unique event id
runId        which swarm run this belongs to
sequence     event number inside this run
occurredAt   timestamp
event        original SwarmEvent
```

Persist event:

```ts
await redis
  .multi()
  .hset(session, ...fields)
  .expire(session, SESSION_TTL_SECONDS)
  .xadd(eventStream, "MAXLEN", "~", EVENT_STREAM_MAX_LENGTH, "*", "envelope", JSON.stringify(envelope))
  .expire(eventStream, SESSION_TTL_SECONDS)
  .exec();
```

Meaning:

```text
Update current run projection in Redis hash.
Append full event envelope to Redis Stream.
Expire both session and event stream after TTL.
```

Replay endpoint:

```text
src/app/api/swarm/[runId]/route.ts
```

GET flow:

```ts
const session = await getRunSession(runId);
return Response.json({ session, events: await getRunEvents(runId) });
```

Meaning:

```text
Client can retrieve stored session and past events for a runId.
This is the base for reconnect/replay.
```

Interview answer:

> Redis stores two things: a current run projection and an append-only event stream. The projection gives quick status/final/error lookup, while the event stream preserves the event history for replay. The route `GET /api/swarm/:runId` can return both, which is the foundation for reconnect and debugging.

## Updated EventBus responsibility

Old responsibility:

```text
local queue for SSE only
```

New responsibility:

```text
local queue for SSE
+ Redis persistence
+ Kafka publishing
+ sequence numbers
+ run lifecycle finish
```

Code idea:

```ts
const envelope: SwarmEventEnvelope = {
  version: 1,
  id: `${this.runId}:${++this.sequence}`,
  runId: this.runId,
  sequence: this.sequence,
  occurredAt: Date.now(),
  event,
};
```

Then:

```ts
this.delivery = this.delivery
  .catch(() => undefined)
  .then(() => this.deliver(envelope));
```

Meaning:

```text
Serialize durable delivery so Redis/Kafka see events in local emission order.
```

Delivery:

```ts
await Promise.all([persistRunEvent(envelope), publishSwarmEvent(envelope)]);
```

Meaning:

```text
Write the event to Redis and Kafka.
Do both as part of event delivery.
```

Tradeoff:

```text
Good: more production-ready, replayable, observable.
Cost: more infrastructure, more latency risk, more failure modes.
```

## Strict vs non-strict delivery

Environment:

```text
MURMUR_STRICT_EVENT_DELIVERY=1
```

If strict:

```text
Kafka/Redis delivery failure can fail the run contract.
```

If not strict:

```text
App logs delivery failure but keeps the live run working.
```

Interview answer:

> I made event delivery configurable. In local/demo mode, Kafka or Redis can be absent and the app still works. In production strict mode, failed durable delivery can fail the run because auditability and replay may be part of the product contract.

## Fixed-window rate limit tradeoff

Current approach:

```text
20 runs per 3600 seconds
```

Pros:

```text
Simple.
Fast.
Atomic with Redis.
Works across many app servers.
```

Cons:

```text
Boundary burst problem.
A user can send 20 requests at 10:59 and 20 more at 11:00.
```

More advanced options:

```text
sliding window log
sliding window counter
token bucket
leaky bucket
```

Interview answer:

> Fixed window is a pragmatic first production rate limiter. It is simple, cheap, and distributed through Redis, but it allows bursts at window boundaries. For stricter abuse control, I would move to token bucket or sliding window.

## Production upgrade interview answer

> The app now keeps the original SSE path for live browser updates, but adds Kafka and Redis for production concerns. Kafka receives versioned swarm event envelopes keyed by `runId`, which supports audit trails, ordered per-run event consumption, and downstream analytics. Redis handles distributed rate limiting and stores run sessions plus append-only event streams for replay. This separates live UX from durable infrastructure: SSE is for the active browser, Redis is for state/replay, Kafka is for distributed event consumers.

## Next production skill sessions

```text
1. Kafka topic/message key/partition ordering
2. Redis fixed-window rate limiter line-by-line
3. Redis Streams vs Kafka: when to use which
4. Reconnect/replay design with runId and sequence
5. Strict delivery tradeoffs and failure handling
```

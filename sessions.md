# Murmur Learning Sessions

## Session 1
- **Route:** Basic to Interview
- **Prerequisite Check:** Corrected to Murmur - Agent Swarm Orchestrator. Grasp speed: FAST.
- **Concepts Taught:** Mental model of Planner/Workers/Validator/Synthesizer, DAG, SSE. EventBus (AsyncIterator). Planner (Zod), Orchestrator (DAG execution).
- **Learner Gaps:** (None yet)

## Session 2
- **Route:** Basic to Interview
- **Prerequisite Check:** Handled in Session 1.
- **Concepts Taught:** Worker/Validator loop, Shared Blackboard (upstream results), Zod schema (quality gate), Failure fallback (rate limits).
- **Learner Gaps:** (Pending)

## Session 3
- **Route:** Basic to Interview
- **Prerequisite Check:** Continuing from Session 2. Current target: live streaming/UI data path.
- **Concepts Taught:** In progress — SSE route, EventBus AsyncIterator, client stream reader, Zustand reducer, React Flow rendering.
- **Learner Gaps:** Pending checkpoint response.
- **Next Session Focus:** TBD after Session 3 checkpoint.

## Session 4
- **Route:** Basic to Interview
- **Prerequisite Check:** Continuing prior route; user asked specifically for Zod in this project.
- **Concepts Taught:** Zod as runtime schema for AI structured outputs; planner schema; validator schema; `genObject` type contract; difference between TypeScript interfaces and runtime validation.
- **Learner Gaps:** Pending checkpoint response.
- **Next Session Focus:** Trace one real planner output through `plan()` into `runSwarm()`.

## Session 5
- **Route:** Basic to Interview
- **Prerequisite Check:** User requested production upgrade: Kafka for distributed event streaming and Redis for rate limit management.
- **Concepts Taught:** In progress — in-memory event bus vs distributed event stream; Kafka event publishing; Redis shared counters for rate limits; optional infrastructure via env vars.
- **Learner Gaps:** Pending review after PR.
- **Next Session Focus:** Explain Kafka topic/message key design and Redis fixed-window tradeoffs.

## Session 6
- **Route:** Basic to Interview
- **Prerequisite Check:** User asked to store the current API/SSE/live-streaming explanation and flowchart.
- **Concepts Taught:** API call from `useRunSwarm.ts` to `/api/swarm`; Next.js route mapping; request JSON validation; backend-only API key check; normal API vs streaming API; SSE `data: ...\n\n` format; `ReadableStream`; `TextEncoder`; browser `TextDecoder`; buffer handling; Zustand `apply(event)` update path.
- **Examples Used:** User prompt `"Generate research on AI agents in healthcare"` and click `Swarm`.
- **Small Terms Explained:** `fetch`, `POST`, `headers`, `content-type`, `JSON.stringify`, `signal`, `ReadableStream`, `controller.enqueue`, `SSE`, `TextEncoder`, `TextDecoder`, `buffer`.
- **Learner Gaps:** User is clarifying streaming and SSE mechanics; next focus should be client-side stream reading.
- **Next Session Focus:** `useRunSwarm.ts` deep dive: `AbortController`, `response.body.getReader()`, decoding chunks, buffering partial SSE frames, `JSON.parse`, `apply(event)`, and common stream bugs.

## Session 7
- **Route:** Basic to Interview
- **Prerequisite Check:** User asked to store Kafka/rate-limiting updates in the learning sessions and add the related skills.
- **Concepts Taught:** Production upgrade path from in-memory EventBus to durable/shared infrastructure; Kafka producer; Kafka topic/key/message headers; Redis shared connection; Redis fixed-window rate limiter; HTTP 429 + `retry-after`; Redis run session projection; Redis Stream event replay; `runId`; event envelope versioning and sequence numbers; strict vs non-strict event delivery.
- **Examples Used:** Current Murmur production files: `src/lib/swarm/kafka.ts`, `src/lib/swarm/rateLimit.ts`, `src/lib/swarm/session.ts`, `src/lib/swarm/bus.ts`, `src/app/api/swarm/route.ts`, `src/app/api/swarm/[runId]/route.ts`.
- **Small Terms Explained:** Kafka, topic, producer, message key, `acks: -1`, idempotent producer, Redis, `INCR`, `EXPIRE`, `TTL`, fixed-window limit, Redis Stream, `runId`, sequence, replay, HTTP 429.
- **Learner Gaps:** Needs line-by-line reinforcement of Kafka topic/key partitioning and Redis rate-limit script.
- **Next Session Focus:** Kafka topic/message-key/partition ordering, then Redis fixed-window rate limiter line-by-line.

## Session 8
- **Route:** Basic to Interview
- **Prerequisite Check:** Learner already understood SSE as the browser-facing stream and asked to make Kafka/Redis deeply integrated rather than optional.
- **Concepts Taught:** Required infrastructure contract; fail-closed readiness gate; Kafka topic provisioning; `runId` partition key; all-replica acknowledgement; Redis `PING`; AOF persistence; atomic Lua persistence; deterministic Redis Stream IDs; idempotency; health/readiness endpoint; non-root Docker services; local single-node vs managed replicated production infrastructure; Redis-first durable delivery; transactional-outbox limitation.
- **Examples Used:** `POST /api/swarm → readiness → Redis rate limit → EventBus branches into immediate SSE plus ordered Redis→Kafka durability`; local `murmur.swarm.events` topic with six partitions and seven-day retention; production container connected to `kafka:19092` and `redis:6379`.
- **Small Terms Explained:** readiness, fail closed, topic provisioning, partition, message key, ISR, AOF, append-only log, deterministic ID, idempotency, Lua atomicity, health check, named volume, non-root container, transactional outbox.
- **Implementation Work:** Added required validated config, Kafka/Redis readiness checks, `/api/health`, atomic Redis event persistence, visible durable-delivery errors, Docker Compose Kafka/Redis stack, topic initializer, production Dockerfile, KafkaJS Node 24 patch, and deployment documentation.
- **Doubts and Answers:** Kafka/Redis are no longer optional. Redis is the recoverable source record; Kafka is the required distributed event stream; SSE remains the live browser transport.
- **Learner Gaps:** Still needs line-by-line reinforcement of Kafka partition ordering and the Redis Lua persistence/rate-limit scripts.
- **Grasp-Speed Signal:** Fast, architecture-oriented requests; learner prefers implementation first with interview explanation attached.
- **Adaptation Used:** Implemented the full path, then mapped every operational failure to a production concept instead of teaching Kafka only in isolation.
- **Recurring Difficult Topics:** EventBus vs Kafka vs SSE responsibilities; Redis atomic commands and TTL behavior.
- **Next Session Focus:** Trace one `run.start` envelope through Redis and Kafka, then explain the two Lua scripts line by line and answer medium/hard production interview questions.

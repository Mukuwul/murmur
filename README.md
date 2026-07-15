# ✺ Murmur — Agent Swarm Orchestrator


> Watch a swarm of AI agents **self-organize** to solve what a single agent can't —
> plan, delegate, validate, and synthesize, all live on one screen.

**Built for the Microsoft Build AI 2026 Hackathon · Theme: _Agent Swarms_.**

A murmuration is the swarm behavior of starlings — thousands of birds acting as one
intelligent system with no central controller. Murmur does that with AI agents: you
hand it a complex goal, and a planner decomposes it into a dependency graph of
specialist agents that run **in parallel**, **check each other's work**, and fuse their
outputs into a single deliverable — while you watch the whole thing think.

---

## Why this is hard (and why a swarm wins)

One LLM call answering "build me a go-to-market strategy" gives you a shallow, generic
wall of text. Real work is **decomposable, parallel, and self-correcting**. Murmur models
that:

| Problem with a single agent | Murmur's swarm answer |
| --- | --- |
| Does everything in one pass, shallow | **Planner** splits the goal into focused subtasks |
| Serial and slow | Independent tasks run **concurrently** (DAG waves) |
| No quality control | **Validator** scores every output, rejects weak work, triggers a revision |
| One perspective | **Specialist agents** (researcher / analyst / writer / coder) each do what they're best at |
| Opaque "trust me" output | Every token of every agent is **streamed live** onto a graph |

## How it works

```
            ┌──────────┐
   goal ──▶ │ Planner  │  decomposes into a task DAG (structured output → always valid)
            └────┬─────┘
                 │ assigns
      ┌──────────┼───────────┐         ┌───────────┐
      ▼          ▼           ▼   review │ Validator │  scores 0–10, can reject → revise
 ┌─────────┐ ┌────────┐ ┌────────┐◀────▶└───────────┘
 │Researcher│ │Analyst │ │ Writer │   (parallel wave; dependent tasks wait their turn)
 └────┬─────┘ └───┬────┘ └───┬────┘
      └───────────┼──────────┘ results
                  ▼
            ┌──────────────┐
            │ Synthesizer  │ ──▶ final deliverable
            └──────────────┘
```

- **Self-organization** — the planner decides *how many* agents and *which types* per goal; nothing is hard-coded.
- **Parallel DAG execution** — the orchestrator runs each wave of dependency-free tasks concurrently.
- **Self-correction** — the validator gate is also the reliability mechanism: weak outputs get one feedback-driven revision before they're accepted.
- **Shared blackboard** — downstream agents receive upstream outputs as context.
- **Live observability** — a streaming SSE event bus drives a React Flow graph; click any node to read its output as it's written.
- **Production controls** — required Kafka publishing mirrors every swarm event for distributed consumers; required Redis provides rate limits, durable run state, and replay across app instances.

## Architecture

```
src/lib/swarm/
  types.ts         shared domain + streaming event types
  config.ts        validated required Kafka/Redis environment contract
  infrastructure.ts cached readiness probe for both dependencies
  bus.ts           local SSE queue + ordered durable event delivery
  kafka.ts         idempotent Kafka producer for distributed event streaming / audit trails
  redis.ts          shared Redis connection for production state
  session.ts        Redis run projection + append-only event stream for replay
  rateLimit.ts     atomic Redis-backed shared rate limiter
  models.ts        env-configurable Claude model roles
  planner.ts       streamObject → validated task DAG
  worker.ts        per-specialist system prompts; streams tokens
  validator.ts     generateObject → score / approve / feedback
  orchestrator.ts  DAG wave scheduler + validator-retry loop + synthesis
src/app/api/health/route.ts  Kafka-topic + Redis readiness endpoint
src/app/api/swarm/route.ts   readiness gate → POST goal → Server-Sent Events stream
src/app/api/swarm/[runId]    GET persisted session + events for replay
src/lib/store.ts             Zustand store; reduces events → graph state
src/components/               React Flow graph, animated nodes, live side panel
```

**Stack:** Next.js 16 (App Router) · TypeScript · Vercel AI SDK · OpenRouter (Claude + open models) · KafkaJS · ioredis · React Flow · Zustand.

**Models (mixed by role):** structured-output roles (planner, validator) run on a capable paid Claude model; plain-text roles (worker, synthesizer) run on free models. All slugs are env-overridable.

## Run it locally

```bash
pnpm install
cp .env.example .env.local   # only needed if .env.local does not exist
# add OPENROUTER_API_KEY to .env.local
pnpm infra:up                # starts Kafka, creates the topic, starts Redis
pnpm infra:topic             # verifies six topic partitions
pnpm dev                     # http://localhost:3000
curl -i http://localhost:3000/api/health
```

Then give the swarm a goal, e.g. _"Create a go-to-market strategy for an AI code-review startup"_,
and watch it work.

### Required Kafka and Redis flow

Murmur deliberately fails closed when Kafka or Redis is unavailable. The app checks both dependencies before spending model tokens:

```text
POST /api/swarm
  → Kafka topic metadata + Redis PING readiness gate
  → Redis distributed run-rate limit
  → runSwarm emits a versioned event envelope
  → EventBus branches:
      ├─ local queue → SSE immediately for low-latency UI
      └─ ordered durable chain → Redis atomic write → Kafka acks=all
  → run completion waits for the durable chain; failure terminates the stream
```

The local stack uses pinned `apache/kafka:4.3.1` and `redis:8.8.0-alpine` images. It explicitly creates `murmur.swarm.events` with six partitions and seven-day retention; Kafka automatic topic creation is disabled. Redis uses AOF with `appendfsync everysec` plus a named volume.

Useful commands:

| Command | Purpose |
| --- | --- |
| `pnpm infra:up` | Start and health-check Kafka + Redis |
| `pnpm infra:ps` | Show container health |
| `pnpm infra:topic` | Describe topic partitions and leader metadata |
| `pnpm infra:logs` | Follow Kafka/Redis logs |
| `pnpm infra:down` | Stop services but preserve data volumes |
| `pnpm infra:reset` | Stop services and delete local data volumes |

### Production environment

Use managed, replicated Kafka and Redis endpoints rather than the single-node local Compose stack:

```bash
KAFKA_BROKERS=broker-1:9092,broker-2:9092
KAFKA_SWARM_EVENTS_TOPIC=murmur.swarm.events
KAFKA_SSL=1
KAFKA_SASL_MECHANISM=scram-sha-512
KAFKA_USERNAME=...
KAFKA_PASSWORD=...
REDIS_URL=rediss://default:password@host:6379
```

- Kafka receives every `SwarmEvent` as a versioned envelope, keyed by run id and acknowledged by all in-sync replicas. Kafka preserves order within one run's partition while different runs can scale across partitions.
- Redis enforces atomic distributed limits for new runs/model attempts. A Lua script atomically stores an idempotent event sequence in both the session projection and Redis Stream.
- A completed or reconnecting client can retrieve Redis-backed state from `GET /api/swarm/:runId`; the POST response includes that id in `x-murmur-run-id`.
- `GET /api/health` returns HTTP 200 only when Redis responds and the required Kafka topic exists; otherwise it returns HTTP 503.
- Configure the Kafka topic with replication factor 3 and `min.insync.replicas=2`, use TLS/SASL, enable Redis high availability/backups, and alert on readiness failures, consumer lag, Redis memory, and event-delivery latency.

Kafka and Redis are two different systems, so their writes cannot share one transaction. Murmur writes Redis first as the recoverable source record and fails the run visibly if Kafka publishing fails. SSE stays responsive because the local event branch does not wait on each remote acknowledgement, but the run cannot close successfully until durable delivery catches up. For a strict no-gap event pipeline at larger scale, move Kafka publication to a dedicated transactional-outbox worker that retries unpublished Redis events.

## Build and deploy

The repository includes a multi-stage production `Dockerfile`; `next.config.ts` emits a minimal standalone server bundle. Build it with:

```bash
docker build -t murmur:local .
```

Inject secrets and managed infrastructure endpoints at runtime—never bake them into the image. The `/api/swarm` route streams for up to five minutes (`maxDuration = 300`). If deploying on a serverless platform, verify that its request duration and Kafka connection model fit long-lived SSE runs; a container service plus background workers is the safer high-throughput architecture.

---

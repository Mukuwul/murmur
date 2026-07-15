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
- **Production controls** — optional Kafka publishing mirrors every swarm event for distributed consumers, and optional Redis limits runs/model calls across app instances.

## Architecture

```
src/lib/swarm/
  types.ts         shared domain + streaming event types
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
src/app/api/swarm/route.ts   POST goal → Server-Sent Events stream
src/app/api/swarm/[runId]    GET persisted session + events for replay
src/lib/store.ts             Zustand store; reduces events → graph state
src/components/               React Flow graph, animated nodes, live side panel
```

**Stack:** Next.js 16 (App Router) · TypeScript · Vercel AI SDK · OpenRouter (Claude + open models) · KafkaJS · ioredis · React Flow · Zustand.

**Models (mixed by role):** structured-output roles (planner, validator) run on a capable paid Claude model; plain-text roles (worker, synthesizer) run on free models. All slugs are env-overridable.

## Run it

```bash
pnpm install
cp .env.example .env.local   # add your OPENROUTER_API_KEY
pnpm dev                     # http://localhost:3000
```

Then give the swarm a goal, e.g. _"Create a go-to-market strategy for an AI code-review startup"_,
and watch it work.

### Optional production infrastructure

Murmur runs locally without Kafka or Redis. In production, set these env vars to enable shared infrastructure:

```bash
KAFKA_BROKERS=broker-1:9092,broker-2:9092
KAFKA_SWARM_EVENTS_TOPIC=murmur.swarm.events
REDIS_URL=redis://default:password@host:6379
```

- Kafka receives every `SwarmEvent` as a versioned envelope, keyed by run id and acknowledged by all replicas. This preserves order within a run for observability, replay, audit, and downstream consumers.
- Redis enforces distributed limits for new swarm runs and model attempts, and stores each run's expiring session projection plus append-only event stream. A completed or reconnecting client can retrieve it from `GET /api/swarm/:runId`; the POST response includes the id in `x-murmur-run-id`.
- `MURMUR_STRICT_EVENT_DELIVERY=1` makes configured Kafka/Redis delivery part of the run contract. Leave it off during local development when either dependency is intentionally absent.

## Deploy

One-click on Vercel — set `OPENROUTER_API_KEY` in project env vars. Add `REDIS_URL` and
Kafka settings for production shared state. The `/api/swarm` route streams for up to
5 minutes (`maxDuration = 300`).

---

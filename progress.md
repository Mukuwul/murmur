# Murmur Learning Progress

## Session 3 startup
- Read `sessions.md`, `progress.md`, `README.md`, and `package.json`.
- Scanned project file list with `rg --files`.
- Read streaming/UI path files:
  - `src/lib/swarm/types.ts`
  - `src/lib/swarm/bus.ts`
  - `src/app/api/swarm/route.ts`
  - `src/lib/store.ts`
  - `src/lib/useRunSwarm.ts`
  - `src/components/SwarmGraph.tsx`
  - `src/components/SidePanel.tsx`
  - `src/app/page.tsx`
- Selected Session 3 topic: SSE event pipeline from backend swarm events to live graph UI.

## Interview revision request
- User asked to explain project mapping files, important code blocks, senior-interview concepts, medium/hard questions with answers, Next.js/TypeScript fundamentals, and production/token tradeoffs.
- Read:
  - `src/lib/swarm/planner.ts`
  - `src/lib/swarm/orchestrator.ts`
  - `src/lib/swarm/worker.ts`
  - `src/lib/swarm/validator.ts`

## Teaching continuation
- Continued from build-from-zero roadmap.
- Focus: first commands, first folders/files, Next.js/TypeScript basics, and explaining why the swarm backend should be built before UI.

## Stored API/SSE/live-streaming notes
- Created `imp notes.md`.
- Stored the user-requested live streaming explanation and full flowchart:
  - `GoalBar` click
  - `useRunSwarm()`
  - `fetch("/api/swarm")`
  - `src/app/api/swarm/route.ts`
  - `runSwarm(goal, bus)`
  - `EventBus`
  - `ReadableStream`
  - SSE `data: ...\n\n`
  - `TextEncoder` / `TextDecoder`
  - browser buffer handling
  - `Zustand apply(event)`
  - React rerender path

## Stored Kafka and Redis production skills
- Updated `imp notes.md` with a dedicated production skills section for Kafka, Redis sessions, and rate limiting.
- Read and referenced:
  - `src/lib/swarm/kafka.ts`
  - `src/lib/swarm/rateLimit.ts`
  - `src/lib/swarm/session.ts`
  - `src/lib/swarm/bus.ts`
  - `src/lib/swarm/redis.ts`
  - `src/app/api/swarm/route.ts`
  - `src/app/api/swarm/[runId]/route.ts`
- Added Session 7 to `sessions.md`.
- Skills captured:
  - Kafka topic/key/event publishing
  - Redis shared rate limiting
  - Redis run session projection
  - Redis Stream event replay
  - event envelope versioning
  - strict vs non-strict delivery tradeoffs

## Session 4 Zod explanation
- User asked to explain Zod using the tutor skill.
- Read `tech-learning-tutor` skill instructions.
- Scanned repo for Zod usage with `rg`.
- Read:
  - `src/lib/swarm/planner.ts`
  - `src/lib/swarm/validator.ts`
  - `src/lib/swarm/run.ts`
  - `src/lib/swarm/types.ts`
  - `src/lib/swarm/orchestrator.ts`
- Finding: Zod is used as the runtime contract for structured LLM outputs in planner and validator flows.

## Production upgrade: Kafka and Redis
- User requested production-level upgrades and a PR.
- Read:
  - `package.json`
  - `src/lib/swarm/bus.ts`
  - `src/app/api/swarm/route.ts`
  - `src/lib/swarm/run.ts`
  - `src/lib/swarm/models.ts`
  - `README.md`
- Added dependencies: `kafkajs`, `ioredis`.
- Added optional Kafka publisher for swarm events.
- Added optional Redis fixed-window rate limiter for run starts and model attempts.
- Added `.env.example` documenting OpenRouter, Kafka, and Redis settings.

## Mandatory Kafka/Redis production hardening
- User clarified that Kafka and Redis must be deeply integrated and required, then requested a GitHub push after completion.
- Consulted current primary documentation for KafkaJS producer keys/idempotence, Apache Kafka 4.3.1 Docker/KRaft configuration, and ioredis connection behavior.
- Added/changed:
  - `src/lib/swarm/config.ts`: required environment validation, TLS/SASL options, and timeout settings.
  - `src/lib/swarm/infrastructure.ts`: cached concurrent Kafka/Redis readiness probes.
  - `src/app/api/health/route.ts`: HTTP 200/503 readiness endpoint.
  - `src/app/api/swarm/route.ts`: fail-closed readiness gate before model usage.
  - `src/lib/swarm/redis.ts`: required shared client, command/connect timeouts, reconnect behavior, PING, and disconnect helper.
  - `src/lib/swarm/kafka.ts`: required idempotent producer, explicit topic, `runId` key, headers/timestamp, admin topic check, and disconnect helper.
  - `src/lib/swarm/session.ts`: atomic Lua projection + Redis Stream write with deterministic sequence IDs.
  - `src/lib/swarm/bus.ts`: Redis-first delivery, required Kafka publish, terminal iterator errors on durable-delivery failure.
  - `src/app/api/swarm/[runId]/route.ts`: run ID validation and Redis 503 handling.
  - `docker-compose.yml`: pinned Kafka 4.3.1, Redis 8.8.0, health checks, volumes, topic provisioning, six partitions, seven-day retention.
  - `Dockerfile`, `.dockerignore`, standalone Next output, and pinned pnpm version.
  - `patches/kafkajs@2.2.4.patch`: prevents Node 24 negative-timer warnings when KafkaJS has no pending requests.
  - Removed build-time Google Fonts dependency for offline/restricted CI builds.
- Local configuration created in ignored `.env.local`; no secret was added.

### Commands and outcomes
- `pnpm lint`: passed.
- `pnpm exec tsc --noEmit`: passed.
- `docker-compose config`: passed.
- First `pnpm infra:up`: failed because Colima/Docker daemon was stopped.
- `colima start`: succeeded.
- Second `pnpm infra:up`: images downloaded; Kafka failed due named-volume ownership (`AccessDeniedException`).
- Added one-shot root volume initializer; broker itself remains UID 1000/non-root.
- Third `pnpm infra:up`: Kafka and Redis healthy; Kafka topic initializer completed.
- `pnpm infra:topic`: verified six partitions, leader/ISR metadata, replication factor 1 locally, and seven-day retention.
- Redis `PING`: returned `PONG`.
- First `pnpm dev`: sandbox denied port 3000; elevated host run succeeded.
- `/api/health`: returned HTTP 200 with Kafka and Redis healthy.
- Node 24 emitted a KafkaJS negative timeout warning; traced to empty request queue scheduling and added a pinned pnpm patch. Repeated health checks then exited cleanly without the warning.
- First patch install aborted due no TTY; offline retry failed because one tarball was uncached; normal lockfile install restored dependencies and applied the corrected patch.
- First production build failed because `next/font/google` could not reach Google Fonts; replaced with offline system font stacks.
- Second production build hit sandbox-only Turbopack port binding; host-permitted `pnpm build` passed.
- `docker build -t murmur:local .`: first attempt exposed missing patch copy and unpinned pnpm; corrected Dockerfile/package metadata; final build passed.
- Production image smoke test on `murmur_default`: `/api/health` returned HTTP 200 for Kafka and Redis; temporary app container removed.
- Removed the temporary project-local `.pnpm-store` cache.

### Revisit items
- Local Compose is intentionally single-node and not high availability. Production needs replicated managed Kafka/Redis, TLS/SASL, backups, alerts, and topic replication factor 3.
- Redis and Kafka cannot share a transaction. Current ordering preserves Redis as the recoverable source and fails visibly on Kafka error; strict no-gap asynchronous publication needs an outbox worker.

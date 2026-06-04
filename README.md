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

## Architecture

```
src/lib/swarm/
  types.ts         shared domain + streaming event types
  bus.ts           async event queue → interleaves parallel agent streams into one HTTP stream
  models.ts        env-configurable Claude model roles
  planner.ts       streamObject → validated task DAG
  worker.ts        per-specialist system prompts; streams tokens
  validator.ts     generateObject → score / approve / feedback
  orchestrator.ts  DAG wave scheduler + validator-retry loop + synthesis
src/app/api/swarm/route.ts   POST goal → Server-Sent Events stream
src/lib/store.ts             Zustand store; reduces events → graph state
src/components/               React Flow graph, animated nodes, live side panel
```

**Stack:** Next.js 16 (App Router) · TypeScript · Vercel AI SDK · OpenRouter (Claude + open models) · React Flow · Zustand.

**Models (mixed by role):** structured-output roles (planner, validator) run on a capable paid Claude model; plain-text roles (worker, synthesizer) run on free models. All slugs are env-overridable.

## Run it

```bash
pnpm install
cp .env.example .env.local   # add your OPENROUTER_API_KEY
pnpm dev                     # http://localhost:3000
```

Then give the swarm a goal, e.g. _"Create a go-to-market strategy for an AI code-review startup"_,
and watch it work.

## Deploy

One-click on Vercel — set `OPENROUTER_API_KEY` in project env vars. The `/api/swarm`
route streams for up to 5 minutes (`maxDuration = 300`).

---

Solo build by [@mightbeanshuu](https://github.com/mightbeanshuu).

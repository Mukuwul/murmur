import { GoalBar } from "@/components/GoalBar";
import { SwarmGraph } from "@/components/SwarmGraph";
import { SidePanel } from "@/components/SidePanel";
import { RecentRuns } from "@/components/RecentRuns";

export default function Home() {
  return (
    <main className="murmur-app">
      <header className="murmur-header">
        <div className="murmur-brand">
          <span className="murmur-logo">✺</span>
          <div>
            <h1>Murmur</h1>
            <p>An agent swarm that plans, delegates, validates, and synthesizes — live.</p>
          </div>
        </div>
        <a className="murmur-gh" href="https://github.com/mightbeanshuu/murmur" target="_blank" rel="noreferrer">
          Agent Swarms · Microsoft Build AI 2026
        </a>
      </header>

      <GoalBar />

      <section className="murmur-stage">
        <RecentRuns />
        <div className="murmur-graph">
          <SwarmGraph />
        </div>
        <SidePanel />
      </section>
    </main>
  );
}

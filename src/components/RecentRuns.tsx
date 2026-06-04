"use client";

import { useCallback, useEffect, useState } from "react";
import { useSwarm } from "@/lib/store";
import { deleteRun, listRuns, seedOnce, type SavedRun } from "@/lib/history";
import { SAMPLE_RUN } from "@/lib/sampleRun";

function ago(ts: number): string {
  const s = Math.max(1, Math.round((Date.now() - ts) / 1000));
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

export function RecentRuns() {
  const [runs, setRuns] = useState<SavedRun[]>([]);
  const loadRun = useSwarm((s) => s.loadRun);
  const runStatus = useSwarm((s) => s.runStatus);
  const goal = useSwarm((s) => s.goal);

  const refresh = useCallback(() => setRuns(listRuns()), []);

  useEffect(() => {
    seedOnce(SAMPLE_RUN);
    refresh();
  }, [refresh]);

  // Refresh the list whenever a run finishes (it just got persisted).
  useEffect(() => {
    if (runStatus === "done") refresh();
  }, [runStatus, refresh]);

  return (
    <aside className="murmur-runs">
      <div className="murmur-runs-head">
        <span>Recent runs</span>
        <span className="murmur-runs-count">{runs.length}</span>
      </div>
      <div className="murmur-runs-list">
        {runs.length === 0 && <p className="murmur-runs-empty">No runs yet. Deploy a swarm →</p>}
        {runs.map((r) => {
          const active = runStatus === "done" && r.goal === goal;
          const workers = r.agents.filter(
            (a) => a.agentType !== "planner" && a.agentType !== "validator",
          ).length;
          return (
            <button
              key={r.id}
              className={`murmur-run-item${active ? " is-active" : ""}`}
              onClick={() => loadRun(r)}
              title={r.goal}
            >
              <span className="murmur-run-goal">{r.goal}</span>
              <span className="murmur-run-meta">
                {ago(r.at)} · {workers} agents
                {r.stats ? ` · ${(r.stats.ms / 1000).toFixed(0)}s` : ""}
              </span>
              <span
                className="murmur-run-del"
                role="button"
                tabIndex={-1}
                title="Delete"
                onClick={(e) => {
                  e.stopPropagation();
                  deleteRun(r.id);
                  refresh();
                }}
              >
                ✕
              </span>
            </button>
          );
        })}
      </div>
    </aside>
  );
}

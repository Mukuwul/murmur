"use client";

import { useCallback, useRef } from "react";
import { useSwarm } from "./store";
import type { SwarmEvent } from "./swarm/types";

/** Opens the streaming swarm run and feeds every event into the store. */
export function useRunSwarm() {
  const reset = useSwarm((s) => s.reset);
  const setRunId = useSwarm((s) => s.setRunId);
  const apply = useSwarm((s) => s.apply);
  const abortRef = useRef<AbortController | null>(null);

  return useCallback(
    async (goal: string) => {
      abortRef.current?.abort();
      const ctrl = new AbortController();
      abortRef.current = ctrl;
      reset(goal);

      let res: Response;
      try {
        res = await fetch("/api/swarm", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ goal }),
          signal: ctrl.signal,
        });
      } catch (e) {
        apply({ kind: "error", message: (e as Error).message });
        return;
      }

      if (!res.ok || !res.body) {
        const msg = await res.json().catch(() => ({ error: "Request failed" }));
        apply({ kind: "error", message: msg.error ?? `HTTP ${res.status}` });
        return;
      }

      const runId = res.headers.get("x-murmur-run-id");
      if (runId) setRunId(runId);

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const chunks = buffer.split("\n\n");
        buffer = chunks.pop() ?? "";
        for (const chunk of chunks) {
          const line = chunk.trim();
          if (!line.startsWith("data:")) continue;
          try {
            apply(JSON.parse(line.slice(5).trim()) as SwarmEvent);
          } catch {
            // ignore partial / malformed frames
          }
        }
      }
    },
    [reset, setRunId, apply],
  );
}

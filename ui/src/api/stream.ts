import { useEffect, useRef, useState } from "react";
import type { MetricsSnapshot, Run, StreamFrame } from "./types";

export interface LiveState {
  run: Run | null;
  snapshots: MetricsSnapshot[];
  connected: boolean;
}

/**
 * Subscribe to the UI-app's multiplexed SSE stream (/api/stream) for the lifetime of the
 * component. The server sends a full snapshot frame first (so a reconnect resyncs), then live
 * state/progress/metrics/done frames. Frames carry a monotonic seq so a gap can be detected.
 */
export function useLiveStream(capacity = 900): LiveState {
  const [run, setRun] = useState<Run | null>(null);
  const [snapshots, setSnapshots] = useState<MetricsSnapshot[]>([]);
  const [connected, setConnected] = useState(false);
  const lastSeq = useRef(0);

  useEffect(() => {
    const source = new EventSource("/api/stream");

    const dispatch = (event: MessageEvent) => {
      let frame: StreamFrame;
      try {
        frame = JSON.parse(event.data) as StreamFrame;
      } catch {
        return; // keep-alive comment / malformed frame
      }
      if (frame.seq != null) lastSeq.current = frame.seq;

      switch (frame.type) {
        case "snapshot":
          setRun(frame.snapshot.current);
          break;
        case "state":
          setRun((prev) => (prev ? { ...prev, state: frame.state } : prev));
          break;
        case "progress":
          setRun((prev) =>
            prev
              ? { ...prev, progress: { sent: frame.sent, failed: frame.failed }, elapsed_seconds: frame.elapsed_seconds }
              : prev,
          );
          break;
        case "metrics":
          setSnapshots((prev) => {
            const next = prev.length >= capacity ? prev.slice(prev.length - capacity + 1) : prev;
            return [...next, frame.snapshot];
          });
          break;
        case "done":
          setRun((prev) =>
            prev ? { ...prev, state: "done", summary: frame.summary, report_path: frame.report_path } : prev,
          );
          break;
        case "error":
          setRun((prev) => (prev ? { ...prev, state: "failed" } : prev));
          break;
      }
    };

    // The server emits NAMED SSE events (event: <frame.type>). EventSource only routes unnamed
    // events to onmessage, so register a listener per known frame type plus onmessage as a
    // fallback for any unnamed frame.
    const types = ["snapshot", "state", "started", "progress", "metrics", "done", "error"] as const;
    types.forEach((t) => source.addEventListener(t, dispatch as EventListener));
    source.onmessage = dispatch;
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);

    return () => source.close();
  }, [capacity]);

  return { run, snapshots, connected };
}


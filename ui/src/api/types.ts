// Types mirroring the UI-app server (ui/server) contract. The UI-app is a separate control
// plane from the zip01 backend (system-under-test); these are NOT the grader-facing shapes.

export type ScenarioKind = "baseline" | "burst" | "custom";

export interface LoadTestConfig {
  scenario: ScenarioKind;
  devices: number;
  duration_seconds: number;
  rps: number;
  connections: number;
  metrics_interval_seconds: number;
  burst: boolean;
}

export type RunState = "idle" | "running" | "stopping" | "done" | "failed";

export interface RunProgress {
  sent: number;
  failed: number;
}

export interface RunSummary {
  duration_seconds: number;
  planned: number;
  dispatched: number;
  sent: number;
  failed: number;
  throughput_rps: number;
  fall_warns_sent: number;
  sse_alarms_received: number;
  alarm_latency_p95_ms: number | null;
}

export interface Run {
  run_id: string;
  state: RunState;
  started_at: number;
  elapsed_seconds: number;
  config: LoadTestConfig & { target: string };
  progress: RunProgress;
  summary: RunSummary | null;
  report_path: string | null;
}

/** Mirrors helpers/_loadtest.py MetricsSnapshot — the live charts consume this shape. */
export interface MetricsSnapshot {
  elapsed_seconds: number;
  phase: string;
  counters: Record<string, number>;
}

/** One frame on the multiplexed /api/stream SSE channel. */
export type StreamFrame =
  | { seq?: number; type: "snapshot"; snapshot: { current: Run | null; runs: Run[] } }
  | { seq?: number; type: "state"; state: RunState; run_id: string | null }
  | { seq?: number; type: "started"; config: Record<string, unknown> }
  | { seq?: number; type: "progress"; elapsed_seconds: number; phase: string; sent: number; failed: number }
  | { seq?: number; type: "metrics"; snapshot: MetricsSnapshot }
  | { seq?: number; type: "done"; summary: RunSummary; report_path: string }
  | { seq?: number; type: "error"; message: string };

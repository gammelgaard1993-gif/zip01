import type { LoadTestConfig, Run } from "./types";

const JSON_HEADERS = { "Content-Type": "application/json" };

async function parse<T>(response: Response): Promise<T> {
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = (body as { detail?: { error?: string } }).detail;
    throw new Error(detail?.error ?? `request failed (${response.status})`);
  }
  return body as T;
}

export function startRun(config: LoadTestConfig): Promise<Run> {
  return fetch("/api/runs", {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(config),
  }).then(parse<Run>);
}

export function stopRun(runId: string): Promise<Run> {
  return fetch(`/api/runs/${runId}/stop`, { method: "POST", headers: JSON_HEADERS }).then(
    parse<Run>,
  );
}

export function getCurrentRun(): Promise<Run | { state: "idle"; run_id: null }> {
  return fetch("/api/runs/current").then(parse<Run | { state: "idle"; run_id: null }>);
}

export function listRuns(): Promise<{ runs: Run[] }> {
  return fetch("/api/runs").then(parse<{ runs: Run[] }>);
}

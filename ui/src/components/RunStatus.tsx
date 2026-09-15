import type { Run } from "../api/types";

interface Props {
  run: Run | null;
  connected: boolean;
  onStop: () => void;
}

export default function RunStatusView({ run, connected, onStop }: Props) {
  const running = run?.state === "running" || run?.state === "stopping";
  const state = run?.state ?? "idle";
  return (
    <section className="kpis">
      <article className="kpi">
        <span>Run state</span>
        <strong data-state={state}>{state}</strong>
      </article>
      <article className="kpi">
        <span>Connection</span>
        <strong>{connected ? "live" : "offline"}</strong>
      </article>
      <article className="kpi">
        <span>Events sent</span>
        <strong>{run ? run.progress.sent.toLocaleString() : "-"}</strong>
      </article>
      <article className="kpi">
        <span>Failed</span>
        <strong>{run ? run.progress.failed.toLocaleString() : "-"}</strong>
      </article>
      <article className="kpi">
        <span>Elapsed</span>
        <strong>{run ? `${Math.round(run.elapsed_seconds)}s` : "-"}</strong>
      </article>
      <article className="kpi">
        <span>&nbsp;</span>
        <strong>
          <button type="button" disabled={!running} onClick={onStop}>
            Stop run
          </button>
        </strong>
      </article>
    </section>
  );
}

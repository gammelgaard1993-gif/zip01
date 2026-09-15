import { useCallback } from "react";
import RunConfigForm from "./components/RunConfigForm";
import RunStatusView from "./components/RunStatus";
import LiveDashboard from "./components/LiveDashboard";
import { useLiveStream } from "./api/stream";
import { startRun, stopRun } from "./api/client";
import type { LoadTestConfig } from "./api/types";

export default function App() {
  const { run, snapshots, connected } = useLiveStream();
  const running = run?.state === "running" || run?.state === "stopping";

  const handleRun = useCallback(async (config: LoadTestConfig) => {
    try {
      await startRun(config);
    } catch (error) {
      console.error("failed to start run", error);
    }
  }, []);

  const handleStop = useCallback(async () => {
    if (!run) return;
    try {
      await stopRun(run.run_id);
    } catch (error) {
      console.error("failed to stop run", error);
    }
  }, [run]);

  return (
    <main>
      <h1>Load Test Console</h1>
      <p>Configure a scenario, run it, and follow the pipeline live.</p>
      <RunConfigForm running={running} onRun={handleRun} />
      <RunStatusView run={run} connected={connected} onStop={handleStop} />
      <LiveDashboard snapshots={snapshots} duration={run?.config.duration_seconds} />
    </main>
  );
}

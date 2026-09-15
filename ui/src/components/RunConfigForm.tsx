import { useState } from "react";
import type { LoadTestConfig, ScenarioKind } from "../api/types";

const PRESETS: Record<Exclude<ScenarioKind, "custom">, Omit<LoadTestConfig, "scenario">> = {
  baseline: { devices: 500, duration_seconds: 90, rps: 1, connections: 32, metrics_interval_seconds: 1, burst: false },
  burst: { devices: 500, duration_seconds: 90, rps: 1, connections: 32, metrics_interval_seconds: 1, burst: true },
};

interface Props {
  running: boolean;
  onRun: (config: LoadTestConfig) => void;
}

export default function RunConfigForm({ running, onRun }: Props) {
  const [scenario, setScenario] = useState<ScenarioKind>("baseline");
  const [config, setConfig] = useState<Omit<LoadTestConfig, "scenario">>(PRESETS.baseline);

  const applyScenario = (next: ScenarioKind) => {
    setScenario(next);
    if (next !== "custom") setConfig(PRESETS[next]);
  };

  const set = <K extends keyof typeof config>(key: K, value: (typeof config)[K]) =>
    setConfig((prev) => ({ ...prev, [key]: value }));

  const numberField = (
    label: string,
    key: Exclude<keyof typeof config, "burst">,
    step = 1,
  ) => (
    <label>
      {label}
      <input
        type="number"
        step={step}
        value={config[key]}
        disabled={running}
        onChange={(e) => set(key, Number(e.target.value))}
      />
    </label>
  );

  return (
    <section className="controls">
      <label>
        Scenario
        <select value={scenario} disabled={running} onChange={(e) => applyScenario(e.target.value as ScenarioKind)}>
          <option value="baseline">Baseline</option>
          <option value="burst">Burst (10x)</option>
          <option value="custom">Custom</option>
        </select>
      </label>
      {numberField("Devices", "devices")}
      {numberField("Duration (s)", "duration_seconds")}
      {numberField("Events / device / s", "rps", 0.1)}
      {numberField("Connections", "connections")}
      {numberField("Metrics interval (s)", "metrics_interval_seconds", 0.5)}
      <label className="checkbox">
        <input
          type="checkbox"
          checked={config.burst}
          disabled={running || scenario !== "custom"}
          onChange={(e) => set("burst", e.target.checked)}
        />
        Burst (10x)
      </label>
      <button type="button" disabled={running} onClick={() => onRun({ scenario, ...config })}>
        Run test
      </button>
    </section>
  );
}

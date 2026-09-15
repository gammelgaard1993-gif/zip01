import { useEffect, useRef } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { MetricsSnapshot } from "../../api/types";

interface Props {
  title: string;
  /** counter key for the rate line (delta per second between snapshots) */
  rateKey: string;
  /** counter keys summed into the queue-depth series */
  queueKeys: [string, string];
  color: string;
  snapshots: MetricsSnapshot[];
  /** total run duration in seconds; pins the X axis so progress is centered, not bottom-hugging */
  duration?: number;
}

/** Ceiling with headroom so the data line sits centered rather than pinned to the axis max. */
function paddedCeiling(peak: number, floor: number): number {
  const target = Math.max(floor, peak * 1.35);
  const magnitude = Math.pow(10, Math.floor(Math.log10(target)));
  const fraction = target / magnitude;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * magnitude;
}

/**
 * Live rate + queue-depth chart backed by uPlot. Replaces the hand-rolled canvas
 * stageChart from helpers/_loadtest.py. Data is rebuilt from the snapshot buffer;
 * uPlot handles the redraw efficiently for high-rate streams.
 */
export default function StageChart({ title, rateKey, queueKeys, color, snapshots, duration }: Props) {
  const rootRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  useEffect(() => {
    if (!rootRef.current) return;
    const options: uPlot.Options = {
      title,
      width: rootRef.current.clientWidth,
      height: 260,
      scales: {
        x: { time: false },
        rate: { auto: false },
        queue: { auto: false },
      },
      axes: [
        { stroke: "#64707a" },
        { scale: "rate", stroke: "#64707a" },
        { scale: "queue", stroke: "#bd3043", side: 1 },
      ],
      series: [
        {},
        { label: "rate/s", stroke: color, width: 2, scale: "rate" },
        { label: "queue", stroke: "#bd3043", width: 1, scale: "queue" },
      ],
    };
    const chart = new uPlot(options, [[], [], []], rootRef.current);
    chartRef.current = chart;
    return () => {
      chart.destroy();
      chartRef.current = null;
    };
  }, [title, rateKey, queueKeys, color]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const x: number[] = [];
    const rate: number[] = [];
    const queue: number[] = [];
    snapshots.forEach((current, index) => {
      const prior = snapshots[Math.max(0, index - 1)];
      const seconds = Math.max(0.001, current.elapsed_seconds - prior.elapsed_seconds);
      const delta = (current.counters[rateKey] ?? 0) - (prior.counters[rateKey] ?? 0);
      x.push(current.elapsed_seconds);
      rate.push(index === 0 ? 0 : Math.max(0, delta) / seconds);
      queue.push((current.counters[queueKeys[0]] ?? 0) + (current.counters[queueKeys[1]] ?? 0));
    });
    chart.setData([x, rate, queue]);

    // Pin X to the full run duration (with a small margin) so early samples don't stretch the
    // axis; pad Y above the observed peak so the line sits centered instead of at the ceiling.
    const xMax = duration && duration > 0 ? duration * 1.02 : Math.max(1, x[x.length - 1] ?? 1) * 1.05;
    chart.setScale("x", { min: 0, max: xMax });
    chart.setScale("rate", { min: 0, max: paddedCeiling(Math.max(0, ...rate), 10) });
    chart.setScale("queue", { min: 0, max: paddedCeiling(Math.max(0, ...queue), 10) });
  }, [snapshots, rateKey, queueKeys, duration]);

  return <div className="panel"><div ref={rootRef} /></div>;
}

import StageChart from "./charts/StageChart";
import type { MetricsSnapshot } from "../api/types";

interface Props {
  snapshots: MetricsSnapshot[];
  /** total run duration in seconds; used to pin the charts' X axis for the whole run */
  duration?: number;
}

const STAGES = [
  { title: "Ingress Requests / Second", rateKey: "events_ingested_total", queueKeys: ["queue_depth_high", "queue_depth_normal"], color: "#1677b8" },
  { title: "SQLite Writer Batches / Second", rateKey: "sqlite_writer_batches_committed_total", queueKeys: ["sqlite_writer_queue_depth_priority", "sqlite_writer_queue_depth_normal"], color: "#d56b22" },
  { title: "Worker Events / Second", rateKey: "worker_events_handled_total", queueKeys: ["worker_queue_depth_high", "worker_queue_depth_normal"], color: "#178056" },
] as const;

export default function LiveDashboard({ snapshots, duration }: Props) {
  return (
    <section className="grid">
      {STAGES.map((stage) => (
        <StageChart
          key={stage.title}
          title={stage.title}
          rateKey={stage.rateKey}
          queueKeys={[stage.queueKeys[0], stage.queueKeys[1]]}
          color={stage.color}
          snapshots={snapshots}
          duration={duration}
        />
      ))}
    </section>
  );
}

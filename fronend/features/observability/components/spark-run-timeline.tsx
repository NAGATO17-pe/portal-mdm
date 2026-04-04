"use client";

import { useReconnectIndicator } from "@/hooks/use-reconnect-indicator";
import { useSparkStream } from "@/features/observability/hooks/use-spark-stream";

type Props = {
  runId: string;
};

export const SparkRunTimeline = ({ runId }: Props) => {
  const { event, status } = useSparkStream(runId);
  const connectionLabel = useReconnectIndicator(status === "connected");

  return (
    <section className="rounded-lg bg-white/5 p-4 shadow-card" aria-live="polite">
      <h2 className="text-lg font-semibold">Observabilidad Spark</h2>
      <p className="text-sm text-foreground/70">Estado de conexión: {connectionLabel}</p>
      {event ? (
        <dl className="mt-3 grid gap-2 text-sm md:grid-cols-2">
          <div>
            <dt className="text-foreground/70">App ID</dt>
            <dd>{event.spark_app_id}</dd>
          </div>
          <div>
            <dt className="text-foreground/70">Stage</dt>
            <dd>{event.stage}</dd>
          </div>
          <div>
            <dt className="text-foreground/70">Status</dt>
            <dd>{event.status}</dd>
          </div>
          <div>
            <dt className="text-foreground/70">Throughput</dt>
            <dd>{event.throughput_rows_sec} rows/s</dd>
          </div>
        </dl>
      ) : (
        <p className="mt-3 text-sm">Esperando eventos del backend...</p>
      )}
    </section>
  );
};

"use client";

import { useDashboardMetrics } from "@/features/dashboard/hooks/use-dashboard-metrics";
import { LoadingState } from "@/components/states/loading-state";

export const MetricsCards = () => {
  const { data, isLoading, isError } = useDashboardMetrics();

  if (isLoading) return <LoadingState label="Cargando métricas" />;
  if (isError || !data) return <p role="alert">No fue posible obtener métricas del dashboard.</p>;

  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      <article className="rounded-lg bg-white/5 p-4 shadow-card">
        <p className="text-sm text-foreground/70">Ejecuciones activas</p>
        <p className="text-2xl font-semibold">{data.running_runs}</p>
      </article>
      <article className="rounded-lg bg-white/5 p-4 shadow-card">
        <p className="text-sm text-foreground/70">Fallidas 24h</p>
        <p className="text-2xl font-semibold">{data.failed_runs_24h}</p>
      </article>
      <article className="rounded-lg bg-white/5 p-4 shadow-card">
        <p className="text-sm text-foreground/70">Duración promedio (s)</p>
        <p className="text-2xl font-semibold">{data.avg_duration_sec}</p>
      </article>
      <article className="rounded-lg bg-white/5 p-4 shadow-card">
        <p className="text-sm text-foreground/70">Última corrida exitosa</p>
        <p className="text-sm font-semibold">{data.last_success_at ?? "Sin datos"}</p>
      </article>
    </div>
  );
};

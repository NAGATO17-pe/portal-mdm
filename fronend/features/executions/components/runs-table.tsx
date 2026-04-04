"use client";

import { useRuns } from "@/features/executions/hooks/use-runs";
import { StatusBadge } from "@/components/ui/status-badge";
import { LoadingState } from "@/components/states/loading-state";
import { EmptyState } from "@/components/states/empty-state";

export const RunsTable = () => {
  const { data, isLoading, isError } = useRuns();

  if (isLoading) return <LoadingState label="Cargando ejecuciones" />;
  if (isError) return <p role="alert">No se pudieron cargar las ejecuciones.</p>;
  if (!data?.length) return <EmptyState title="Sin ejecuciones" description="Aún no hay corridas registradas." />;

  return (
<<<<<<< HEAD
    <div className="glass-card overflow-x-auto p-4">
=======
    <div className="overflow-x-auto rounded-lg border border-white/10 bg-white/5 p-4 shadow-card">
>>>>>>> main
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-foreground/70">
          <tr>
            <th className="p-2">Run ID</th>
            <th className="p-2">Pipeline</th>
            <th className="p-2">Estado</th>
            <th className="p-2">Progreso</th>
          </tr>
        </thead>
        <tbody>
          {data.map((run) => (
<<<<<<< HEAD
            <tr key={run.id} className="border-t border-white/15 transition hover:bg-white/10">
=======
            <tr key={run.id} className="border-t border-white/10">
>>>>>>> main
              <td className="p-2 font-mono text-xs">{run.id}</td>
              <td className="p-2">{run.pipelineName}</td>
              <td className="p-2">
                <StatusBadge status={run.status} />
              </td>
              <td className="p-2">{run.progress}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

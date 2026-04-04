import { RunsTable } from "@/features/executions/components/runs-table";

export default function ExecutionsPage() {
  return (
    <section className="space-y-4">
      <h2 className="text-xl font-semibold">Control de ejecuciones ETL</h2>
      <RunsTable />
    </section>
  );
}

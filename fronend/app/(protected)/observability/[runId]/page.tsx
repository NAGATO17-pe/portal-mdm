import { SparkRunTimeline } from "@/features/observability/components/spark-run-timeline";

type Props = {
  params: {
    runId: string;
  };
};

export default function ObservabilityRunPage({ params }: Props) {
  return (
    <section className="space-y-4">
      <h2 className="text-xl font-semibold">Observabilidad y Spark</h2>
      <p className="text-sm text-foreground/70">Run ID: {params.runId}</p>
      <SparkRunTimeline runId={params.runId} />
    </section>
  );
}

import Link from "next/link";

export default function ObservabilityIndexPage() {
  return (
    <section className="space-y-4">
      <h2 className="text-xl font-semibold">Observabilidad y Spark</h2>
      <p className="text-sm text-foreground/70">Selecciona una ejecución para abrir el stream técnico de Spark.</p>
      <Link href="/observability/demo-run" className="inline-flex rounded-md bg-primary px-3 py-2 text-sm font-medium text-white">
        Abrir ejecución demo
      </Link>
    </section>
  );
}

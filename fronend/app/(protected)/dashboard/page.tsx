import { MetricsCards } from "@/features/dashboard/components/metrics-cards";

export default function DashboardPage() {
  return (
    <section className="space-y-4">
      <h2 className="text-xl font-semibold">Dashboard operativo</h2>
      <MetricsCards />
    </section>
  );
}

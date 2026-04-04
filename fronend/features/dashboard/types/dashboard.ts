import { z } from "zod";

export const dashboardMetricsSchema = z.object({
  running_runs: z.number().int().nonnegative(),
  failed_runs_24h: z.number().int().nonnegative(),
  avg_duration_sec: z.number().nonnegative(),
  last_success_at: z.string().nullable()
});

export type DashboardMetrics = z.infer<typeof dashboardMetricsSchema>;

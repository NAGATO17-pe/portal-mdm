export const APP_NAME = "ACP ETL Control Portal";

export const QUERY_KEYS = {
  dashboard: ["dashboard"] as const,
  runs: (filters?: Record<string, unknown>) => ["runs", filters] as const,
  runById: (runId: string) => ["runs", runId] as const,
  sparkByRun: (runId: string) => ["spark", runId] as const,
  logsByRun: (runId: string) => ["logs", runId] as const
};

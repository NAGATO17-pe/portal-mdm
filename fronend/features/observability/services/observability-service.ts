import { apiClient } from "@/services/api/http-client";
import { sparkExecutionSchema } from "@/features/observability/types/spark";

export const fetchSparkExecution = (runId: string) =>
  apiClient.request({ path: `/api/v1/observability/spark/${runId}`, schema: sparkExecutionSchema });

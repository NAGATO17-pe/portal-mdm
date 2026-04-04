import { apiClient } from "@/services/api/http-client";
import { dashboardMetricsSchema } from "@/features/dashboard/types/dashboard";

export const fetchDashboardMetrics = async () =>
  apiClient.request({ path: "/api/v1/dashboard/metrics", schema: dashboardMetricsSchema });

"use client";

import { useQuery } from "@tanstack/react-query";
import { QUERY_KEYS } from "@/lib/constants";
import { fetchDashboardMetrics } from "@/features/dashboard/services/dashboard-service";

export const useDashboardMetrics = () =>
  useQuery({
    queryKey: QUERY_KEYS.dashboard,
    queryFn: fetchDashboardMetrics,
    staleTime: 15_000,
    refetchInterval: 30_000
  });

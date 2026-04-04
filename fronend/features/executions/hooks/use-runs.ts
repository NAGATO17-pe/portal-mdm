"use client";

import { useQuery } from "@tanstack/react-query";
import { QUERY_KEYS } from "@/lib/constants";
import { fetchRuns } from "@/features/executions/services/executions-service";

export const useRuns = () =>
  useQuery({
    queryKey: QUERY_KEYS.runs(),
    queryFn: fetchRuns,
    staleTime: 10_000,
    refetchInterval: 15_000
  });

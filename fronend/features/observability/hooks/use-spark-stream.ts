"use client";

import { useEffect, useMemo, useState } from "react";
import { getSseUrl } from "@/lib/env";
import { EventStreamClient } from "@/services/streaming/event-stream";
import type { SparkExecution } from "@/features/observability/types/spark";

export const useSparkStream = (runId: string) => {
  const client = useMemo(() => new EventStreamClient<SparkExecution>(), []);
  const [event, setEvent] = useState<SparkExecution | null>(null);
  const [status, setStatus] = useState<"connecting" | "connected" | "error">("connecting");

  useEffect(() => {
<<<<<<< HEAD
=======
    setStatus("connecting");

>>>>>>> main
    client.connect(getSseUrl(`/api/v1/stream/runs/${runId}`), {
      onOpen: () => setStatus("connected"),
      onError: () => setStatus("error"),
      onMessage: (payload) => setEvent(payload)
    });

    return () => client.disconnect();
  }, [client, runId]);

  return { event, status };
};

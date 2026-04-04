import { z } from "zod";

export const sparkExecutionSchema = z.object({
  run_id: z.string(),
  spark_app_id: z.string(),
  stage: z.string(),
  status: z.string(),
  throughput_rows_sec: z.number().nonnegative(),
  duration_sec: z.number().nonnegative()
});

export type SparkExecution = z.infer<typeof sparkExecutionSchema>;

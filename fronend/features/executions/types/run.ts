import { z } from "zod";
import { runStatusSchema } from "@/schemas/common";

export const runDtoSchema = z.object({
  run_id: z.string(),
  pipeline_name: z.string(),
  status: runStatusSchema,
  started_at: z.string().nullable(),
  finished_at: z.string().nullable(),
  progress_percent: z.number().min(0).max(100)
});

export const runListSchema = z.array(runDtoSchema);

export type RunDto = z.infer<typeof runDtoSchema>;
export type RunModel = {
  id: string;
  pipelineName: string;
  status: z.infer<typeof runStatusSchema>;
  startedAt: string | null;
  finishedAt: string | null;
  progress: number;
};

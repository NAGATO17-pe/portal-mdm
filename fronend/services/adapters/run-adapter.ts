import type { RunDto, RunModel } from "@/features/executions/types/run";

export const toRunModel = (dto: RunDto): RunModel => ({
  id: dto.run_id,
  pipelineName: dto.pipeline_name,
  status: dto.status,
  startedAt: dto.started_at,
  finishedAt: dto.finished_at,
  progress: dto.progress_percent
});

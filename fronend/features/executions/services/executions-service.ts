import { apiClient } from "@/services/api/http-client";
import { toRunModel } from "@/services/adapters/run-adapter";
import { runDtoSchema, runListSchema, type RunModel } from "@/features/executions/types/run";

export const fetchRuns = async (): Promise<RunModel[]> => {
  const response = await apiClient.request({ path: "/api/v1/etl/runs", schema: runListSchema });
  return response.map(toRunModel);
};

export const fetchRunDetails = async (runId: string): Promise<RunModel> => {
  const response = await apiClient.request({ path: `/api/v1/etl/runs/${runId}`, schema: runDtoSchema });
  return toRunModel(response);
};

export const startRun = async (pipelineName: string): Promise<{ runId: string }> => {
  const payloadSchema = runDtoSchema.pick({ run_id: true });
  const response = await apiClient.request({
    path: "/api/v1/etl/runs",
    method: "POST",
    body: { pipeline_name: pipelineName },
    schema: payloadSchema
  });

  return { runId: response.run_id };
};

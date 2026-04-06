import { z } from "zod";
import { getApiBaseUrl } from "@/lib/env";

type RequestOptions<T> = {
  path: string;
  method?: "GET" | "POST" | "PATCH" | "PUT" | "DELETE";
  body?: unknown;
  schema: z.ZodSchema<T>;
  signal?: AbortSignal;
};

export class ApiClient {
  async request<T>({ path, method = "GET", body, schema, signal }: RequestOptions<T>): Promise<T> {
    const response = await fetch(`${getApiBaseUrl()}${path}`, {
      method,
      signal,
      credentials: "include",
      headers: {
        "Content-Type": "application/json"
      },
      body: body ? JSON.stringify(body) : undefined,
      cache: "no-store"
    });

    if (!response.ok) {
      throw new Error(`API error (${response.status}) while requesting ${path}`);
    }

    const data: unknown = await response.json();
    return schema.parse(data);
  }
}

export const apiClient = new ApiClient();

const DEFAULT_API_BASE_URL = "http://localhost:8000";

export const getApiBaseUrl = (): string => process.env.NEXT_PUBLIC_API_URL ?? DEFAULT_API_BASE_URL;

export const getSseUrl = (path: string): string => `${getApiBaseUrl()}${path}`;

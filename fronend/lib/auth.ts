import { cookies } from "next/headers";

export const SESSION_COOKIE = "acp_session";

export const isAuthenticated = (): boolean => Boolean(cookies().get(SESSION_COOKIE)?.value);

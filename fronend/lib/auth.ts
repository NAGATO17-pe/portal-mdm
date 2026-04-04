import { cookies } from "next/headers";

export const SESSION_COOKIE = "acp_session";

export const isAuthenticated = async (): Promise<boolean> => {
  const cookieStore = await cookies();
  return Boolean(cookieStore.get(SESSION_COOKIE)?.value);
};

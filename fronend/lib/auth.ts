import { cookies } from "next/headers";

export const SESSION_COOKIE = "acp_session";

<<<<<<< HEAD
export const isAuthenticated = async (): Promise<boolean> => {
  const cookieStore = await cookies();
  return Boolean(cookieStore.get(SESSION_COOKIE)?.value);
};
=======
export const isAuthenticated = (): boolean => Boolean(cookies().get(SESSION_COOKIE)?.value);
>>>>>>> main

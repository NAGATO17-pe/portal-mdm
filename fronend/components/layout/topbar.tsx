import { APP_NAME } from "@/lib/constants";

export const Topbar = () => (
<<<<<<< HEAD
  <header className="glass-card flex h-14 items-center justify-between px-4">
    <h1 className="text-sm font-semibold md:text-base">{APP_NAME}</h1>
    <form action="/api/auth/logout" method="post">
      <button className="rounded-md border border-white/30 bg-white/10 px-3 py-1.5 text-xs transition hover:bg-white/20" type="submit">
=======
  <header className="flex h-14 items-center justify-between rounded-lg bg-white/5 px-4 backdrop-blur">
    <h1 className="text-sm font-semibold md:text-base">{APP_NAME}</h1>
    <form action="/api/auth/logout" method="post">
      <button className="rounded-md border border-white/20 px-3 py-1.5 text-xs hover:bg-white/10" type="submit">
>>>>>>> main
        Cerrar sesión
      </button>
    </form>
  </header>
);

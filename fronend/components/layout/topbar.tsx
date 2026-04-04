import { APP_NAME } from "@/lib/constants";

export const Topbar = () => (
  <header className="glass-card flex h-14 items-center justify-between px-4">
    <h1 className="text-sm font-semibold md:text-base">{APP_NAME}</h1>
    <form action="/api/auth/logout" method="post">
      <button className="rounded-md border border-white/30 bg-white/10 px-3 py-1.5 text-xs transition hover:bg-white/20" type="submit">
        Cerrar sesión
      </button>
    </form>
  </header>
);

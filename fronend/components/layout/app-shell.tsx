import type { ReactNode } from "react";
import { Sidebar } from "@/components/layout/sidebar";
import { Topbar } from "@/components/layout/topbar";

type Props = { children: ReactNode };

export const AppShell = ({ children }: Props) => (
<<<<<<< HEAD
  <div className="premium-grid mx-auto flex min-h-screen max-w-[1440px] flex-col gap-4 p-4 md:flex-row">
    <Sidebar />
    <main className="flex-1 space-y-4">
      <Topbar />
      <section className="glass-card p-4 md:p-6">{children}</section>
=======
  <div className="mx-auto flex min-h-screen max-w-[1400px] flex-col gap-4 p-4 md:flex-row">
    <Sidebar />
    <main className="flex-1 space-y-4">
      <Topbar />
      {children}
>>>>>>> main
    </main>
  </div>
);

import { redirect } from "next/navigation";
import type { ReactNode } from "react";
import { isAuthenticated } from "@/lib/auth";
import { AppShell } from "@/components/layout/app-shell";

type Props = { children: ReactNode };

export default async function ProtectedLayout({ children }: Props) {
  if (!(await isAuthenticated())) {
    redirect("/login");
  }

  return <AppShell>{children}</AppShell>;
}

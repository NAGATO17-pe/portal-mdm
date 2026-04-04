import { redirect } from "next/navigation";
import { isAuthenticated } from "@/lib/auth";

export default async function HomePage(): Promise<void> {
  redirect((await isAuthenticated()) ? "/dashboard" : "/login");
}

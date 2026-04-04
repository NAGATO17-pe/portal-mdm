import { redirect } from "next/navigation";
<<<<<<< HEAD
import { isAuthenticated } from "@/lib/auth";

export default async function HomePage(): Promise<void> {
  redirect((await isAuthenticated()) ? "/dashboard" : "/login");
=======

export default function HomePage(): never {
  redirect("/dashboard");
>>>>>>> main
}

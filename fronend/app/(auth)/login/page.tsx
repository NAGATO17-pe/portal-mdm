import { redirect } from "next/navigation";
import { Button } from "@/components/ui/button";
import { isAuthenticated } from "@/lib/auth";

export default async function LoginPage() {
  if (await isAuthenticated()) {
    redirect("/dashboard");
  }

  return (
    <main className="mx-auto mt-20 max-w-md rounded-lg bg-white/5 p-6 shadow-card">
      <h1 className="mb-2 text-xl font-semibold">Iniciar sesión</h1>
      <p className="mb-6 text-sm text-foreground/70">Autenticación integrada con backend FastAPI (cookies HttpOnly).</p>
      <form className="space-y-4" action="/api/auth/mock-login" method="post">
        <div>
          <label className="mb-1 block text-sm" htmlFor="user">
            Usuario
          </label>
          <input id="user" name="user" required className="h-10 w-full rounded-md border border-white/20 bg-transparent px-3" />
        </div>
        <div>
          <label className="mb-1 block text-sm" htmlFor="password">
            Contraseña
          </label>
          <input id="password" name="password" required type="password" className="h-10 w-full rounded-md border border-white/20 bg-transparent px-3" />
        </div>
        <Button className="w-full" type="submit">
          Entrar
        </Button>
      </form>
    </main>
  );
}

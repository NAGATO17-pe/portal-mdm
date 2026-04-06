import Link from "next/link";

const links = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/executions", label: "Ejecuciones" },
  { href: "/observability", label: "Observabilidad" },
  { href: "/mdm", label: "MDM" },
  { href: "/settings", label: "Configuración" }
];

export const Sidebar = () => (
<<<<<<< HEAD
  <aside className="glass-card w-full max-w-56 p-3 md:min-h-[calc(100vh-2rem)]">
=======
  <aside className="w-full max-w-56 rounded-lg bg-white/5 p-3 backdrop-blur md:min-h-[calc(100vh-2rem)]">
>>>>>>> main
    <nav aria-label="Principal">
      <ul className="space-y-1">
        {links.map((link) => (
          <li key={link.href}>
<<<<<<< HEAD
            <Link className="block rounded-md px-3 py-2 text-sm transition hover:bg-white/20" href={link.href}>
=======
            <Link className="block rounded-md px-3 py-2 text-sm hover:bg-white/10" href={link.href}>
>>>>>>> main
              {link.label}
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  </aside>
);

import Link from "next/link";

const links = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/executions", label: "Ejecuciones" },
  { href: "/observability", label: "Observabilidad" },
  { href: "/mdm", label: "MDM" },
  { href: "/settings", label: "Configuración" }
];

export const Sidebar = () => (
  <aside className="w-full max-w-56 rounded-lg bg-white/5 p-3 backdrop-blur md:min-h-[calc(100vh-2rem)]">
    <nav aria-label="Principal">
      <ul className="space-y-1">
        {links.map((link) => (
          <li key={link.href}>
            <Link className="block rounded-md px-3 py-2 text-sm hover:bg-white/10" href={link.href}>
              {link.label}
            </Link>
          </li>
        ))}
      </ul>
    </nav>
  </aside>
);

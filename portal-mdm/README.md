# Portal MDM — ACP

Portal único multi-rol (Analistas / Administradores MDM / Ejecutivos) para la
plataforma ACP. Construido con Next.js 16 (App Router), TypeScript, Tailwind 4
y los lineamientos del documento `Plan de Mejora Continua`.

## Stack

- Next.js 16 (App Router, React 19, Turbopack dev)
- TypeScript 5
- Tailwind CSS 4 con design tokens (`app/globals.css`)
- shadcn/ui (Radix + Tailwind) — primitives instaladas, generación bajo demanda
- TanStack Table v8, TanStack Query
- React Hook Form + Zod
- Plotly.js + Recharts
- Lucide React (sin emojis)
- jose (decode/verify JWT en Edge runtime)

## Sprints implementados en este commit

- **Sprint 0** — Setup completo: proyecto generado con `create-next-app`,
  dependencias instaladas, design tokens dark enterprise aplicados, fonts Inter
  + JetBrains Mono cargadas vía `next/font`, estructura de carpetas final.
- **Sprint 1 (parcial)** — Auth & Layout base:
  - `proxy.ts` (Next 16 reemplaza el legacy `middleware.ts`) con validación de
    JWT optimista y RBAC por rol.
  - Route handlers: `POST /api/auth/login`, `POST /api/auth/logout`,
    `GET /api/auth/me` (cookie httpOnly de sesión).
  - `/login` con React Hook Form + Zod, accesibilidad básica (focus visible,
    `aria-invalid`, `role="alert"`).
  - Layouts por rol con `RoleShell` (sidebar + topbar).

Las páginas internas son placeholders (`PagePlaceholder`) hasta los Sprints 2-6.

## Variables de entorno

Copia `.env.example` a `.env.local`:

```bash
cp .env.example .env.local
```

| Variable              | Propósito                                         |
| --------------------- | ------------------------------------------------- |
| `NEXT_PUBLIC_API_URL` | Base URL del FastAPI backend                      |
| `JWT_COOKIE_NAME`     | Nombre de la cookie httpOnly de sesión            |
| `JWT_PUBLIC_SECRET`   | Secret HMAC para verificar firma JWT (producción) |

## Scripts

```bash
npm run dev     # next dev (Turbopack)
npm run build   # build de producción
npm run start   # servidor de producción
npm run lint    # eslint
```

## Convenciones

- Sin emojis en UI: usar Lucide React siempre.
- Touch targets ≥ 44 px, focus rings visibles, contraste WCAG AA.
- Server Components por defecto; `"use client"` solo donde se necesite.
- `proxy.ts` solo hace gating optimista — la verificación criptográfica del JWT
  vive en `lib/auth/session.ts` y corre en Server Components / route handlers.

## Roles → rutas

| Rol        | Prefijos                                            | Home        |
| ---------- | --------------------------------------------------- | ----------- |
| Analista   | `/explore`, `/models`, `/reports`                   | `/explore`  |
| Admin      | `/entities`, `/workflows`, `/quality`, `/audit`     | `/entities` |
| Ejecutivo  | `/overview`                                         | `/overview` |

Mapping centralizado en `lib/auth/rbac.ts`.

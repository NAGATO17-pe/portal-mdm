# ACP ETL Portal Frontend

Implementación inicial del portal de control ETL con **Next.js App Router + TypeScript + Tailwind CSS**.

## Arquitectura implementada

- `app/`: rutas, layouts y composición por área funcional.
- `components/`: componentes UI reutilizables, estados y layout.
- `features/`: módulos de dominio (`dashboard`, `executions`, `observability`, `mdm`, `settings`, `auth`).
- `services/`: cliente HTTP tipado, streaming SSE y adaptadores DTO -> modelo de UI.
- `schemas/`: validación con Zod de contratos de backend.
- `hooks/`: hooks transversales.
- `store/`: base para estado global compartido.
- `styles/`: tokens globales + dark mode.

## Flujo backend

- REST encapsulado con `ApiClient` y validación de respuestas.
- SSE encapsulado con `EventStreamClient` y reconexión exponencial.
- Separación estricta de visualización vs. orquestación (Spark solo se representa desde datos del backend).

## Seguridad y sesión

- Las rutas protegidas validan cookie de sesión (`acp_session`) en server layout.
- Login/logout mock vía rutas `app/api/auth/*` usando cookie `HttpOnly`.
- Esta capa mock permite migrar después a integración real con FastAPI sin romper componentes.

## Calidad

- TypeScript estricto
- ESLint con reglas Next.js + jsx-a11y
- Vitest + Testing Library (base)
- Playwright smoke e2e
- Workflow CI (`.github/workflows/frontend-ci.yml`) para lint, typecheck, unit tests, build y e2e smoke

## Inicio rápido

<<<<<<< HEAD
Variables útiles en desarrollo:

- `NEXT_PUBLIC_API_URL`: URL base del backend FastAPI.
- `NEXT_ALLOWED_DEV_ORIGINS`: hosts separados por coma para permitir HMR desde red local (ej. `http://192.168.18.70:3000`).


=======
>>>>>>> main
```bash
npm install
npm run dev
```
<<<<<<< HEAD


## UI Premium y rendimiento

- Estética glassmorphism empresarial con animaciones suaves (`glass-card`, `premium-grid`).
- Reducción de redirecciones: `/` ahora decide sesión y redirige directo a `/dashboard` o `/login`.
- Stack actualizado a versiones recientes para mejor compatibilidad y optimización en Next.js 16.
=======
>>>>>>> main

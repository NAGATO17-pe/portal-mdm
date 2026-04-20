# ROADMAP TÉCNICO ACP DWH
## Cierre Arquitectónico | Corte 2026-04-15

---

## PARTE 1 — MATRIZ DE ESTADO REAL POR MÓDULO

Leyenda: `[OK]` implementado y operativo · `[PARCIAL]` funciona pero con deuda · `[SIM]` simulado o stub · `[NO]` no implementado

---

### ETL

| Módulo / Componente | Estado | Deuda concreta |
|---|:---:|---|
| `bronce/cargador.py` — layouts, firmas, enrutamiento | `[OK]` | concentra demasiada lógica; falta partición por dominio |
| `bronce/cargador.py` — falso positivo ruta incompatible | `[OK]` | corregido en baseline |
| `pipeline.py` — modos completo / facts | `[OK]` | pesado como orquestador; candidato a delegación |
| `pipeline.py` — dependencias declaradas por fact | `[OK]` | — |
| `ContextoTransaccionalETL` | `[OK]` | — |
| `utils/sql_lotes.py` | `[OK]` | — |
| `utils/fechas.py` — campaña desde SQL | `[OK]` | — |
| `config/parametros.py` | `[OK]` | — |
| `dq/cuarentena.py` | `[OK]` | — |
| `mdm/lookup.py` — `resolver_geografia` | `[PARCIAL]` | reglas de submódulo 9.x/11.x no cerradas con negocio |
| `mdm/homologador.py` | `[PARCIAL]` | simulación en portal; real en ETL |
| `Dim_Personal` — completitud | `[PARCIAL]` | fallback "Sin Nombre"; sin enriquecimiento real aún |
| **Facts ya en patrón transaccional completo** | | |
| `fact_evaluacion_pesos.py` | `[OK]` | — |
| `fact_tasa_crecimiento_brotes.py` | `[OK]` | — |
| `fact_telemetria_clima.py` | `[OK]` | — |
| `fact_conteo_fenologico.py` | `[OK]` | — |
| `fact_evaluacion_vegetativa.py` | `[OK]` | — |
| `fact_cosecha_sap.py` | `[OK]` | — |
| `fact_maduracion.py` | `[OK]` | rechazos inline (no usa helper común) |
| `fact_fisiologia.py` | `[OK]` | — |
| `fact_induccion_floral.py` | `[OK]` | — |
| `fact_tareo.py` | `[PARCIAL]` | rechazos inline; deuda de calidad de origen |
| `fact_peladas.py` | `[PARCIAL]` | `_registrar_rechazo` local; `homologar_columna` pasa `engine` no `conexion`; **bug: pierde motivo geográfico detallado** |
| `fact_sanidad_activo.py` | `[PARCIAL]` | `homologar_columna` pasa `engine` no `conexion` |
| `fact_ciclo_poda.py` | `[PARCIAL]` | `homologar_columna` pasa `engine` no `conexion` |
| `gold/marts.py` | `[OK]` | bloqueo por Gold correcto; encoding Windows corregido |
| Tests ETL — atomicidad (6 facts) | `[OK]` | faltan para las 7 facts pendientes |
| Tests ETL — suite completa en Windows | `[PARCIAL]` | fricción por temporales en pytest |

---

### Backend

| Módulo / Componente | Estado | Deuda concreta |
|---|:---:|---|
| FastAPI modular + middleware + logging | `[OK]` | — |
| `nucleo/settings.py` + configuración central | `[OK]` | — |
| Autenticación JWT | `[OK]` | — |
| Roles: admin / operador_etl / analista_mdm / viewer | `[OK]` | portal reconoce rol `editor` que backend no define |
| `Control.Corrida` — corridas durables | `[OK]` | — |
| `Control.Corrida_Evento` — eventos persistidos | `[OK]` | — |
| `Control.Corrida_Paso` — pasos persistidos | `[OK]` | — |
| `Control.Bloqueo_Ejecucion` — lock concurrencia | `[OK]` | — |
| `Control.Comando_Ejecucion` — cola de comandos | `[OK]` | — |
| `runner/runner.py` + `runner/ejecutor.py` | `[OK]` | — |
| Cancelación / timeout | `[OK]` | — |
| `repositorios/repo_control.py` | `[PARCIAL]` | muy grande; necesita partición por dominio antes de escalar |
| Rutas ETL / cuarentena / catálogos / auth / health | `[OK]` | — |
| Homologación vía API | `[SIM]` | endpoint existe pero flujo real no cerrado desde portal |
| Configuración editable vía API | `[SIM]` | no hay endpoint de escritura real para `Config.Parametros_Pipeline` |
| Suite de pruebas backend | `[OK]` | 100 passed, 1 warning de cache |

---

### Portal Streamlit

| Módulo / Componente | Estado | Deuda concreta |
|---|:---:|---|
| Login / JWT desde backend | `[OK]` | — |
| `utils/api_client.py` — cliente HTTP reutilizable | `[OK]` | — |
| Corridas ETL: consulta y disparo via API | `[OK]` | — |
| Cuarentena via API | `[OK]` | — |
| Catálogos — parte via API | `[PARCIAL]` | mezcla API + SQL directo |
| Homologación | `[SIM]` | stub visual; no conectado a flujo real |
| Configuración de parámetros | `[SIM]` | muestra valores; no persiste nada real |
| `utils/componentes.py` — `health_status_panel()` | `[PARCIAL]` | **bug confirmado**: llama `badge_html(bg_color=..., text_color=...)` pero firma solo acepta `(texto, tipo)` → `TypeError` |
| `utils/auth.py` — roles locales | `[PARCIAL]` | define rol `editor` que no existe en backend; autorización local paralela al JWT |
| `utils/db.py` — SQL directo | `[PARCIAL]` | importa `obtener_engine` del ETL via `sys.path`; acceso SQL directo activo |
| `paginas/configuracion/pruebas_bd.py` | `[PARCIAL]` | consola SQL expuesta; rompe separación de capas |
| Tests portal | `[NO]` | sin cobertura automática de lógica compartida |

---

### SQL / DWH

| Módulo / Componente | Estado | Deuda concreta |
|---|:---:|---|
| DDL base — esquemas Bronce/Silver/Gold/MDM/Config | `[OK]` | — |
| `seed_dimensiones.sql` | `[OK]` | — |
| Tablas `Control.*` — control-plane completo | `[OK]` | — |
| Scripts de endurecimiento (fases 17-23) | `[OK]` | — |
| Retención y monitoreo operativo | `[OK]` | — |
| Naming de base: `ACP_Geographic_Phenology` vs `ACP_DataWarehose_Proyecciones` | `[PARCIAL]` | drift entre scripts históricos y entorno operativo real |
| Baseline de despliegue reproducible único | `[NO]` | no existe script maestro consolidado del estado actual |
| Separación scripts históricos vs scripts vivos | `[NO]` | todo coexiste en la misma carpeta sin clasificación |
| Gobierno formal de versiones DDL | `[NO]` | scripts sueltos por fase; sin consolidación oficial |

---

## PARTE 2 — PLAN DE CIERRE ARQUITECTÓNICO

### Criterio de priorización

El orden no es por facilidad sino por **riesgo y deuda de bloqueo**:
- primero lo que rompe trazabilidad o coherencia hoy
- luego lo que impide cerrar la capa web correctamente
- luego lo que consolida el contrato del dato
- luego lo que habilita el siguiente salto de plataforma

---

## SPRINT 1 — Bugs activos y coherencia de contratos
**Objetivo:** eliminar los problemas que ya están causando daño operativo real.

### 1.1 Corregir bug `fact_peladas` — motivo geográfico detallado

**Archivo:** `silver/facts/fact_peladas.py`

El problema no es solo el mensaje. Es que `_motivo_rechazo_geografia()` local ignora el campo `detalle` que devuelve `resolver_geografia()`. Ya con la refactorización en curso (usar `_motivo_cuarentena_geografia` del helper + `_registrar_rechazo` del helper), esto queda corregido.

Completar también:
- pasar `conexion` en vez de `engine` a `homologar_columna`
- test de atomicidad para `fact_peladas`

### 1.2 Corregir bug `badge_html` / `health_status_panel`

**Archivo:** `acp_mdm_portal/utils/componentes.py` línea 346-349

```python
# Actual (rompe):
badge_html("Servidor API Conectado", bg_color="#ECFDF3", text_color="#027A48")

# Correcto:
badge_html("Servidor API Conectado", tipo="success")
```

Verificar que `badge_html` tenga tipo `"success"` y `"error"` en su tabla de estilos o agregar esos tipos.

### 1.3 Terminar migración transaccional de facts pendientes

Completar los 4 gaps restantes identificados:
- `fact_tareo.py` — rechazos inline → `_registrar_rechazo` del helper
- `fact_sanidad_activo.py` — `homologar_columna` con `conexion`
- `fact_ciclo_poda.py` — `homologar_columna` con `conexion`
- `fact_maduracion.py` — rechazos inline → `_registrar_rechazo` del helper

Agregar tests de atomicidad para cada una.

### 1.4 Unificar roles portal ↔ backend

**Archivo:** `acp_mdm_portal/utils/auth.py`

- eliminar rol `editor` de la matriz local
- roles válidos: `admin`, `operador_etl`, `analista_mdm`, `viewer`
- decisión de autorización debe venir del JWT del backend, no de la matriz local

**Entregable sprint 1:** facts todas en patrón transaccional · bug badge corregido · roles alineados

---

## SPRINT 2 — Desacoplar portal del SQL directo
**Objetivo:** el portal ya no puede tocar SQL directamente ni el engine del ETL.

### 2.1 Encapsular o eliminar `pruebas_bd.py`

Si se necesita diagnóstico de conexión, debe hacerse via endpoint `/health` del backend, no abriendo el engine ETL.  
Opciones: mover a herramienta de admin separada o eliminar de la navegación del portal.

### 2.2 Refactorizar `utils/db.py`

- eliminar `sys.path.append(_ETL_DIR)`
- eliminar importación de `obtener_engine` del ETL
- la única conexión de datos del portal debe ser el backend API
- si se necesita verificar conectividad SQL, exponer ese endpoint en el backend y consumirlo

### 2.3 Migrar catálogos restantes a API

Los catálogos que aún hacen SQL directo (geografía, personal, variedades) deben pasar a consumir `GET /catalogos/...` del backend.

### 2.4 Decidir formalmente el futuro de Streamlit

No es una decisión técnica menor. Las opciones son:
- **A)** Streamlit como herramienta operativa interna de largo plazo (endurecerla más)
- **B)** Streamlit como transición corta hasta Next.js (no invertir más en UI; solo conectar lo que falta)

La decisión cambia el scope de los siguientes sprints.

**Entregable sprint 2:** portal sin SQL directo · portal sin sys.path al ETL · catálogos via API

---

## SPRINT 3 — Convertir flujos simulados a API real
**Objetivo:** lo que el portal muestra debe ser lo que realmente persiste.

### 3.1 Homologación real

Hoy `paginas/homologacion.py` es visual. Debe:
- consumir `POST /mdm/homologacion` del backend
- el backend persiste en `MDM.Diccionario_Homologacion`
- resultado visible en la misma pantalla sin reload completo

### 3.2 Configuración de parámetros editable

Hoy muestra valores de `Config.Parametros_Pipeline` pero no permite editar.  
El backend debe exponer:
- `GET /config/parametros` — lectura
- `PATCH /config/parametros/{clave}` — escritura con rol `admin`

El portal consume esos endpoints.

### 3.3 Reglas de validación persistibles

Si existen reglas editables, deben vivir en SQL y editarse via API, no en código.

**Entregable sprint 3:** homologación real operativa · parámetros editables desde portal · nada simulado visible al operador

---

## SPRINT 4 — Consolidar SQL / DWH operativo
**Objetivo:** el contrato del dato debe ser reproducible y no ambiguo.

### 4.1 Declarar base canónica oficial

Decidir formalmente y documentar en `CLAUDE.md` o `README_OPERATIVO_PIPELINE.md`:
- nombre oficial de base: `ACP_DataWarehose_Proyecciones` (o el correcto)
- todos los scripts nuevos usan ese nombre
- los scripts históricos quedan marcados como `legacy/`

### 4.2 Script maestro de despliegue consolidado

Crear `sql/baseline_deploy_YYYYMMDD.sql` que:
- crea todos los esquemas y tablas en orden correcto
- incluye seeds de dimensiones mínimas
- es ejecutable en entorno limpio sin necesidad de correr 23 fases en orden

### 4.3 Separar scripts históricos

```
sql/
  baseline/       ← estado actual reproducible
  fases/          ← historial de evolución (solo referencia)
  parches/        ← correcciones puntuales aplicadas
```

### 4.4 Cerrar reglas de geografía con negocio

Este es el punto más crítico de negocio pendiente:
- documentar formalmente la semántica de módulo / submodulo / turno / válvula / cama
- acordar con el equipo el tratamiento de tokens `9.1`, `9.2`, `11.1`, `11.2`
- codificar esa decisión en `mdm/lookup.py` y en `Config.Parametros_Pipeline`

**Entregable sprint 4:** baseline SQL reproducible · nombre canónico declarado · regla geográfica cerrada

---

## SPRINT 5 — Performance ETL y calidad de dominio
**Objetivo:** el ETL no solo carga correctamente, carga eficientemente.

### 5.1 Migrar facts con patrón fila a fila a lotes

Facts que aún insertan sin `ejecutar_en_lotes`:
- `fact_peladas.py` — insert unitario por fila
- `fact_tareo.py` — insert unitario por fila
- `fact_sanidad_activo.py` — insert unitario por fila
- `fact_ciclo_poda.py` — insert unitario por fila

Patrón a seguir: `fact_maduracion.py` → acumula `payload_inserts` → `ejecutar_en_lotes`.

### 5.2 Enriquecer `Dim_Personal`

- definir fuente adicional (SAP / RRHH) para nombre y datos completos
- implementar carga incremental de personal con upsert

### 5.3 Revisar `fact_tareo` con equipo de negocio

El rechazo por geografía o actividad no resuelta en tareo es en parte deuda de origen.  
Necesita alineación con negocio sobre qué hacer con registros sin match.

### 5.4 Modularizar `bronce/cargador.py` y `pipeline.py`

- `cargador.py` puede partir en: validación de layout, enrutamiento, persistencia
- `pipeline.py` puede delegar la ejecución de facts a un `ejecutor_facts.py` separado

**Entregable sprint 5:** todas las facts con lotes · pipeline más modular · personal más completo

---

## SPRINT 6 — Siguiente salto de plataforma
**Objetivo:** cuando capas 1-5 estén cerradas, habilitar la siguiente generación.

### 6.1 Frontend Next.js + BFF

- diseñar contrato BFF sobre el backend existente
- el backend FastAPI actual puede mantenerse como BFF o servir como API interna
- Streamlit se retira de producción

### 6.2 Airflow productivo

- solo tiene sentido cuando el ETL ya no depende de estado frágil o reglas abiertas
- integrar después de sprint 4 (reglas de negocio cerradas)

### 6.3 Módulo predictivo ML

- requiere que el dato operacional sea confiable y estable
- depende del cierre de geografía y cama (sprint 4)

---

## RESUMEN DE ORDEN DE EJECUCIÓN

```
Sprint 1  →  bugs activos + facts al patrón transaccional + roles alineados
Sprint 2  →  portal sin SQL directo
Sprint 3  →  flujos simulados → API real
Sprint 4  →  SQL consolidado + regla geográfica cerrada con negocio
Sprint 5  →  performance ETL + calidad de dominio
Sprint 6  →  Next.js + Airflow + ML (cuando lo anterior esté cerrado)
```

---

## DEUDA QUE NO ENTRA EN NINGÚN SPRINT SI NO SE DECIDE

Estas tres cosas requieren decisión de negocio o de arquitectura, no solo trabajo técnico:

1. **Semántica definitiva de geografía** — sin esta decisión, cualquier mejora al lookup es provisional
2. **Futuro de Streamlit** — determina si sprint 3 merece inversión o si es un parche
3. **Nombre canónico de base de datos** — determina si el drift SQL se puede cerrar sin riesgo de romper lo que ya corre

Sin estas tres decisiones tomadas formalmente, los sprints 3, 4 y 6 quedan en limbo.

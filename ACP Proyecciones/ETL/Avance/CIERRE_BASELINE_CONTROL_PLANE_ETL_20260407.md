# Cierre Baseline Control-Plane ETL 2026-04-07

## Estado

Baseline cerrado para `ETL + backend + Control.*` en `ACP_DataWarehose_Proyecciones`.

La plataforma queda validada para operacion diaria local con:

- ejecucion completa del pipeline
- `rerun` dirigido por fact
- control-plane persistente en SQL Server
- trazabilidad de pasos por corrida
- health checks operativos
- retencion de historial de `Control.*`

## Alcance cerrado

Queda dentro del baseline:

- ETL Python en [pipeline.py](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\pipeline.py)
- backend FastAPI en [backend](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\backend)
- runner separado en [runner](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\backend\runner)
- esquema `Control.*`
- scripts operativos [fase21_endurecimiento_control_plane.sql](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\fase21_endurecimiento_control_plane.sql) y [fase22_retencion_control_plane.sql](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\fase22_retencion_control_plane.sql)

Queda fuera del baseline:

- frontend eliminado
- Streamlit como frente operativo final
- reglas de negocio pendientes de MDM/geografia especial
- definiciones funcionales futuras por dominio

## Cambios estructurales cerrados

### 1. Backend y control-plane

- `POST /api/v1/etl/corridas` mantiene contrato vigente.
- `GET /api/v1/etl/corridas/{id}` ya devuelve parametros ETL decodificados y `pasos`.
- `GET /api/v1/etl/corridas/{id}/pasos` expone la traza persistida.
- `GET /api/v1/etl/facts` publica el catalogo oficial de facts soportadas por `rerun`.
- El runner ya no registra un solo paso generico; detecta hitos reales del pipeline y los persiste en `Control.Corrida_Paso`.
- Endpoints de salud operativos:
  - `/health/live`
  - `/health/ready`
  - `/health/ready/control`
  - `/health/ready/runner`
  - `/health/lock`

### 2. ETL

- Validacion temporal por dominio ya implementada.
- Telemetria uniforme por fact ya implementada.
- `rerun` dirigido por fact ya operativo.
- DQ repetitivo con cuarentena idempotente en `MDM.Cuarentena`.
- Catalogo de estrategia de reproceso centralizado en [utils/ejecucion.py](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\utils\ejecucion.py).
- Refactor de claridad y batching aplicado en:
  - [fact_conteo_fenologico.py](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\silver\facts\fact_conteo_fenologico.py)
  - [fact_fisiologia.py](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\silver\facts\fact_fisiologia.py)
  - [fact_tasa_crecimiento_brotes.py](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\silver\facts\fact_tasa_crecimiento_brotes.py)

### 3. SQL operativo

- Vistas operativas creadas y validadas:
  - `Control.vw_Corridas_Activas`
  - `Control.vw_Cola_Comandos`
  - `Control.vw_Ultima_Corrida_Por_Tabla`
- Procedimiento de retencion creado y validado:
  - `Control.sp_Purgar_Historial_Control`

## Evidencia de validacion

### Pruebas

- Backend: `92 passed`
- ETL local: verde
- AST de archivos criticos tocados: `OK`

### Validacion SQL

- [fase21_endurecimiento_control_plane.sql](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\fase21_endurecimiento_control_plane.sql) aplicado correctamente
- [fase22_retencion_control_plane.sql](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\fase22_retencion_control_plane.sql) aplicado correctamente
- `OBJECT_ID('Control.sp_Purgar_Historial_Control')` validado
- ejecucion de purge validada sin error

### Corridas ETL validadas

#### Corrida funcional amplia

- `Bronce archivos`: `12`
- `Bronce filas`: `491174`
- `Gold` refrescado sin bloqueo
- `Duracion total`: `419.86s`

#### Corrida de no regresion posterior a optimizacion

- `Bronce archivos`: `3`
- `Bronce filas`: `332941`
- `Fact_Fisiologia`: `45555 leidos`, `43900 insertados`, `1655 rechazados`
- `Fact_Tasa_Crecimiento_Brotes`: `268474 leidos`, `263388 insertados`, `5086 rechazados`
- `Duracion total`: `296.96s`

## Decision operativa

Se considera cerrado el baseline tecnico para operacion local controlada.

Interpretacion:

- no hay bloqueantes estructurales en ETL
- no hay bloqueantes estructurales en backend
- el control-plane ya tiene persistencia, salud, vistas y retencion
- los rechazos residuales actuales corresponden a backlog funcional de fuente/MDM, no a fallo de plataforma

## Backlog residual no bloqueante

Permanece abierto, pero no bloquea cierre de baseline:

- geografia especial en `Fact_Tasa_Crecimiento_Brotes`
- variedades no reconocidas en dominios dependientes de MDM
- residual de `Fact_Fisiologia`
- definicion funcional futura de reglas por dominio
- clarificacion futura del uso de `Bronce.Ciclos_Fenologicos`

## Criterio de reapertura

Solo reabrir este baseline si ocurre alguno de estos casos:

- una corrida completa vuelve a bloquear `Gold` por error estructural del ETL
- falla el runner o la persistencia de `Control.*`
- el `rerun` dirigido deja de respetar el manifiesto oficial
- el backend deja de exponer consistentemente `corrida`, `pasos`, `facts` o `lock`

## Referencias

- [README_OPERATIVO_PIPELINE.md](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\ETL\README_OPERATIVO_PIPELINE.md)
- [README.backend.md](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\backend\README.backend.md)
- [RUNBOOK_CONTROL_PLANE_ETL.md](D:\Proyecto2026\ACP_DWH\ACP%20Proyecciones\backend\RUNBOOK_CONTROL_PLANE_ETL.md)

# Baseline de Avance Consolidado

Fecha de corte: 2026-04-15

Proyecto: ACP DWH Proyecciones
Ruta ETL: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL`
Ruta portal: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\acp_mdm_portal`

## 1. Estado general real al corte

- `pytest tests -q` llegó a estar verde en la línea base operativa de atomicidad.
- `py pipeline.py` llegó a correr en verde con datos reales en la línea base validada.
- Gold volvió a publicar correctamente cuando no había errores bloqueantes.
- Se corrigieron bugs reales probados contra SQL Server, no solo sobre mocks.
- Se avanzó fuerte en refactor transaccional, pero no quedó cerrado al 100% en todos los facts.

## 2. Refactor transaccional ya hecho

### Helper central nuevo

- `utils/contexto_transaccional.py`

### Helpers adaptados a engine o connection compartida

- `utils/sql_lotes.py`
- `dq/cuarentena.py`
- `mdm/homologador.py`

### Facts ya migradas al patrón de atomicidad compartida

- `silver/facts/fact_evaluacion_pesos.py`
- `silver/facts/fact_tasa_crecimiento_brotes.py`
- `silver/facts/fact_telemetria_clima.py`
- `silver/facts/fact_conteo_fenologico.py`
- `silver/facts/fact_evaluacion_vegetativa.py`
- `silver/facts/fact_cosecha_sap.py`

## 3. Facts aún pendientes contra ese mismo estándar

- `silver/facts/fact_maduracion.py`
- `silver/facts/fact_peladas.py`
- `silver/facts/fact_tareo.py`
- `silver/facts/fact_fisiologia.py`
- `silver/facts/fact_induccion_floral.py`
- `silver/facts/fact_sanidad_activo.py`
- `silver/facts/fact_ciclo_poda.py`

## 4. Fuera de ese refactor

- `silver/facts/fact_proyecciones.py`
  - carga manual
  - no sigue el mismo flujo automático de ETL

## 5. Bugs reales corregidos y validados

### 5.1 Fact_Evaluacion_Vegetativa

Archivo:
- `silver/facts/fact_evaluacion_vegetativa.py`

Problemas corregidos:
- fila inválida por `Plantas_en_Floracion` se rechazaba pero seguía entrando a `payload_inserts`
- llamada vieja a `contexto.marcar_estado_carga(engine, ...)` seguía viva después del refactor

Fix aplicado:
- `continue` quedó en el nivel correcto
- fila inválida ya no entra al insert
- `marcar_estado_carga` quedó usando la firma nueva sin `engine`

Resultado real validado:
- corrida aislada del fact
- `23096 insertados`
- `321 rechazados`
- sin excepción

### 5.2 Auditoría de cargas

Archivo:
- `auditoria/log.py`

Problema corregido:
- el código no coincidía con el esquema real desplegado de `Auditoria.Log_Carga`

Fix aplicado:
- detección dinámica de columnas reales
- compatibilidad con:
  - `Estado` o `Estado_Proceso`
  - `Nombre_Archivo` o `Nombre_Archivo_Fuente`
- `obtener_ultimo_estado()` devuelve alias uniforme `Estado`

Resultado:
- dashboard y lectura de auditoría dejaron de romper por nombre de columna

### 5.3 Gold en consola Windows

Archivo:
- `gold/marts.py`

Problema corregido:
- emojis en logs rompían `charmap codec can't encode`

Fix:
- logs pasados a ASCII
- uso de `[OK]` y `[BLOCK]`

Resultado:
- Gold ejecutable desde consola Windows sin error por encoding

### 5.4 Fact_Maduracion

Archivo:
- `silver/facts/fact_maduracion.py`
- soporte DDL: `fase23_bronce_maduracion_estado_carga.sql`

Problemas corregidos:
- `Bronce.Maduracion` no tenía `Estado_Carga`
- el fact seguía leyendo como si existiera esa columna

Fix:
- se ajustó el soporte operativo para que `Bronce.Maduracion` tenga `Estado_Carga`

Resultado:
- `Bronce.Maduracion` quedó con `26800` filas en `PROCESADO`
- pipeline volvió a pasar ese punto

### 5.5 Cargador Bronce y falso positivo por ruta incompatible

Archivo:
- `bronce/cargador.py`

Problema corregido:
- validación global de ruta marcaba archivos correctos como incompatibles cuando la ruta actual no tenía firma propia
- caso real reportado en `conteo_fruta`

Fix:
- si la ruta actual no tiene firma declarada, la validación global se omite

Resultado:
- se eliminó el falso positivo `RUTA_CONTENIDO_INCOMPATIBLE` para ese caso

## 6. Geografía y lookup

Archivo principal:
- `mdm/lookup.py`

Avance:
- se consolidó `resolver_geografia(...)` como API pública principal
- `obtener_id_geografia(...)` quedó como wrapper temporal
- fallback legacy mejorado para cargar también `SubModulo`
- ahora entiende tokens tipo `9.1`, `9.2`, `11.1`, `11.2`

Estado:
- mejor que antes
- todavía no resuelve todo mientras no se cierre la regla exacta de geografía con el equipo

## 7. Rechazo, cuarentena y trazabilidad fina

Archivo base:
- `silver/facts/_helpers_fact_comunes.py`

Avance:
- se centralizó registro de rechazos
- se normalizó salida de resumen de facts
- quedó contrato común:
  - `leidos`
  - `insertados`
  - `rechazados`
  - `cuarentena`
  - `motivos_principales`

Facts tocadas para este estándar:
- `fact_conteo_fenologico.py`
- `fact_cosecha_sap.py`
- `fact_fisiologia.py`
- `fact_ciclo_poda.py`
- `fact_sanidad_activo.py`
- `fact_maduracion.py`
- `fact_induccion_floral.py`
- `fact_tasa_crecimiento_brotes.py`
- `fact_evaluacion_pesos.py`
- `fact_evaluacion_vegetativa.py`
- `fact_peladas.py`
- `fact_tareo.py`
- `fact_telemetria_clima.py`

Resultado:
- ya no queda el patrón viejo de responder resúmenes incompletos en esos módulos
- el motivo del rechazo persiste mejor que antes

## 8. Fact_Peladas

Estado real observado:
- el bronce ya carga
- hubo corrida real con `173 leidos | 157 insertados | 16 rechazados`

Hallazgo de negocio:
- los rechazos principales eran por geografía no encontrada
- submódulos `9.1`, `9.2`, `11.1`, `11.2` sí deben ser tratados como reglas válidas

Decisión operativa tomada:
- duplicado debe ir a cuarentena, no descartarse en silencio

Pendiente:
- cierre exacto de reglas geográficas con el equipo antes de seguir endureciendo lógica

## 9. Campaña, fechas y hardcodes operativos

Archivo:
- `utils/fechas.py`

Antes:
- rango de campaña fijo en código

Ahora:
- lee `CAMPANA_FECHA_INICIO` y `CAMPANA_FECHA_FIN` desde `Config.Parametros_Pipeline`
- conserva fallback seguro si la configuración no responde

Resultado:
- una sola fuente de verdad para campaña base

## 10. Parámetros operativos pasados a configuración

Archivos:
- `config/parametros.py`
- `utils/ejecucion.py`
- `pipeline.py`
- `seed_dimensiones.sql`

Movido a config:
- `CAMA_MIN_PERMITIDA`
- `CAMA_MAX_PERMITIDA`
- `MAX_CAMAS_POR_GEOGRAFIA`
- `SP_CAMA_MODO_APLICAR`
- `TABLAS_BRONCE_SP_CAMA`
- `FACTS_BLOQUEANTES_GOLD`
- `ESTADOS_BLOQUEANTES_CALIDAD_CAMA`
- `CAMPANA_FECHA_INICIO`
- `CAMPANA_FECHA_FIN`
- `ID_CONDICION_CULTIVO_DEFAULT`

Resultado:
- pipeline menos hardcodeado
- más gobernable desde SQL y portal futuro

## 11. Fact_Cosecha_SAP

Archivo:
- `silver/facts/fact_cosecha_sap.py`

Antes:
- `ID_Condicion_Cultivo` fijo en código

Ahora:
- toma `ID_CONDICION_CULTIVO_DEFAULT` desde configuración

Resultado:
- menos acoplamiento a un ID duro

## 12. DQ y alias operativos

Archivo:
- `dq/validador.py`

Fix:
- `sanidad_activo` ya entra por la misma validación de `Total_Plantas_Raw`

Resultado:
- alias operativo quedó alineado con el dominio actual

## 13. Portal MDM

Ruta:
- `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\acp_mdm_portal`

Hallazgos reales:
- arquitectura híbrida
  - parte consume backend API
  - parte sigue consultando SQL directo
- `homologacion.py` todavía está más cerca de simulación que de flujo final real
- `pruebas_bd.py` sigue rompiendo separación de capas
- `utils/db.py` toca internals del pool

### BOM corregido

Archivos corregidos:
- `acp_mdm_portal/utils/auth.py`
- `acp_mdm_portal/paginas/catalogos/geografia.py`
- `acp_mdm_portal/paginas/catalogos/personal.py`
- `acp_mdm_portal/paginas/catalogos/variedades.py`

Fix:
- regrabados en `UTF-8` sin BOM

Resultado:
- parser OK en esos cuatro archivos

## 14. Tests agregados o ajustados

Atomicidad y lookup:
- `tests/test_utils_contexto_transaccional.py`
- `tests/test_fact_evaluacion_pesos_atomicidad.py`
- `tests/test_fact_tasa_crecimiento_brotes_atomicidad.py`
- `tests/test_fact_telemetria_clima_atomicidad.py`
- `tests/test_fact_conteo_fenologico_atomicidad.py`
- `tests/test_fact_evaluacion_vegetativa_atomicidad.py`
- `tests/test_fact_cosecha_sap_atomicidad.py`
- `tests/test_mdm_lookup.py`

Config y ejecución:
- `tests/test_config_parametros.py`
- `tests/test_utils_ejecucion.py`
- `tests/test_pipeline.py`

Bronce:
- `tests/test_bronce_enrutamiento.py`

DQ:
- `tests/test_dq_validador.py`

Fechas:
- `tests/test_utils_fechas.py`

## 15. Corridas reales relevantes

### Corrida completa validada previamente

- `py pipeline.py` con `exit code 0`
- Gold:
  - `Gold.Mart_Fenologia: 20808`
  - `Gold.Mart_Clima: 1520`
  - `Gold.Mart_Pesos_Calibres: 5629`

### Corrida posterior observada

- hubo corrida donde varios facts marcaron `0`
- interpretación validada:
  - ya no quedaban registros `CARGADO`
  - comportamiento normal post reproceso correcto

### Corrida con hechos reales observados

- `Fact_Conteo_Fenologico`
  - `60394 leidos`
  - `60328 insertados`
  - `66 rechazados`
- `Fact_Maduracion`
  - `26800 leidos`
  - `25270 insertados`
  - luego se identificó problema de duplicados y estado
- `Fact_Telemetria_Clima`
  - `85894 leidos`
  - `85885 insertados`
  - `9 rechazados`
  - `265 cuarentena`
- `Fact_Evaluacion_Pesos`
  - `5672 leidos`
  - `5656 insertados`
  - `16 rechazados`
- `Fact_Fisiologia`
  - `45555 leidos`
  - `43900 insertados`
  - `1655 rechazados`
- `Fact_Evaluacion_Vegetativa`
  - `23417 leidos`
  - `23096 insertados`
  - `321 rechazados`
- `Fact_Tasa_Crecimiento_Brotes`
  - `268474 leidos`
  - `263388 insertados`
  - `5086 rechazados`

## 16. Pendientes reales de negocio y técnica

### Pendientes de negocio

- cerrar regla exacta de geografía
- definir estrategia final de campaña
  - no asumir campaña global ligera
  - decidir si la campaña nace por fecha de poda o por otro evento operativo

### Pendientes de ETL

- terminar de migrar facts faltantes al patrón transaccional completo
- revisar `fact_peladas` con reglas definitivas de geografía
- revisar por qué no llena `Bridge_Geografia_Cama` en escenarios reales de negocio
- seguir eliminando hardcodes remanentes
- cerrar estrategia de homologación real del portal, no solo simulada

### Pendientes de portal

- unificar a backend-only
- eliminar dependencia SQL directa donde ya no corresponde
- conectar homologación a endpoints reales
- decidir si `pruebas_bd.py` vive o sale

## 17. Conclusión operativa

El proyecto no está en cero ni en análisis teórico. Ya hubo estabilización real del ETL, ya hubo pipeline verde, ya hubo Gold publicando, ya se corrigieron bugs reales contra SQL Server y ya se avanzó fuerte en atomicidad, geografía y trazabilidad de rechazos.

Lo que falta ya no es “armar el ETL desde cero”. Lo que falta es cerrar reglas operativas finas:

- geografía exacta
- campaña exacta
- bridge de cama
- portal 100% alineado al backend
- facts restantes al mismo estándar técnico

Ese es el estado real al 2026-04-15.

# Baseline Operativo Formal del ETL ACP

> Nota de vigencia:
> Este baseline conserva el corte inicial de Fase 1 como referencia historica.
> El baseline final validado con la corrida real del `2026-04-06` quedo documentado en [BASELINE_OPERATIVO_ETL_20260406_FINAL.md](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/BASELINE_OPERATIVO_ETL_20260406_FINAL.md).
> El cierre operativo detallado de los dominios activos quedo documentado en [CIERRE_OPERATIVO_ACTIVOS_FASE21_20260406.md](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/CIERRE_OPERATIVO_ACTIVOS_FASE21_20260406.md).

Corte de referencia: `2026-04-06`
Fecha de generación: `2026-04-06 09:40:21`

## Contexto validado
- Servidor SQL: `LCP-PAG-PRACTIC`
- Base SQL: `ACP_DataWarehose_Proyecciones`
- Usuario SQL efectivo: `CERROPRIETO\chernandez`
- Fuente oficial: instancia SQL real actual; los `.md` se usaron como contraste.

## Resumen ejecutivo
- Dictámenes emitidos: `{'CERRADO': 2, 'PENDIENTE FUNCIONAL': 1, 'ESTABLE CON RESIDUAL CONTROLADO': 7, 'PENDIENTE DE DECISIÓN DE NEGOCIO/MDM': 1, 'BLOQUEADO POR FUENTE': 1}`
- Dominios listos para uso analítico directo/feature-ready: `Dim_Geografia, Fact_Ciclo_Poda, Fact_Fisiologia`
- Dominios no listos para ML: `Dim_Personal, Fact_Tasa_Crecimiento_Brotes, Fact_Tareo`
- Pendiente global de homologación: `5`
- Parche temporal aprobado (pendiente de ejecuciÃ³n y validaciÃ³n): `Fundo=Arandano Acp`, `Sector=Sin_Sector_Mapa` para gaps de geografÃ­a en Tasa. Ver `PARCHE_TEMPORAL_GEO_TASA_20260406.md`.

| Dominio | Filas Silver | Dictamen | Residual dominante | Readiness ML |
| --- | ---: | --- | --- | --- |
| Dim_Geografia | 1,031 | CERRADO | Bridge=3866, duplicados_vigentes=0, sp_estado=OK_OPERATIVO | LISTO |
| Dim_Personal | 2 | PENDIENTE FUNCIONAL | filas_reales=1, sin_nombre=1 | NO_LISTO |
| Fact_Conteo_Fenologico | 12,520 | ESTABLE CON RESIDUAL CONTROLADO | ID_Personal=-1 100.0% | CONDICIONAL |
| Fact_Evaluacion_Pesos | 5,656 | ESTABLE CON RESIDUAL CONTROLADO | No se pudo calcular peso promedio de baya (3) | CONDICIONAL |
| Fact_Evaluacion_Vegetativa | 22,872 | ESTABLE CON RESIDUAL CONTROLADO | Geografia no encontrada en Silver.Dim_Geografia. (455) | CONDICIONAL |
| Fact_Ciclo_Poda | 5,205 | CERRADO | Sin residual dominante documentado | LISTO |
| Fact_Maduracion | 25,270 | ESTABLE CON RESIDUAL CONTROLADO | ID_Personal=-1 100.0% | CONDICIONAL |
| Fact_Telemetria_Clima | 214,725 | ESTABLE CON RESIDUAL CONTROLADO | Humedad nula (1512) | CONDICIONAL |
| Fact_Induccion_Floral | 1,424 | ESTABLE CON RESIDUAL CONTROLADO | Geografia no encontrada en Silver.Dim_Geografia. (18) | CONDICIONAL |
| Fact_Tasa_Crecimiento_Brotes | 378,222 | PENDIENTE DE DECISIÓN DE NEGOCIO/MDM | Geografia no encontrada en Silver.Dim_Geografia. (140416) | NO_LISTO |
| Fact_Tareo | 0 | BLOQUEADO POR FUENTE | Bronce=0, Silver=0 | NO_LISTO |
| Fact_Fisiologia | 43,900 | ESTABLE CON RESIDUAL CONTROLADO | Sin residual dominante documentado | LISTO |

## Dominio crítico transversal
- Residual detectado de `Modulo_Raw = '9.'`: `1,663`
- `Bronce.Fisiologia`: `1,655`
- `Bronce.Evaluacion_Pesos`: `8`

## Cruce documental vs SQL
- `VALIDADA EN SQL`: Servidor/base operativos siguen siendo LCP-PAG-PRACTIC / ACP_DataWarehose_Proyecciones. Evidencia: LCP-PAG-PRACTIC / ACP_DataWarehose_Proyecciones
- `VALIDADA EN SQL`: sp_Validar_Calidad_Camas debe quedar en OK_OPERATIVO. Evidencia: {'Cama_Fuera_Regla': 0, 'Geografias_Saturadas': 0, 'Estado_Calidad_Cama': 'OK_OPERATIVO'}
- `INCONSISTENTE ENTRE DOC Y REALIDAD`: Fact_Telemetria_Clima quedó cerrado operativamente y sin duplicados canónicos. Evidencia: Duplicados canónicos: 42869 grupos; cuarentena pendiente: 1562
- `VALIDADA CON RESIDUAL CONTROLADO`: Inducción floral se mantiene funcional desde Silver. Evidencia: Filas Silver: 1424; cuarentena pendiente: 24; duplicados: 24 grupos
- `ABIERTA POR FUENTE/NEGOCIO`: Tasa de crecimiento se mantiene funcional sin residual crítico nuevo. Evidencia: Filas Silver: 378222; cuarentena pendiente: 160956; bronce rechazado: 158726
- `VALIDADA EN SQL`: Dim_Personal sigue en estado conservador y pendiente de validación fuerte. Evidencia: Filas reales: 1; sin nombre: 1
- `VALIDADA EN SQL`: Fact_Tareo sigue diagnosticado y pendiente hasta contar con fuente suficiente. Evidencia: Silver: 0 filas; Bronce: 0 filas
- `VALIDADA CON RESIDUAL CONTROLADO`: El residual vigente de módulo 9. sigue abierto y no debe inferirse automáticamente. Evidencia: Total residual detectado: 1663

## Detalle por dominio

### Dim_Geografia
- Dictamen: `CERRADO`
- Razon: Calidad de camas OK, bridge persistido y sin duplicados vigentes.
- Tabla Silver: `Silver.Dim_Geografia`
- Vigentes: `1,031`
- Bridge geografia-cama: `3,866`
- Test block vigente: `1`
- Sin cama explicita: `1,031`
- Duplicados vigentes: `0`
- SP calidad camas: `{'Cama_Fuera_Regla': 0, 'Geografias_Saturadas': 0, 'Estado_Calidad_Cama': 'OK_OPERATIVO'}`

### Dim_Personal
- Dictamen: `PENDIENTE FUNCIONAL`
- Razon: La dimensión existe, pero sigue siendo demasiado débil para soportar cierre analítico.
- Tabla Silver: `Silver.Dim_Personal`
- Total filas: `2`
- Filas reales: `1`
- Surrogate -1: `1`
- Sin nombre: `1`
- Sin sexo: `2`
- Sin planilla: `1`

### Fact_Conteo_Fenologico
- Dictamen: `ESTABLE CON RESIDUAL CONTROLADO`
- Razon: Estructuralmente sano, pero con dependencia total de un Dim_Personal aún conservador.
- Tabla Silver: `Silver.Fact_Conteo_Fenologico`
- Filas Silver: `12,520`
- Rango evento: `2026-03-02 00:00:00` -> `2026-03-25 00:00:00`
- ID_Personal = -1: `12,520` (100.00%)
- Huérfanas FK: `0`
- Cuarentena pendiente: `0`
- Bronce total: `1,565`
- Bronce rechazado: `0`
- Último log: `OK` | fin=`2026-04-01 11:50:34.446911` | insertadas=`0` | rechazadas=`0`

### Fact_Evaluacion_Pesos
- Dictamen: `ESTABLE CON RESIDUAL CONTROLADO`
- Razon: Estructuralmente sano, pero con dependencia total de un Dim_Personal aún conservador.
- Tabla Silver: `Silver.Fact_Evaluacion_Pesos`
- Filas Silver: `5,656`
- Rango evento: `2025-05-02 00:00:00` -> `2026-03-09 00:00:00`
- ID_Personal = -1: `5,656` (100.00%)
- Huérfanas FK: `0`
- Cuarentena pendiente: `16`
- Bronce total: `5,672`
- Bronce rechazado: `0`
- Último log: `OK` | fin=`2026-04-01 11:50:39.478401` | insertadas=`0` | rechazadas=`0`
- Residual dominante:
  - `Bronce.Evaluacion_Pesos`: No se pudo calcular peso promedio de baya (3)

### Fact_Evaluacion_Vegetativa
- Dictamen: `ESTABLE CON RESIDUAL CONTROLADO`
- Razon: Estructuralmente sano, pero con dependencia total de un Dim_Personal aún conservador.
- Tabla Silver: `Silver.Fact_Evaluacion_Vegetativa`
- Filas Silver: `22,872`
- Rango evento: `2025-03-04` -> `2026-03-13`
- ID_Personal = -1: `22,872` (100.00%)
- Huérfanas FK: `0`
- Cuarentena pendiente: `545`
- Bronce total: `23,417`
- Bronce rechazado: `0`
- Último log: `OK` | fin=`2026-04-01 11:51:19.831816` | insertadas=`0` | rechazadas=`0`
- Residual dominante:
  - `Bronce.Evaluacion_Vegetativa`: Geografia no encontrada en Silver.Dim_Geografia. (455)

### Fact_Ciclo_Poda
- Dictamen: `CERRADO`
- Razon: Dominio sin huérfanas y sin residual dominante abierto.
- Tabla Silver: `Silver.Fact_Ciclo_Poda`
- Filas Silver: `5,205`
- Rango evento: `2026-01-05 00:00:00` -> `2026-03-16 00:00:00`
- Huérfanas FK: `0`
- Cuarentena pendiente: `0`
- Bronce total: `24,117`
- Bronce rechazado: `0`
- Último log: `OK` | fin=`2026-04-01 11:51:20.110127` | insertadas=`0` | rechazadas=`0`

### Fact_Maduracion
- Dictamen: `ESTABLE CON RESIDUAL CONTROLADO`
- Razon: Estructuralmente sano, pero con dependencia total de un Dim_Personal aún conservador.
- Tabla Silver: `Silver.Fact_Maduracion`
- Filas Silver: `25,270`
- Rango evento: `2026-01-02 00:00:00` -> `2026-03-27 00:00:00`
- ID_Personal = -1: `25,270` (100.00%)
- Huérfanas FK: `0`
- Cuarentena pendiente: `0`
- Bronce total: `26,800`
- Bronce rechazado: `0`
- Último log: `OK` | fin=`2026-04-01 11:50:38.583802` | insertadas=`0` | rechazadas=`0`

### Fact_Telemetria_Clima
- Dictamen: `ESTABLE CON RESIDUAL CONTROLADO`
- Razon: Carga viva y sin huérfanas, pero el criterio canónico de no duplicados ya no coincide con el cierre documental.
- Tabla Silver: `Silver.Fact_Telemetria_Clima`
- Filas Silver: `214,725`
- Rango evento: `2022-01-01 01:00:00` -> `2026-03-29 23:30:00`
- Huérfanas FK: `0`
- Cuarentena pendiente: `1,562`
- Bronce total: `214,735`
- Bronce rechazado: `10`
- Duplicados canónicos: `42869` grupos / `85738` filas extra
- Superposición total clima: `42945` grupos / `171780` filas extra
- Último log: `OK` | fin=`2026-04-01 11:50:39.452047` | insertadas=`0` | rechazadas=`0`
- Residual dominante:
  - `Bronce.Clima`: Humedad nula (1,512)

### Fact_Induccion_Floral
- Dictamen: `ESTABLE CON RESIDUAL CONTROLADO`
- Razon: Estructuralmente sano, pero con dependencia total de un Dim_Personal aún conservador.
- Tabla Silver: `Silver.Fact_Induccion_Floral`
- Filas Silver: `1,424`
- Rango evento: `2026-02-04` -> `2026-03-31`
- ID_Personal = -1: `1,424` (100.00%)
- Huérfanas FK: `0`
- Cuarentena pendiente: `24`
- Bronce total: `1,448`
- Bronce rechazado: `24`
- Duplicados canónicos: `24` grupos / `30` filas extra
- Último log: `OK` | fin=`2026-04-01 11:51:19.864884` | insertadas=`0` | rechazadas=`0`
- Residual dominante:
  - `Bronce.Induccion_Floral`: Geografia no encontrada en Silver.Dim_Geografia. (18)

### Fact_Tasa_Crecimiento_Brotes
- Dictamen: `PENDIENTE DE DECISIÓN DE NEGOCIO/MDM`
- Razon: La geografía y MDM siguen explicando un residual masivo.
- Tabla Silver: `Silver.Fact_Tasa_Crecimiento_Brotes`
- Filas Silver: `378,222`
- Rango evento: `2024-02-12` -> `2025-11-17`
- ID_Personal = -1: `378,222` (100.00%)
- Huérfanas FK: `0`
- Cuarentena pendiente: `160,956`
- Bronce total: `536,948`
- Bronce rechazado: `158,726`
- Duplicados canónicos: `120379` grupos / `257843` filas extra
- Último log: `OK` | fin=`2026-04-01 11:51:19.923750` | insertadas=`0` | rechazadas=`0`
- Residual dominante:
  - `Bronce.Tasa_Crecimiento_Brotes`: Geografia no encontrada en Silver.Dim_Geografia. (140,416)

### Fact_Tareo
- Dictamen: `BLOQUEADO POR FUENTE`
- Razon: No hay fuente vigente cargada en Bronce para validar o cerrar el dominio.
- Tabla Silver: `Silver.Fact_Tareo`
- Filas Silver: `0`
- Huérfanas FK: `0`
- Cuarentena pendiente: `0`
- Bronce total: `0`
- Bronce rechazado: `0`
- Último log: `OK` | fin=`2026-04-01 11:50:39.498883` | insertadas=`0` | rechazadas=`0`

### Fact_Fisiologia
- Dictamen: `ESTABLE CON RESIDUAL CONTROLADO`
- Razon: Dominio operativo con residual abierto de módulo 9.
- Tabla Silver: `Silver.Fact_Fisiologia`
- Filas Silver: `43,900`
- Rango evento: `2025-05-02 00:00:00` -> `2026-01-16 00:00:00`
- Huérfanas FK: `0`
- Cuarentena pendiente: `0`
- Bronce total: `45,555`
- Bronce rechazado: `0`
- Último log: `OK` | fin=`2026-04-01 11:51:19.761523` | insertadas=`43,900` | rechazadas=`1,655`

## Pendientes reales para la siguiente fase
- Deuda de código: consolidar `lookup.py`, separar SP oficial de fallback legacy y volver testeable el resolvedor geográfico.
- Deuda de datos/fuente: cerrar la política de recarga, bajar residual geográfico de `Tasa_Crecimiento_Brotes` y recuperar fuente fuerte para `Dim_Personal` / `Fact_Tareo`.
- Deuda de negocio/MDM: formalizar la regla final de `Modulo_Raw = '9.'` y revisar la masa pendiente que sigue entrando a cuarentena por geografía.
- Deuda de readiness ML: no promover a dataset formal los dominios con `ID_Personal = -1` masivo ni los dominios con duplicidad/ambigüedad no resuelta.

## Criterio de no regresión
- No cambiar la fuente oficial del baseline sin registrar nueva fecha de corte.
- No declarar cerrado un dominio solo porque el MD lo diga; prevalece la evidencia SQL.
- No mover a Gold ni a features de ML dominios con `PENDIENTE FUNCIONAL`, `BLOQUEADO POR FUENTE` o `PENDIENTE DE DECISIÓN DE NEGOCIO/MDM`.
- Reejecutar este baseline después de cualquier cambio en geografía, campaña, recarga o Dim_Personal.

## MD contrastados
- `README_OPERATIVO_PIPELINE.md`
- `Avance/CIERRE_ESTABLE_ETL_20260330.md`
- `Avance/ACTUALIZACION_OPERATIVA_CLIMA_20260331.md`
- `Avance/ACTUALIZACION_OPERATIVA_INDUCCION_TASA_20260401.md`
- `Avance/CHECKLIST_OPERATIVO_5_MINUTOS.md`

# Cierre Operativo de Dominios Activos - Fase 21

Fecha de corte: `2026-04-06`
Servidor SQL: `LCP-PAG-PRACTIC`
Base SQL: `ACP_DataWarehose_Proyecciones`

> Nota de vigencia:
> Este documento conserva el cierre tecnico de Fase 21 y queda complementado por la corrida real validada el mismo dia.
> El baseline final oficial quedo en [BASELINE_OPERATIVO_ETL_20260406_FINAL.md](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/BASELINE_OPERATIVO_ETL_20260406_FINAL.md).

## Alcance
- `Fact_Conteo_Fenologico`
- `Fact_Tasa_Crecimiento_Brotes`
- Refresh de `Gold` posterior a la recarga limpia de dominios activos

## Cambios aplicados con evidencia real

### 1. Cierre deterministico de Test Block en Tasa
- Se aplico alias activo en `MDM.Regla_Modulo_Raw`:
  - `Modulo_Raw = 'Test Block'`
  - `Tipo_Conduccion = 'TEST_BLOCK'`
  - `Es_Test_Block = 1`
  - `Es_Activa = 1`
- Se ajusto [fact_tasa_crecimiento_brotes.py](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/silver/facts/fact_tasa_crecimiento_brotes.py) para preservar el token original de `Modulo_Raw` cuando el caso es test block y no enviar `None` al resolvedor.
- Validacion obligatoria ejecutada en SQL:
  - `EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='Test Block', @Turno_Raw='1', @Valvula_Raw='1', @Cama_Raw='1'`
  - Resultado real:
    - `ID_Geografia = 1025`
    - `Estado_Resolucion = 'RESUELTA_TEST_BLOCK'`
    - `Detalle = 'Test block resuelto por Turno/Valvula.'`

## Resultado operativo antes vs despues

### Tasa antes del ajuste Fase 21
- `258427 insertados`
- `10047 rechazados`
- `10370 cuarentena`

### Tasa despues del ajuste Fase 21
- `263388 insertados`
- `5086 rechazados`
- `5086 cuarentena`

### Delta real
- `+4961` filas insertadas a `Silver.Fact_Tasa_Crecimiento_Brotes`
- `-4961` filas rechazadas en Bronce
- `-5284` eventos de cuarentena

Interpretacion:
- El frente `Test Block` dejo de ser residual geografico dominante.
- Las `323` filas que siguen apareciendo con `Modulo_Raw = 'Test Block'` ya no fallan por geografia; fallan por variedad (`COLUSSUS`).

## Corrida final limpia de dominios activos

### Conteo
- Reproceso limpio ejecutado
- Resultado final:
  - `12520 insertados`
  - `0 rechazados`
  - `0 cuarentena`
- Estado Bronce final:
  - `PROCESADO = 1565`

### Tasa
- Reproceso limpio ejecutado
- Resultado final:
  - `263388 insertados`
  - `5086 rechazados`
  - `5086 cuarentena`
- Estado Bronce final:
  - `PROCESADO = 263388`
  - `RECHAZADO = 5086`

### Gold
- Refresh ejecutado despues de la recarga final
- Resultado:
  - `Gold.Mart_Cosecha = 0`
  - `Gold.Mart_Proyecciones = 0`
  - `Gold.Mart_Fenologia = 3256`
  - `Gold.Mart_Clima = 0`
  - `Gold.Mart_Pesos_Calibres = 0`
  - `Gold.Mart_Administrativo = 0`

## Validacion final con corrida real de 3 archivos

### Resumen real validado
- `Bronce archivos = 3`
- `Bronce filas = 294993`
- `Dim_Geografia vigentes = 1129`
- `Dim_Geografia operativos = 1128`
- `Dim_Geografia test_block = 1`
- `Dim_Geografia duplicados = 0`
- `SP_Cama estado = OMITIDO_SIN_TABLAS_CON_CAMA_EN_ESTA_CORRIDA`
- `SP_Cama estado calidad = OK_OPERATIVO`
- `Fact_Conteo_Fenologico = 60328`
- `Fact_Tasa_Crecimiento_Brotes = 263388`
- `Gold.Mart_Fenologia = 20808`

### Conteo en corrida real
- `60328 insertados`
- `66 rechazados`
- `66 cuarentena`
- Validacion SQL:
  - `Bronce.Conteo_Fruta -> PROCESADO = 9106`
  - `Bronce.Conteo_Fruta -> RECHAZADO = 66`
  - `MDM.Cuarentena -> Geografia no encontrada en Dim_Geografia = 66`

### Tasa en corrida real
- `263388 insertados`
- `5086 rechazados`
- `5086 cuarentena`
- Validacion SQL:
  - `Bronce.Tasa_Crecimiento_Brotes -> PROCESADO = 263388`
  - `Bronce.Tasa_Crecimiento_Brotes -> RECHAZADO = 5086`
  - `MDM.Cuarentena -> Geografia especial requiere catalogacion o regla en MDM_Geografia. = 3871`
  - `MDM.Cuarentena -> Variedad no reconocida - requiere revision en MDM = 1115`
  - `MDM.Cuarentena -> Fecha de poda auxiliar posterior a la fecha de evaluacion = 100`

## Residual final de Tasa

### Por motivo
- `Geografia especial requiere catalogacion o regla en MDM_Geografia.`: `3871`
- `Variedad no reconocida - requiere revision en MDM`: `1115`
- `Fecha de poda auxiliar posterior a la fecha de evaluacion`: `100`

### Por Modulo_Raw
- `VIVERO`: `3871`
- `11.1`: `792`
- `Test Block`: `323`
- `9.1`: `100`

### Cruce Motivo x Modulo_Raw
- `VIVERO` -> `Geografia especial requiere catalogacion o regla en MDM_Geografia.`: `3871`
- `11.1` -> `Variedad no reconocida - requiere revision en MDM`: `792`
- `Test Block` -> `Variedad no reconocida - requiere revision en MDM`: `323`
- `9.1` -> `Fecha de poda auxiliar posterior a la fecha de evaluacion`: `100`

## Dictamen operativo por frente
- `Fact_Conteo_Fenologico = ESTABLE CON RESIDUAL CONTROLADO`
- `Fact_Tasa_Crecimiento_Brotes = ESTABLE CON RESIDUAL CONTROLADO`
- `Test Block geografia = CERRADO`
- `VIVERO = PENDIENTE DE DECISION DE NEGOCIO/MDM`
- `Frente cama = ABIERTO, NO BLOQUEANTE PARA ESTA FASE`

## Pendientes reales despues de Fase 21

### Negocio / MDM
- Definir tratamiento formal de `VIVERO` sin forzarlo a geografia temporal generica.
- Resolver backlog de variedades:
  - `FCM15-005 (2022)` -> `396`
  - `FCM15-005 (2023)` -> `396`
  - `COLUSSUS` -> `323`

### Data Quality
- Mantener rechazados los `100` casos con `Fecha_Poda_Aux` posterior a la fecha de evaluacion.
- No crear autocorreccion para esa regla en esta fase.

### Geografia especial
- El residual geografico remanente ya no es `Test Block`; ahora es `VIVERO`.
- El listado operativo de combinaciones `Turno/Valvula/Cama` de `VIVERO` queda en [tasa_vivero_combos_20260406.csv](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/tasa_vivero_combos_20260406.csv).

### MDM variedades
- El entregable operativo para homologacion queda en [tasa_variedades_pendientes_20260406.csv](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/tasa_variedades_pendientes_20260406.csv).

## Criterio de cierre alcanzado
- `Conteo` quedo funcional y con residual pequeno totalmente trazado.
- `Tasa` quedo con residual acotado, explicado y separado por tipo de deuda.
- `Bronce` marca estados correctos para los dominios activos validados.
- `Dim_Geografia` no fue modificada fuera del frente Test Block ya validado.
- No se declaro cerrado el frente de cama porque no entro una corrida con tablas que alimentan `SP_Cama`.

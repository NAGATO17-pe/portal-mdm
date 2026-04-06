# Baseline Operativo Formal del ETL ACP

Fecha de corte oficial: `2026-04-06`
Fuente oficial: `SQL Server real actual`
Servidor validado: `LCP-PAG-PRACTIC`
Base validada: `ACP_DataWarehose_Proyecciones`
Usuario efectivo validado: `CERROPRIETO\\chernandez`

## Regla de lectura
- Este documento fija el baseline final del `2026-04-06`.
- La verdad operativa la define la evidencia SQL y la corrida real del dia.
- Los `.md` historicos se usan como contraste, no como fuente final si contradicen SQL.

## Alcance real validado hoy
- `Dim_Geografia`
- `Fact_Conteo_Fenologico`
- `Fact_Tasa_Crecimiento_Brotes`
- Refresh de `Gold` despues de la corrida real
- Validacion de estado de `SP_Cama` como frente no bloqueante

## Resumen ejecutivo
- El ETL ya no esta en falla sistemica.
- `Test Block` quedo cerrado a nivel de geografia para `Tasa`.
- `Conteo` y `Tasa` quedaron operativos, pero ambos con residual controlado.
- `VIVERO` ya no debe leerse como bug de pipeline; queda como pendiente de negocio/MDM.
- El frente de cama sigue abierto, pero no bloquea esta fase porque la corrida real no incluyo tablas Bronce que alimentan `SP_Cama`.

## Evidencia de la corrida real del 2026-04-06

### Resumen final de pipeline
- `Bronce archivos = 3`
- `Bronce filas = 294993`
- `Dim_Geografia vigentes = 1129`
- `Dim_Geografia operativos = 1128`
- `Dim_Geografia test_block = 1`
- `Dim_Geografia sin cama explicita = 1129`
- `Dim_Geografia duplicados = 0`
- `SP_Cama estado = OMITIDO_SIN_TABLAS_CON_CAMA_EN_ESTA_CORRIDA`
- `SP_Cama estado calidad = OK_OPERATIVO`
- `Fact_Conteo_Fenologico = 60328`
- `Fact_Tasa_Crecimiento_Brotes = 263388`
- `Gold.Mart_Fenologia = 20808`
- `Duracion total = 154.62s`

### Validacion SQL de Conteo
- `Bronce.Conteo_Fruta`
  - `PROCESADO = 9106`
  - `RECHAZADO = 66`
- `MDM.Cuarentena` para `Bronce.Conteo_Fruta`
  - `Geografia no encontrada en Dim_Geografia = 66`

### Validacion SQL de Tasa
- `Bronce.Tasa_Crecimiento_Brotes`
  - `PROCESADO = 263388`
  - `RECHAZADO = 5086`
- `MDM.Cuarentena` para `Bronce.Tasa_Crecimiento_Brotes`
  - `Geografia especial requiere catalogacion o regla en MDM_Geografia. = 3871`
  - `Variedad no reconocida - requiere revision en MDM = 1115`
  - `Fecha de poda auxiliar posterior a la fecha de evaluacion = 100`

## Dictamen formal por frente activo

| Dominio | Estado | Evidencia principal | Residual dominante |
| --- | --- | --- | --- |
| `Dim_Geografia` | `CERRADO` | `1129` vigentes, `0` duplicados, `1` test block vigente | `Sin residual bloqueante` |
| `Fact_Conteo_Fenologico` | `ESTABLE CON RESIDUAL CONTROLADO` | `60328` insertados, `66` rechazados, `66` cuarentena | `Geografia no encontrada en Dim_Geografia` |
| `Fact_Tasa_Crecimiento_Brotes` | `ESTABLE CON RESIDUAL CONTROLADO` | `263388` insertados, `5086` rechazados, `5086` cuarentena | `VIVERO`, variedades pendientes y DQ de fecha |
| `Test Block geografia` | `CERRADO` | `Silver.sp_Resolver_Geografia_Cama` ya resuelve `Test Block` a `ID_Geografia = 1025` | `Residual remanente ya no es geografia` |
| `VIVERO` | `PENDIENTE DE DECISION DE NEGOCIO/MDM` | `3871` filas siguen en geografia especial | `Pendiente de definicion funcional` |
| `Frente cama` | `ABIERTO, NO BLOQUEANTE` | `SP_Cama` omitido por diseno en esta corrida | `Sin evidencia nueva de tablas con cama` |

## Cambio validado hoy en Test Block
- Se agrego alias activo en `MDM.Regla_Modulo_Raw` para `Test Block`.
- Se ajusto [fact_tasa_crecimiento_brotes.py](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/silver/facts/fact_tasa_crecimiento_brotes.py) para preservar `Modulo_Raw` cuando el caso es `Test Block`.
- Validacion SQL ejecutada:
  - `EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='Test Block', @Turno_Raw='1', @Valvula_Raw='1', @Cama_Raw='1'`
  - Resultado real:
    - `ID_Geografia = 1025`
    - `Estado_Resolucion = RESUELTA_TEST_BLOCK`
    - `Detalle = Test block resuelto por Turno/Valvula.`

## Residual final de Tasa

### Por motivo
- `Geografia especial requiere catalogacion o regla en MDM_Geografia. = 3871`
- `Variedad no reconocida - requiere revision en MDM = 1115`
- `Fecha de poda auxiliar posterior a la fecha de evaluacion = 100`

### Por modulo crudo
- `VIVERO = 3871`
- `11.1 = 792`
- `Test Block = 323`
- `9.1 = 100`

### Lectura correcta del residual
- `VIVERO` concentra el residual geografico real pendiente.
- `11.1` ya no es geografia; ahora cae por variedad.
- `Test Block` ya no cae por geografia; ahora cae por variedad `COLUSSUS`.
- `9.1` ya no es geografia; cae por la regla DQ de `Fecha_Poda_Aux`.

## Residual final de Conteo
- `66` filas rechazadas.
- `66` filas en cuarentena.
- Motivo unico detectado:
  - `Geografia no encontrada en Dim_Geografia`

Lectura:
- El fix aplicado en [fact_conteo_fenologico.py](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/silver/facts/fact_conteo_fenologico.py) si quedo validado.
- El objetivo del fix era cerrar correctamente `Estado_Carga` en Bronce, no forzar `rechazados = 0`.
- Por tanto, `Conteo` no queda `CERRADO`; queda `ESTABLE CON RESIDUAL CONTROLADO`.

## Contraste documental contra realidad SQL
- `README_OPERATIVO_PIPELINE.md`
  - `VALIDADA EN SQL`: servidor y base operativa siguen siendo `LCP-PAG-PRACTIC / ACP_DataWarehose_Proyecciones`
- `CIERRE_ESTABLE_ETL_20260330.md`
  - `VALIDADA CON AJUSTE`: el ETL esta estable, pero el cierre final de `Tasa` depende de separar residual tecnico vs negocio/MDM
- `ACTUALIZACION_OPERATIVA_INDUCCION_TASA_20260401.md`
  - `ACTUALIZADA POR EVIDENCIA NUEVA`: `Tasa` queda funcional con residual controlado, no cerrada
- `CHECKLIST_OPERATIVO_5_MINUTOS.md`
  - `VALIDADA CON CONTEXTO`: `SP_Cama` puede quedar omitido sin bloquear cuando la corrida no trae tablas con cama

## Estado carry-forward para dominios no revalidados hoy
- `Fact_Tareo = BLOQUEADO POR FUENTE`
- `Dim_Personal = PENDIENTE FUNCIONAL`
- Los demas dominios conservan el dictamen del baseline preliminar del mismo dia hasta contar con nueva corrida especifica.

## Recomendacion formal posterior al baseline

### Prioridad 1. Negocio / MDM
- Definir tratamiento formal de `VIVERO` sin mapearlo a geografia temporal generica.
- Resolver homologacion de variedades pendientes:
  - `FCM15-005 (2022)`
  - `FCM15-005 (2023)`
  - `COLUSSUS`

### Prioridad 2. Datos
- Revisar las `66` filas de `Conteo` que no encuentran geografia.
- No tocar codigo adicional en `Tasa` hasta cerrar `VIVERO` y variedades.

### Prioridad 3. Calidad de datos
- Mantener los `100` casos de `Fecha_Poda_Aux` posterior a evaluacion como rechazo DQ explicito.
- No autocorregir esa regla en esta fase.

### Prioridad 4. Operacion
- No declarar cerrado el frente de cama hasta correr una carga real que incluya `Bronce.Evaluacion_Pesos` o `Bronce.Evaluacion_Vegetativa`.

## Entregables asociados del 2026-04-06
- [CIERRE_OPERATIVO_ACTIVOS_FASE21_20260406.md](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/CIERRE_OPERATIVO_ACTIVOS_FASE21_20260406.md)
- [tasa_vivero_combos_20260406.csv](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/tasa_vivero_combos_20260406.csv)
- [tasa_variedades_pendientes_20260406.csv](D:/Proyecto2026/ACP_DWH/ACP%20Proyecciones/ETL/Avance/tasa_variedades_pendientes_20260406.csv)

## Dictamen final de Fase 1
- `Fact_Conteo_Fenologico = ESTABLE CON RESIDUAL CONTROLADO`
- `Fact_Tasa_Crecimiento_Brotes = ESTABLE CON RESIDUAL CONTROLADO`
- `Test Block geografia = CERRADO`
- `VIVERO = PENDIENTE DE DECISION DE NEGOCIO/MDM`
- `Frente cama = ABIERTO, NO BLOQUEANTE`

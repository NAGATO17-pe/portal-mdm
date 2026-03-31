# INFORME BASELINE FASE 16 - addendum operativo 2026-03-30

## 1) Resumen ejecutivo
El baseline historico de Fase 16 sigue siendo util, pero el corte operativo vigente debe leerse con el addendum siguiente.

Resultado principal:
- La regla `VI => Test Block` sigue estable.
- La calidad de cama sigue en `OK_OPERATIVO`.
- El bridge de camas ya persiste correctamente.
- Los bloqueos de Bronce ya separan archivos mal ubicados del problema real del ETL.
- El foco activo ya incluye `Fact_Maduracion`.

## 2) Corrida limpia validada de referencia
### Carga
- `Fact_Conteo_Fenologico`: `67860` insertados, `67` rechazados.
- `Fact_Evaluacion_Pesos`: `5658` insertados, `14` rechazados.
- `Fact_Evaluacion_Vegetativa`: `22872` insertados, `545` rechazados.
- `Fact_Ciclo_Poda`: `5205` insertados, `0` rechazados.

### Calidad y resolucion
- Smoke `VI` (cama `0/1/2`): `RESUELTA_TEST_BLOCK` en los 3 casos.
- `Silver.sp_Validar_Calidad_Camas`: `OK_OPERATIVO`.
- `SP_Cama aptas`: `3866`
- `SP_Cama insert bridge`: `3866`
- `Bridge camas despues`: `3866`

### Check estructural de cuarentena
- `Bronce.Evaluacion_Pesos`: cobertura alta de `ID_Registro_Origen`.
- `Bronce.Evaluacion_Vegetativa`: cobertura alta de `ID_Registro_Origen`.

## 3) Criterios de aceptacion (estado)
1. `Fact_Evaluacion_Pesos` cargo > 0: `CUMPLIDO`.
2. `Fact_Evaluacion_Vegetativa` cargo > 0: `CUMPLIDO`.
3. `Fact_Conteo_Fenologico` cargo > 0: `CUMPLIDO`.
4. `Fact_Ciclo_Poda` cargo > 0: `CUMPLIDO`.
5. `VI` resuelve Test Block en `0/1/2`: `CUMPLIDO`.
6. Calidad cama = `OK_OPERATIVO`: `CUMPLIDO`.
7. Si hubo aptas, bridge > 0: `CUMPLIDO`.

## 4) Residual actual (foco real)
### Conteo_Fruta (67 total)
- Geografia no encontrada en Dim_Geografia: `66`.
- Cinta no reconocida o no disponible: `1`.

### Pesos (14 total)
- Geografia especial requiere catalogacion o regla en MDM_Geografia: `8`.
- No se pudo calcular peso promedio de baya: `3`.
- Peso fuera de rango biologico: `3`.

### Vegetativa (545 total)
- Geografia no encontrada en Silver.Dim_Geografia: `455`.
- Plantas en floracion invalida o mayor al total evaluado: `75`.
- Cantidad de plantas evaluadas invalida: `13`.
- Fecha invalida o fuera de campana: `2`.

## 5) Cierres estructurales confirmados
1. `VI` ya no es problema.
2. El bridge de camas ya no queda vacio cuando hay aptas.
3. Los archivos mal ubicados ya no contaminan Bronce.
4. `Fact_Ciclo_Poda` quedo estable con geografia `Modulo + Turno + Valvula`.
5. `Fact_Conteo_Fenologico` quedo estable con parseo de `Valores_Raw`.
6. La geografia ya se normaliza globalmente antes del resolvedor.
7. Las variedades ahora tienen homologacion tipografica segura previa a MDM.

## 6) Nuevo frente operativo
`Fact_Maduracion` es el gap funcional prioritario de la capa Silver:
1. `Bronce.Maduracion` ya tiene lote real.
2. El payload viene en `Valores_Raw`.
3. El modelo aprobado es fila por organo observado con `ID_Cinta`, `ID_Organo` e `ID_Estado_Fenologico`.
4. La validacion final de corrida limpia para este fact sigue pendiente.

## 7) Riesgos y recomendaciones
1. No inferir `9.` hacia `9.1/9.2`.
2. Priorizar backlog geografico de Vegetativa (`455`) por impacto.
3. No auto-fusionar variedades ambiguas; solo equivalencias tipograficas seguras.
4. Mantener corrida diaria con snapshot y validacion de bridge.
5. Si entra lote de `Maduracion`, revisar inmediatamente inserts o cuarentena accionable.

## 8) Estado de congelado
- Baseline historico Fase 16 del `2026-03-25`: `CONGELADO`.
- Addendum operativo vigente al `2026-03-30`: `ACTIVO`.

## Addendum 2026-03-30 - Clima, Tareo y Regla de Campana

### Clima
- Se habilito carga especial en Bronce para clima usando la hoja `BD` del Excel analitico.
- La lectura valida usa `header=2` (fila real 3 del archivo) y mapea explicitamente:
  - `Fecha -> Fecha_Raw`
  - `Hora -> Hora_Raw`
  - `T Max -> TempMax_Raw`
  - `T Min -> TempMin_Raw`
  - `HUMEDAD RELATIVA -> Humedad_Raw`
  - `RADIACION SOLAR -> Radiacion_Raw`
  - `DVP Real -> VPD_Raw`
- `Sector_Raw` se deriva desde el nombre del archivo, por ejemplo `F07`.
- `Fact_Telemetria_Clima` deja de depender de `Dim_Geografia` y usa `Sector_Climatico` directo.
- `Gold.Mart_Clima` agrega por `ID_Tiempo + Sector_Climatico`.
- Script asociado: `fase19_ajuste_fact_clima_sector_climatico.sql`.

### Evidencia operativa validada hoy
- Corrida clima validada:
  - `Bronce filas`: `42947`
  - `Fact_Telemetria_Clima`: `15569` insertados
  - `Fact_Telemetria_Clima`: `27378` rechazados
  - `Gold.Mart_Clima`: `373` filas
- El residual de clima ya no es estructural; se concentra solo en `Fecha invalida o fuera de campana`.
- Las filas rechazadas corresponden a historico meteorologico de `2022`, no a error de parseo del Excel.

### Hallazgo tecnico critico
- La validacion de campana en `utils/fechas.py` sigue globalizada con rango fijo `2025-03-01` a `2026-06-30`.
- Esa regla hoy afecta a todas las facts que llaman `procesar_fecha()`.
- Conclusion aprobada:
  - la validacion de campana no debe seguir siendo global;
  - debe separarse por fact o por dominio;
  - clima debe poder conservar historico aunque este fuera de la campana vigente.

### Tareo
- Se corrigieron aliases reales del layout de `Consolidado_Tareos`.
- Se separaron filas basura del Excel (`Personas`, `Horas`, `TOTAL`, etc.).
- El rechazo restante ya no es bug del parser: la fuente actual no trae `Fundo/Modulo` resolubles.
- `Fact_Tareo` queda diagnosticado y pendiente hasta contar con fuente suficiente o redefinir el modelo de geografia.

### Suite de pruebas
- Se dejo base automatica con `pytest` en `tests/` para estructura, integridad y calidad.
- Los tests ya contemplan `Sector_Climatico` en clima.
- La suite sirve como smoke tecnico del estado estable actual.


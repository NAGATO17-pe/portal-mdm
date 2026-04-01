# CONTEXTO TRANSFERIBLE PARA OTRO CHAT (ACP DWH)

Fecha de corte: 2026-03-30
Proyecto: DWH Geographic Phenology (ACP)
Ruta operativa ETL: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL`

## 1) Objetivo operativo actual
Sostener corrida limpia y trazable en:
1. `Silver.Fact_Evaluacion_Pesos`
2. `Silver.Fact_Evaluacion_Vegetativa`
3. `Silver.Fact_Conteo_Fenologico`
4. `Silver.Fact_Ciclo_Poda`

y cerrar la validacion funcional de:
5. `Silver.Fact_Maduracion`

sin romper:
- reglas MDM ya estabilizadas,
- calidad de cama,
- persistencia del bridge,
- trazabilidad en cuarentena.

## 2) Estado funcional confirmado
1. `VI` resuelve correctamente como `RESUELTA_TEST_BLOCK`.
2. `9.1/9.2`, `11.1/11.2`, `13.1/13.2` y `14.1/14.2` tienen regla canonica activa.
3. `9.` (sin submodulo) se mantiene en cuarentena por diseno.
4. `Silver.sp_Validar_Calidad_Camas` permanece en `OK_OPERATIVO`.
5. Nuevas cuarentenas de Pesos/Vegetativa registran `ID_Registro_Origen` con cobertura alta.
6. El pipeline bloquea archivos mal ubicados con `LAYOUT_INCOMPATIBLE` o `RUTA_CONTENIDO_INCOMPATIBLE`.
7. `SP_Cama` ya persiste bridge y solo corre si en la corrida ingresan tablas Bronce con cama (`Evaluacion_Pesos` o `Evaluacion_Vegetativa`).
8. Hay normalizacion global de geografia antes del resolvedor.
9. Hay homologacion tipografica segura de variedades antes de MDM.

## 3) Corrida limpia validada mas reciente
Resumen relevante:
1. `Fact_Conteo_Fenologico`: `67860` insertados, `67` rechazados.
2. `Fact_Evaluacion_Pesos`: `5658` insertados, `14` rechazados.
3. `Fact_Evaluacion_Vegetativa`: `22872` insertados, `545` rechazados.
4. `Fact_Ciclo_Poda`: `5205` insertados, `0` rechazados.
5. `SP_Cama aptas`: `3866`
6. `SP_Cama insert bridge`: `3866`
7. `Bridge camas despues`: `3866`
8. `Dim_Geografia vigentes`: `1031`
9. Calidad cama: `OK_OPERATIVO`.

Interpretacion:
- El pipeline ya no esta roto por layout mal ubicado.
- El bridge de camas ya persiste correctamente.
- El residual dominante actual es de catalogo/DQ, no de infraestructura ETL.

## 4) Residual actual (foco real)
### Conteo_Fruta
- `66` por `Geografia no encontrada en Dim_Geografia`.
- `1` por `Cinta no reconocida o no disponible`.

### Pesos
- `8` por `Geografia especial requiere catalogacion o regla`.
- `3` por `No se pudo calcular peso promedio de baya`.
- `3` por `Peso fuera de rango biologico`.

### Vegetativa
- `455` por `Geografia no encontrada en Silver.Dim_Geografia`.
- `75` por `Plantas en floracion invalida o mayor al total evaluado`.
- `13` por `Cantidad de plantas evaluadas invalida`.
- `2` por `Fecha invalida o fuera de campana`.

Conclusion tecnica:
- El residual fuerte sigue siendo cobertura de catalogo geografico operativo.
- Ya no es problema del parser de modulo ni del bridge.

## 5) Hallazgos criticos de diagnostico
1. No asumir `ID_Registro_Origen` en `Bronce.Evaluacion_Vegetativa`; para backlog historico usar `MDM.Cuarentena.Valor_Recibido`.
2. El normalizador geografico ya limpia valores tipo:
   - `MODULO 2 -> 2`
   - `TURNO 04 -> 4`
   - `NROVALVULA 15 -> 15`
3. La homologacion de variedades ahora resuelve diferencias tipograficas seguras:
   - `FCM15 – 005` vs `FCM15-005`
   - `FL 19-006` vs `FL19-006`
   - `MEGA CRISP` vs `Megacrisp`
   Los casos ambiguos no se auto-fusionan.

## 6) Frente nuevo: Maduracion
1. `Bronce.Maduracion` existe y el payload real viene dentro de `Valores_Raw`.
2. El diseno aprobado para `Fact_Maduracion` es fila por organo observado.
3. Columnas objetivo:
   - `ID_Personal`
   - `ID_Geografia`
   - `ID_Tiempo`
   - `ID_Variedad`
   - `ID_Estado_Fenologico`
   - `ID_Cinta`
   - `ID_Organo`
   - `Dias_Pasados_Del_Marcado`
   - `Fecha_Evento`
   - `Fecha_Sistema`
   - `Estado_DQ`
4. `ID_Organo` es identificador operativo degenerado; NO existe `Dim_Organo`.
5. `Fact_Conteo_Fenologico` ya no usa `ID_Cinta`; `ID_Cinta` queda concentrado en `Fact_Maduracion`.
6. El loader de `fact_maduracion.py` ya fue reescrito al payload real, pero la validacion final con corrida real sigue siendo tarea activa.

## 7) Scripts y piezas clave vigentes
En `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL`:
1. `fase14_patch_sp_resolver_vi.sql`
2. `fase14_seed_vi_test_block.sql`
3. `fase15_cierre_geografia_pesos_vegetativa.sql`
4. `fase15_reapertura_backlog_geografia.sql`
5. `fase15_reproceso_pesos_vegetativa.py`
6. `fase15_validacion_post_reproceso.sql`
7. `fase16_limpieza_hechos_hoy.sql`
8. `fase16_snapshot_baseline.sql`
9. `fase17_catalogar_geografia_9_2_turnos_10_11.sql`
10. `fase17_reglas_modulo_raw_11_13_14.sql`
11. `fase18_fact_maduracion_y_cinta.sql`

Nota:
- `fase18_fact_maduracion_y_cinta.sql` existe, pero el modelo final de `Fact_Maduracion` se cerro despues. Si se va a reaplicar SQL, validar primero el DDL objetivo real.

## 8) Flujo operativo recomendado
1. Ejecutar pipeline real con ruta absoluta.
2. Si Bronce bloquea por layout/ruta, corregir ubicacion de archivo; no forzar corrida.
3. Si entra `Evaluacion_Pesos` o `Evaluacion_Vegetativa`, validar `SP_Cama` y bridge.
4. Revisar top firmas no resueltas (`Geografia no encontrada`).
5. Catalogar faltantes validos (preview/apply).
6. Reproceso dirigido o pipeline completo.
7. Si hubo lote de `Maduracion`, validar:
   - inserts en `Fact_Maduracion`,
   - cuarentenas por `ID_Organo`, `Estado`, `Cinta` o geografia.

## 9) Criterios de aceptacion operativa
Se considera avance real solo si se cumplen simultaneamente:
1. `Fact_Evaluacion_Pesos > 0` y `Fact_Evaluacion_Vegetativa > 0` en corrida limpia.
2. `VI` smoke (`cama 0/1/2`) = `RESUELTA_TEST_BLOCK`.
3. `sp_Validar_Calidad_Camas = OK_OPERATIVO`.
4. Si `SP_Cama aptas > 0`, entonces `Bridge_Geografia_Cama > 0`.
5. `% con ID_Registro_Origen` en nuevas cuarentenas >= `98%`.
6. Tendencia descendente de `Geografia no encontrada` en Vegetativa.
7. Si hay lote real de Maduracion, `Fact_Maduracion > 0` o cuarentena explicita y util.

## 10) Reglas de no regresion
NO hacer:
1. No mapear `9.` automaticamente a `9.1` o `9.2`.
2. No alterar SP de `VI` si ya esta estable.
3. No usar scripts de limpieza masiva sin preview.
4. No auto-homologar variedades ambiguas por similitud debil.
5. No reintroducir `ID_Cinta` en `Fact_Conteo_Fenologico`.

SI hacer:
1. Mantener enfoque incremental por evidencia.
2. Priorizar catalogo geografico faltante sobre parches de parser.
3. Validar siempre con snapshot post-corrida.
4. Usar `Valor_Recibido` de cuarentena para backlog historico cuando no haya join directo.

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

## Addendum 2026-04-01 - Induccion Floral y Tasa de Crecimiento Brotes

### Estado funcional que debes asumir
1. `Bronce.Induccion_Floral` ya fue ampliada y mapeada con loader especial.
2. `Bronce.Tasa_Crecimiento_Brotes` ya fue ampliada y mapeada con loader especial.
3. `Tasa_Crecimiento_Brotes` procesa solo la hoja `BD_General`.
4. `Valores_Raw` ya no debe contener la estructura principal en ninguno de estos dos dominios.
5. Existen y cargan:
   - `Silver.Fact_Induccion_Floral`
   - `Silver.Fact_Tasa_Crecimiento_Brotes`

### Reglas de interpretacion
1. `ID_Personal = -1` es esperado si `Dim_Personal` sigue vacia o no resuelve DNIs.
2. Si ves duplicados en `Fact_Induccion_Floral`, primero revisar si el mismo archivo fue cargado dos veces en Bronce.
3. No asumir bug del fact sin validar:
   - duplicados por lote
   - `Nombre_Archivo`
   - `Fecha_Sistema`
4. No abrir Gold nuevo para estos dominios mientras el objetivo siga siendo presentacion.
5. Para modelado futuro, la fuente correcta es `Silver`, no `Gold`.

### Dictamen vigente
- `Fact_Induccion_Floral`: funcional
- `Fact_Tasa_Crecimiento_Brotes`: funcional
- `Bronce` de ambos dominios: estable
- residual actual: operativo/catalogo, no estructural

## Addendum 2026-04-01 - Transferencia Rapida de Fisiologia

- Baseline real validado: `43900` insertados / `1655` pendientes.
- Residual actual: solo `Modulo_Raw = '9.'`.
- Regla por turno de `Modulo 11`: probada, regresiva y desactivada.
- Regla final de `9.`: pendiente de cierre por negocio y MDM.
- Validacion aceptable para este frente: solo con rerun real y evidencia SQL comparativa.


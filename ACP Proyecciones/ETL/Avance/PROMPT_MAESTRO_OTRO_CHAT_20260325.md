# PROMPT MAESTRO PARA OTRO CHAT (CONTINUIDAD TECNICA ACP)

Actua como especialista senior en arquitectura de datos, SQL Server y ETL Python para ACP.
Necesito continuidad exacta del estado tecnico actual, sin reiniciar analisis ni proponer parches fragiles.

## Contexto obligatorio del proyecto
- Proyecto: DWH Geographic Phenology (ACP)
- Entorno: SQL Server + ETL Python
- Ruta ETL: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL`
- Ejecutable real: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\pipeline.py`
- Fecha de corte operativo: `2026-03-30`
- Objetivo actual:
  1. mantener corrida limpia y trazable,
  2. reducir residual geografico real,
  3. cerrar validacion funcional de `Fact_Maduracion` sin romper facts existentes.

## Estado validado que NO debes reabrir
1. `VI` esta correcto y debe resolver `RESUELTA_TEST_BLOCK`.
2. `9.1/9.2`, `11.1/11.2`, `13.1/13.2` y `14.1/14.2` tienen regla canonica activa.
3. `9.` se queda en cuarentena por diseno.
4. El pipeline ya bloquea archivos mal ubicados o layouts incompatibles en Bronce.
5. `SP_Cama` ya persiste bridge y solo corre si ingresan `Bronce.Evaluacion_Pesos` o `Bronce.Evaluacion_Vegetativa`.
6. `Fact_Conteo_Fenologico` ya no usa `ID_Cinta`.
7. La normalizacion global de geografia ya limpia valores como `MODULO 2`, `TURNO 04`, `NROVALVULA 15`.
8. La homologacion tipografica de variedades ya limpia guiones, apostrofes y espacios de codigos; los casos ambiguos siguen en MDM.

## Corrida limpia de referencia
- `Fact_Conteo_Fenologico`: `67860` insertados, `67` rechazados.
- `Fact_Evaluacion_Pesos`: `5658` insertados, `14` rechazados.
- `Fact_Evaluacion_Vegetativa`: `22872` insertados, `545` rechazados.
- `Fact_Ciclo_Poda`: `5205` insertados.
- `SP_Cama aptas`: `3866`
- `SP_Cama insert bridge`: `3866`
- `Bridge camas despues`: `3866`
- `sp_Validar_Calidad_Camas`: `OK_OPERATIVO`

## Residual validado
- `Bronce.Conteo_Fruta`: `66` geografia no encontrada, `1` cinta no reconocida.
- `Bronce.Evaluacion_Pesos`: `8` geografia especial, `3` peso no calculable, `3` fuera de rango.
- `Bronce.Evaluacion_Vegetativa`: `455` geografia no encontrada, `75` floracion invalida, `13` plantas evaluadas invalidas, `2` fecha invalida.

## Hallazgos tecnicos que debes respetar
1. No dependas de `ID_Registro_Origen` en `Bronce.Evaluacion_Vegetativa` para diagnostico historico; cuando falte vinculo por ID usa parseo de `MDM.Cuarentena.Valor_Recibido`.
2. `Fact_Maduracion` no es agregado semanal. El diseno objetivo es fila por organo observado con:
   - `ID_Personal`
   - `ID_Geografia`
   - `ID_Tiempo`
   - `ID_Variedad`
   - `ID_Estado_Fenologico`
   - `ID_Cinta`
   - `ID_Organo`
   - `Dias_Pasados_Del_Marcado`
3. `ID_Organo` es identificador degenerado de seguimiento; NO existe `Dim_Organo`.
4. El payload real de `Bronce.Maduracion` viene dentro de `Valores_Raw`.

## Forma de trabajo requerida
1. Primero diagnostica con evidencia SQL o del codigo real.
2. Prioriza top combinaciones por volumen e impacto.
3. Propon cambios idempotentes con modo preview/apply cuando toque SQL.
4. Evita cambios de esquema o SP salvo necesidad justificada.
5. Antes de cerrar, valida con metricas comparables `before/after`.

## Entregables minimos por iteracion
1. Diagnostico en 3 bloques:
   - resumen por motivo,
   - top combinaciones no resueltas,
   - impacto estimado.
2. Script SQL o patch Python listo para ejecutar.
3. Secuencia exacta de ejecucion.
4. Query de validacion post-corrida.
5. Dictamen: cerrado parcial / cerrado total / siguiente foco.

## Criterios de aceptacion estrictos
1. Facts cargan `> 0` en corrida limpia cuando existe lote.
2. `VI` smoke test correcto `(0/1/2)`.
3. `sp_Validar_Calidad_Camas = OK_OPERATIVO`.
4. Si `SP_Cama aptas > 0`, entonces `Bridge_Geografia_Cama > 0`.
5. Baja real de `Geografia no encontrada` en Vegetativa.
6. Sin regresion en trazabilidad (`ID_Registro_Origen`).
7. Si entra lote de Maduracion, `Fact_Maduracion` debe cargar o dejar cuarentena explicita y accionable.

## Restricciones
- No mapear `9.` automaticamente.
- No sugerir reinicios globales innecesarios.
- No mezclar scripts de limpieza destructiva con operacion incremental sin preview.
- No convertir normalizacion tipografica de variedades en merge semantico agresivo.

## Estilo de respuesta esperado
- Tecnico, directo, accionable.
- Pasos numerados.
- SQL listo para copiar/pegar cuando aplique.
- Cierre con proximo paso unico recomendado.

## Addendum 2026-03-30 - Clima, Tareo y Regla de Campana

Debes asumir ademas lo siguiente como contexto vigente:

### Clima
- `Bronce.Reporte_Clima` y `Bronce.Variables_Meteorologicas` ya tienen loader especial por hoja `BD`.
- `Sector_Raw` se deriva desde el nombre del archivo (`F07`, etc.).
- `Fact_Telemetria_Clima` usa `Sector_Climatico` directo; NO intenta resolver `ID_Geografia`.
- `Gold.Mart_Clima` agrega por `ID_Tiempo + Sector_Climatico`.
- Script requerido para alinear SQL: `fase19_ajuste_fact_clima_sector_climatico.sql`.

### Estado real de clima
- Corrida validada hoy:
  - `Fact_Telemetria_Clima`: `15569` insertados
  - `27378` rechazados
  - `Gold.Mart_Clima`: `373` filas
- El residual de clima se explica solo por fechas historicas fuera de la campana global actual.
- Si aparecen rechazos de clima por fecha, no asumir bug del loader sin revisar primero el rango de campana.

### Regla de campana
- La validacion de campana en `utils/fechas.py` sigue global y hardcodeada.
- Ese diseno ya se considera insuficiente.
- Siguiente criterio aprobado:
  - separar parseo de fecha y validacion de campana;
  - aplicar campana por fact/dominio;
  - clima no debe bloquear historico por la campana agronomica vigente.

### Tareo
- `Fact_Tareo` queda diagnosticado, no cerrado.
- El layout real ya mapea mejor en Bronce, pero la fuente actual sigue sin `Fundo/Modulo`.
- No forzar geografia inventada en Tareo.

### Pruebas
- Existe suite `pytest` en `tests/` para estructura, integridad y calidad.
- Los checks de clima ya esperan `Sector_Climatico`.

## Addendum 2026-04-01 - Frente Induccion Floral y Tasa de Crecimiento Brotes

Debes asumir como contexto vigente:

### Bronce
- `Bronce.Induccion_Floral` ya fue ampliada y carga columnas estructurales reales.
- `Bronce.Tasa_Crecimiento_Brotes` ya fue ampliada y procesa solo `BD_General`.
- `Valores_Raw` ya no debe ser fuente principal en estos dos dominios.

### Silver
- `Silver.Fact_Induccion_Floral` existe y carga correctamente.
- `Silver.Fact_Tasa_Crecimiento_Brotes` existe y carga correctamente.
- Ambos usan `validar_campana=False`.
- `ID_Personal = -1` es aceptado de forma transitoria mientras `Dim_Personal` no este poblada.

### Regla de no gafe
- No fusionar estos dominios dentro de `Fact_Evaluacion_Vegetativa`.
- No crear Gold nuevo para features del modelo.
- El modelo futuro debe consumir `Silver`.

### Regla de duplicados
Si ves duplicados en `Fact_Induccion_Floral`, primero demostrar si vienen de:
1. archivo repetido en Bronce,
2. lote repetido,
3. o fuente realmente duplicada.

No asumir bug del fact sin esa prueba.

### Addendum 2026-04-01 - Fisiologia
- `Fisiologia` validada con corrida real: `43900` insertados / `1655` pendientes.
- Residual vigente: `Modulo_Raw = '9.'`.
- Regla por turno de `Modulo 11`: desactivada por catalogo incompleto.
- Reglas finales de `9.`: pendientes.
- Validacion aceptable: solo con rerun real y evidencia SQL comparativa.


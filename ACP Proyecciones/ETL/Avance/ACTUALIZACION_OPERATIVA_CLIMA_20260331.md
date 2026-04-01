# ACTUALIZACION OPERATIVA CLIMA - 2026-03-31

## 1) Objetivo del trabajo
Cerrar el frente tecnico-operativo de `Fact_Telemetria_Clima` sobre la instancia real, eliminando reproceso artificial, recuperando granularidad horaria y controlando duplicados logicos conflictivos sin contaminar Silver.

---

## 2) Hallazgos raiz identificados

### 2.1 Parseo y campana estaban acoplados
El parser de fechas aplicaba validacion de campana como parte inseparable del proceso. Esto rechazaba historico climatico valido por pertenecer a fechas anteriores al rango agronomico de arandano.

### 2.2 FK contra `Dim_Tiempo`
Al desacoplar la validacion de campana aparecio el verdadero siguiente cuello: habia fechas climaticas parseables cuyo `ID_Tiempo` no existia en `Silver.Dim_Tiempo`, generando conflicto de FK.

### 2.3 Columna equivocada para marcar estado en Bronce
La tabla `Bronce.Variables_Meteorologicas` usa `ID_Variables_Met` como surrogate tecnico real. El fact estaba intentando actualizar estado de carga usando `ID_Variables_Meteorologicas`, columna residual no identity. Consecuencia: miles de filas quedaban siempre en `CARGADO` y se reprocesaban en cada corrida.

### 2.4 Perdida de hora real en variables meteorologicas
`Bronce.Variables_Meteorologicas` no expone `Hora_Raw` como columna fisica. La hora real queda serializada dentro de `Valores_Raw`. El fact la ignoraba y terminaba insertando `Fecha_Evento` con hora `00:00:00` o colapsando varias mediciones al mismo timestamp diario.

### 2.5 Duplicados logicos conflictivos
Una vez recuperada la hora real aparecieron casos donde la misma clave `Sector_Climatico + Fecha_Evento` tenia mas de una medicion distinta. Caso validado: `F07`, `2025-11-25 14:30:00`, con diferencias en `VPD`, `Radiacion`, `Temperatura` y `Humedad` dentro del mismo archivo/carga.

---

## 3) Cambios implementados en codigo

### 3.1 `utils/fechas.py`
- `procesar_fecha()` fue refactorizado para aceptar `validar_campana=False`.
- Se mantuvieron los limites de campana para facts agronomicos.
- Clima ya no depende de la regla agronomica para parsear historico.

### 3.2 `silver/facts/fact_telemetria_clima.py`
Se aplicaron los siguientes cambios:

1. aceptacion de historico climatico con `validar_campana=False`;
2. cuarentena de `Fecha valida pero fuera de Dim_Tiempo` en vez de romper por FK;
3. uso correcto de `ID_Variables_Met` para actualizar `Estado_Carga` en Bronce;
4. eliminacion de salida ruidosa de progreso en consola;
5. lectura de `Valores_Raw` para extraer `Hora_Raw`;
6. reconstruccion de `Fecha_Evento` con granularidad completa;
7. deduplicacion por clave logica `Sector_Climatico + Fecha_Evento`;
8. manejo diferenciado entre duplicado exacto y duplicado conflictivo;
9. envio de conflictos a `MDM.Cuarentena` y exclusion de Silver.

### 3.3 `dq/validador.py`
- Correccion del minimo biologico de `Peso_Baya_g` a `0.5`.
- No fue el frente principal del dia, pero quedo alineado con DDL y reglas vigentes.

### 3.4 `seed_dimensiones.sql`
- Se documento y aplico extension de `Silver.Dim_Tiempo` para historico desde `2020-01-01`.

### 3.5 Pruebas unitarias
Se agregaron o ajustaron pruebas para:
- parseo con y sin validacion de campana,
- recuperacion de `Hora_Raw` desde `Valores_Raw`,
- colapso de duplicado exacto de clima,
- cuarentena de duplicado logico conflictivo,
- validacion corregida de peso de baya.

---

## 4) Limpieza operativa ejecutada sobre la base real
Para evitar diagnosticos contaminados por corridas previas se ejecuto limpieza controlada del dominio clima:

1. limpieza de `Silver.Fact_Telemetria_Clima`,
2. reset de `Bronce.Reporte_Clima` a `CARGADO`,
3. reset de `Bronce.Variables_Meteorologicas` a `CARGADO`,
4. reproceso limpio completo,
5. validacion SQL posterior en Silver y cuarentena.

Adicionalmente se evaluaron deduplicaciones intermedias en Bronce para aislar si el problema era historico o de logica vigente.

---

## 5) Evidencia validada al cierre

### 5.1 Estado final de duplicados en Silver
Query canonica ejecutada:

```sql
SELECT
    Sector_Climatico,
    Fecha_Evento,
    COUNT(*) AS Filas
FROM Silver.Fact_Telemetria_Clima
WHERE Precipitacion_mm IS NULL
  AND (VPD IS NOT NULL OR Radiacion_Solar IS NOT NULL)
GROUP BY Sector_Climatico, Fecha_Evento
HAVING COUNT(*) > 1
ORDER BY Fecha_Evento DESC;
```

Resultado final validado: `0 filas`.

### 5.2 Evidencia de conflicto bien contenido
`MDM.Cuarentena` ya registra casos reales de:
- `Duplicado logico conflictivo en Bronce.Variables_Meteorologicas...`
- `Humedad nula`

Interpretacion:
- el ETL ya no inserta mediciones conflictivas en Silver;
- las deja trazadas para revision operativa.

### 5.3 Rango final de clima en Silver
Resultado validado posterior a estabilizacion:
- `Fecha_Min`: `2022-01-01 01:00:00`
- `Fecha_Max`: `2026-03-29 23:30:00`
- volumen consistente con historico disponible en fuente.

### 5.4 Auditoria de carga
`Auditoria.Log_Carga` reporto corridas `OK` para `Silver.Fact_Telemetria_Clima`, sin error de motor, confirmando que el frente ya no falla por:
- FK a `Dim_Tiempo`,
- `int(None)` sobre IDs de Bronce,
- reproceso masivo artificial,
- colapso horario de `Fecha_Evento`.

---

## 6) Estado final consolidado del frente clima

### 6.1 Lo que queda estable
- carga historica de clima,
- trazabilidad por origen,
- cuarentena de fechas fuera de `Dim_Tiempo`,
- uso correcto de surrogate tecnico en Bronce,
- recuperacion de hora real,
- manejo de conflicto por duplicado logico,
- validacion final de no duplicados en Silver.

### 6.2 Lo que NO se debe reabrir sin necesidad
- volver a acoplar clima a campana agronomica,
- volver a usar `ID_Variables_Meteorologicas` para marcar estado,
- volver a colapsar `Variables_Meteorologicas` a fecha diaria,
- resolver conflicto climatico eligiendo una fila arbitrariamente sin cuarentena.

### 6.3 Pendiente documentado para despues
Sigue vigente un pendiente analitico en `Gold.Mart_Clima`:
- hoy agrega directamente desde `Silver.Fact_Telemetria_Clima`,
- mezcla filas parciales de dos origenes logicos,
- puede sesgar `AVG(Temperatura/Humedad)` y no publica `Radiacion_Solar`.

Este punto se considera pendiente analitico posterior, no bloqueo del cierre operativo del fact.

---

## 7) Dictamen de cierre del dia
El frente `Fact_Telemetria_Clima` queda **cerrado operativamente** al corte `2026-03-31`.

No se trata de una solucion parcial ni de un workaround temporal. El dominio clima queda con:
- control de historico,
- control de granularidad horaria,
- control de conflicto logico,
- trazabilidad en cuarentena,
- y criterio operativo reproducible para futuras corridas.

Cualquier nueva incidencia de clima posterior a este cierre debe tratarse ya como residuo puntual de fuente o nueva regla de negocio, no como falla estructural del ETL.

## Addendum 2026-04-01 - Referencia Operativa de Fisiologia

- Validacion real registrada:
  - `Fact_Fisiologia = 43900`
  - `Bronce.Fisiologia.PROCESADO = 43900`
  - `Bronce.Fisiologia.CARGADO = 1655`
- El residual vigente se concentra solo en `Modulo_Raw = '9.'`.
- La regla por turno de `Modulo 11` se probo y luego se desactivo por regresion real contra catalogo incompleto.
- No cerrar cambios de este frente solo con sintaxis; exigir corrida real y evidencia SQL.

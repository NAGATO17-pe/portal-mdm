# CONTEXTO OPERATIVO DWH - 2026-03-30

## 1) Estado actual del sistema
El sistema ya paso de problemas estructurales a tres frentes operativos controlados:
1. residual geografico real,
2. DQ biologico/fechas en facts,
3. cierre funcional de `Fact_Maduracion`.

Antes:
- fallaba la resolucion de geografia especial,
- el bridge de camas podia quedar en `0`,
- archivos mal ubicados contaminaban Bronce,
- `Conteo` y `Ciclo_Poda` tenian layout/logica desalineados.

Ahora:
- `VI` se resuelve como Test Block correctamente,
- el pipeline bloquea archivos mal ubicados en Bronce,
- `SP_Cama` persiste bridge y valida consistencia,
- `Fact_Ciclo_Poda` y `Fact_Conteo_Fenologico` ya estan cerrados,
- hay normalizacion global de geografia y homologacion tipografica segura de variedades.

## 2) Reglas vigentes clave
1. `VI` => Test Block.
2. `9.1/9.2`, `11.1/11.2`, `13.1/13.2`, `14.1/14.2` son modulos validos por regla MDM.
3. `9.` (sin submodulo) queda en cuarentena por diseno.
4. Cama operativa esperada: `1..100`.
5. `Fact_Conteo_Fenologico` ya no usa `ID_Cinta`.
6. `Fact_Maduracion` si usa `ID_Cinta`, `ID_Organo` e `ID_Estado_Fenologico`.
7. `ID_Organo` es identificador degenerado; NO existe `Dim_Organo`.

## 3) Resumen operativo validado
Corrida limpia de referencia:
- `Fact_Conteo_Fenologico`: `67860` insertados, `67` rechazados.
- `Fact_Evaluacion_Pesos`: `5658` insertados, `14` rechazados.
- `Fact_Evaluacion_Vegetativa`: `22872` insertados, `545` rechazados.
- `Fact_Ciclo_Poda`: `5205` insertados, `0` rechazados.
- `SP_Cama aptas`: `3866`
- `SP_Cama insert bridge`: `3866`
- `Bridge camas despues`: `3866`
- `sp_Validar_Calidad_Camas`: `OK_OPERATIVO`

## 4) Que significa esto en lenguaje simple
- `VI` es un carril exclusivo ya senalizado.
- `9.` sigue siendo direccion incompleta: no se adivina.
- `Geografia no encontrada` es direccion valida no registrada aun en el GPS.
- `LAYOUT_INCOMPATIBLE` es archivo en la carpeta equivocada o con estructura no soportada.
- `ID_Registro_Origen` en cuarentena es el numero de ticket.

## 5) Flujo diario recomendado
1. Ejecutar `py "D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\pipeline.py"`.
2. Confirmar `Servidor SQL` y `Base SQL`.
3. Revisar 7 semaforos:
   - Carga de Pesos > 0.
   - Carga de Vegetativa > 0.
   - Si hubo lote, `Fact_Maduracion > 0` o cuarentena util.
   - `VI` smoke = `RESUELTA_TEST_BLOCK`.
   - Calidad cama = `OK_OPERATIVO`.
   - Si `SP_Cama aptas > 0`, entonces bridge > 0.
   - `% cuarentena con ID origen` alto.

## 6) Troubleshooting rapido
### Si Bronce bloquea por layout o ruta
- Revisar carpeta de entrada.
- Revisar `data/rechazados/<carpeta>/`.
- No forzar carga manual de ese archivo.

### Si `SP_Cama` reporta aptas pero bridge sigue en `0`
- Es inconsistencia del paso 6.
- No publicar Gold.
- Validar misma instancia SQL y commit real.

### Si sube `Geografia no encontrada`
- Es backlog de catalogo/regla pendiente.
- No es necesariamente falla de codigo.

### Si falla `Fact_Maduracion`
- Revisar primero `Valores_Raw` en `Bronce.Maduracion`.
- Validar campos `FECHA_Raw`, `MODULO_Raw`, `TURNO_Raw`, `NROVALVULA_Raw`, `VARIEDAD_Raw`, `ORGANO_Raw`, `DESCRIPCIONESTADOCICLO_Raw`, `COLOR_Raw`.

## 7) Prioridad operativa actual
1. Reducir `Geografia no encontrada` en Vegetativa (`455`).
2. Mantener control de `9.` en cuarentena sin forzar inferencias.
3. Completar validacion funcional de `Fact_Maduracion` con lote real y DDL final consistente.
4. Sostener corrida limpia y snapshot diario para tendencia real.

## 8) Bitacora tecnica resumida
### 8.1 Error metodologico corregido
- Se dejo de asumir `ID_Registro_Origen` en `Bronce.Evaluacion_Vegetativa`.
- Para backlog historico se usa `MDM.Cuarentena.Valor_Recibido`.

### 8.2 Bloqueo de Bronce ya operativo
- Archivos mal ubicados ahora generan `LAYOUT_INCOMPATIBLE` o `RUTA_CONTENIDO_INCOMPATIBLE`.
- Se mueven a `data/rechazados`.
- El pipeline se detiene antes de Silver/Gold.

### 8.3 Bridge de camas ya persistente
- El paso 6 ahora usa transaccion con commit real.
- Solo corre si entran tablas Bronce con cama.

### 8.4 Incidentes cerrados
- `Fact_Ciclo_Poda`: cerrado.
- `Fact_Conteo_Fenologico`: cerrado.

### 8.5 Frente nuevo
- `Fact_Maduracion`:
  - fuente real: `Bronce.Maduracion`
  - payload: `Valores_Raw`
  - modelo objetivo: fila por organo observado
  - pendiente: validacion final de corrida estable con el DDL definitivo.

## 9) Criterio de verdad para decisiones
Se considera evidencia valida de avance cuando simultaneamente se cumpla:
1. `Fact_Evaluacion_Pesos > 0` y `Fact_Evaluacion_Vegetativa > 0`.
2. `VI` = `RESUELTA_TEST_BLOCK` en smoke `0/1/2`.
3. `sp_Validar_Calidad_Camas` = `OK_OPERATIVO`.
4. Si `SP_Cama aptas > 0`, entonces `Bridge_Geografia_Cama > 0`.
5. `% con ID_Registro_Origen` en nuevas cuarentenas >= `98%`.
6. Tendencia descendente de `Geografia no encontrada` en Vegetativa.

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


## 8) Addendum 2026-03-31 - Auditoria profunda y cierre del frente Clima

### 8.1 Decision operativa sobre portal
- El frente de portal tipo Streamlit NO se considera activo para implementacion inmediata.
- La definicion vigente es redisenarlo mas adelante por una alternativa mas robusta y con mayor alcance funcional.
- Cualquier referencia al portal se mantiene solo como contexto historico de planeamiento; no como entregable actual del ETL.

### 8.2 Hallazgo raiz corregido en `Fact_Telemetria_Clima`
Durante la auditoria de clima sobre la instancia real se identificaron y corrigieron cuatro problemas distintos:

1. **Regla de campana acoplada a parseo de fechas**
   - `utils/fechas.py` fue ajustado para separar parseo y validacion de campana.
   - `procesar_fecha()` ahora acepta `validar_campana=False`.
   - Esto evita rechazar historico climatico valido solo por quedar fuera del rango agronomico de arandano.

2. **FK contra `Silver.Dim_Tiempo` por historico sin seed**
   - `Fact_Telemetria_Clima` dejo de romper por `FK_Fact_Telemetria_Clima_Tiempo`.
   - Cuando la fecha parsea pero no existe en `Dim_Tiempo`, ahora va a cuarentena con motivo `Fecha valida pero fuera de Dim_Tiempo`.
   - Posteriormente se extendio `Silver.Dim_Tiempo` para cubrir historico desde `2020-01-01`.

3. **Uso de columna equivocada para marcar estado en Bronce**
   - La tabla real `Bronce.Variables_Meteorologicas` usa `ID_Variables_Met` como `IDENTITY` tecnica.
   - El fact estaba leyendo via alias correcto, pero al cerrar la carga intentaba actualizar estado usando `ID_Variables_Meteorologicas`.
   - Consecuencia: las filas quedaban en `CARGADO` y se reprocesaban en corridas posteriores, generando backlog artificial y falsa sensacion de bucle.
   - Correcion aplicada: los updates de estado ahora usan `ID_Variables_Met`.

4. **Perdida de granularidad horaria en Variables Meteorologicas**
   - La hora no existe como columna fisica en `Bronce.Variables_Meteorologicas`; queda serializada en `Valores_Raw` como `Hora_Raw=HH:MM:SS`.
   - El fact estaba construyendo `Fecha_Evento` solo con `Fecha_Raw`, colapsando mediciones subdiarias al mismo dia.
   - Correcion aplicada: se extrae `Hora_Raw` desde `Valores_Raw` y se reconstruye `Fecha_Evento` con granularidad real.

### 8.3 Regla nueva de calidad para duplicado logico de clima
Se implemento una regla explicita en `Fact_Telemetria_Clima` para resolver duplicados logicos por `Sector_Climatico + Fecha_Evento`.

Comportamiento vigente:
- Duplicado exacto: se conserva una sola fila.
- Duplicado conflictivo: ninguna fila entra a Silver y todas se envian a `MDM.Cuarentena`.
- Motivo registrado: `Duplicado logico conflictivo en Bronce.Variables_Meteorologicas: multiples mediciones para mismo Sector_Climatico + Fecha_Evento`.

Esto se valido con caso real en `F07`, `2025-11-25 14:30:00`, donde existian dos mediciones distintas para la misma clave temporal.

### 8.4 Limpieza operativa ejecutada
Para validar el frente clima sin arrastre historico se siguio una limpieza controlada de dominio:
- reset de `Bronce.Reporte_Clima` a `CARGADO`,
- reset de `Bronce.Variables_Meteorologicas` a `CARGADO`,
- limpieza de `Silver.Fact_Telemetria_Clima`,
- reproceso integral del dominio clima.

Despues del reproceso limpio:
- la hora subdiaria quedo preservada,
- desaparecio el reproceso masivo artificial,
- los duplicados conflictivos dejaron de entrar a Silver,
- la validacion `HAVING COUNT(*) > 1` para clima sin precipitacion quedo en `0` filas.

### 8.5 Queries de control que quedan como canon para clima
1. Validacion de duplicados en Silver:
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

2. Validacion de residuo reciente en cuarentena:
```sql
SELECT TOP (20)
    Fecha_Ingreso,
    Campo_Origen,
    Valor_Recibido,
    Motivo,
    ID_Registro_Origen
FROM MDM.Cuarentena
WHERE Tabla_Origen = 'Bronce.Clima'
ORDER BY ID_Cuarentena DESC;
```

3. Validacion de rango en Silver:
```sql
SELECT
    MIN(Fecha_Evento) AS Fecha_Min,
    MAX(Fecha_Evento) AS Fecha_Max,
    COUNT(*) AS Filas
FROM Silver.Fact_Telemetria_Clima;
```

### 8.6 Estado final consolidado del frente clima al 2026-03-31
- `Fact_Telemetria_Clima` queda operativo y estable.
- Historico desde `2022-01-01` hasta `2026-03-29` validado sobre la base actual.
- Regla de campana ya no bloquea clima historico.
- `Dim_Tiempo` extendida para historico desde `2020-01-01`.
- Duplicados logicos conflictivos ya no contaminan Silver.
- La auditoria confirma que el problema residual del frente clima ya no es estructural.

## 9) Addendum 2026-04-01 - Induccion Floral y Tasa de Crecimiento Brotes

### 9.1 Decision de modelado
Queda aprobado como criterio vigente:
- `Induccion_Floral` va a fact propio
- `Tasa_Crecimiento_Brotes` va a fact propio
- no deben fusionarse dentro de `Fact_Evaluacion_Vegetativa`

### 9.2 Estado de Bronce
`Bronce.Induccion_Floral` y `Bronce.Tasa_Crecimiento_Brotes` ya no usan el loader generico.

Situacion consolidada:
- `Induccion_Floral` ya guarda sus columnas estructurales en campos fisicos y deja `Valores_Raw` vacio para el layout actual
- `Tasa_Crecimiento_Brotes` solo toma la hoja `BD_General`
- el cargador ya lee como texto para evitar DNIs o claves con `.0`

### 9.3 Estado de Silver
Facts nuevos activos:
- `Silver.Fact_Induccion_Floral`
- `Silver.Fact_Tasa_Crecimiento_Brotes`

Comportamiento vigente:
- ambos resuelven `ID_Tiempo`, `ID_Geografia`, `ID_Variedad` e `ID_Personal`
- ambos usan `validar_campana=False`
- ambos envian a cuarentena solo por errores reales de fecha, geografia o negocio

### 9.4 Hallazgos operativos
1. `ID_Personal = -1` queda aceptado por ahora porque `Dim_Personal` aun no esta poblada para estos dominios.
2. Los duplicados de `Fact_Induccion_Floral` se explicaron por recarga doble del mismo archivo en Bronce; no por bug del fact.

### 9.5 Recomendaciones finales
1. No abrir Gold nuevo para estos dominios por ahora.
2. Si luego se arma dataset de modelo, debe salir desde `Silver`.
3. No meter parche anti-duplicado definitivo hasta definir politica de reingesta por archivo.

## 10) Addendum 2026-04-01 - Estado de Fisiologia

1. Baseline real validado: `43900` insertados.
2. Residual vigente: `1655` filas en `Bronce.Fisiologia` con `Estado_Carga='CARGADO'`.
3. El residual actual se concentra solo en `Modulo_Raw = '9.'`.
4. `Modulo 11` no debe resolverse por turno por ahora; esa regla se desactivo por regresion real.
5. No cerrar ningun cambio de este frente sin corrida real y evidencia SQL antes/despues.

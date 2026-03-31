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


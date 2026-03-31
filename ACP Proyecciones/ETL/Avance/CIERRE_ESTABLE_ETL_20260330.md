# CIERRE DOCUMENTAL ETL ESTABLE - 2026-03-30

## 1) Proposito del documento
Este documento congela el estado estable del ETL de ACP al corte `2026-03-30` y sirve como punto formal de restauracion operativa.

Su objetivo no es reemplazar la documentacion tecnica detallada existente, sino dejar un checkpoint claro y util para:
1. restaurar el ETL a un estado sano conocido,
2. distinguir lo ya estabilizado de lo aun pendiente,
3. evitar reabrir problemas estructurales ya cerrados,
4. fijar una base segura antes de pasar a fases posteriores de portal/API/orquestacion.

---

## 2) Fuente de verdad del checkpoint
Este cierre se basa en evidencia operativa validada sobre la instancia real:
- Servidor SQL: `LCP-PAG-PRACTIC`
- Base SQL: `ACP_DataWarehose_Proyecciones`
- Fecha de corte: `2026-03-30`

Archivos de soporte vigentes al corte:
- `README_OPERATIVO_PIPELINE.md`
- `Avance/CONTEXTO_OPERATIVO_DWH_20260325.md`
- `Avance/CHECKLIST_OPERATIVO_5_MINUTOS.md`
- `Avance/INFORME_BASELINE_FASE16_20260325.md`

Este documento se considera el resumen ejecutivo consolidado del estado estable alcanzado.

---

## 3) Alcance del estado estable
El checkpoint cubre como estables los siguientes frentes:

### 3.1 Capa Bronce
- Carga de archivos Excel por carpeta operativa.
- Deteccion de header real.
- Normalizacion robusta de encabezados.
- Manejo de columnas duplicadas post-normalizacion.
- Bloqueo de archivos mal ubicados o incompatibles por contenido.
- Serializacion controlada de payload extra en `Valores_Raw` cuando aplica.

### 3.2 Capa Silver
- `Dim_Geografia`
- `Dim_Personal` en estado actual conservador
- `Fact_Conteo_Fenologico`
- `Fact_Evaluacion_Pesos`
- `Fact_Evaluacion_Vegetativa`
- `Fact_Ciclo_Poda`
- `Fact_Maduracion`

### 3.3 Gobernanza MDM / DQ
- Homologacion segura de variedades.
- Normalizacion global de geografia antes del resolvedor.
- Reglas activas de `Modulo_Raw`.
- Cuarentena con `ID_Registro_Origen` y `Campo_Origen`.
- Alias seguros de `Dim_Cinta`.

### 3.4 Gobernanza de camas
- `Silver.sp_Upsert_Cama_Desde_Bronce`
- `Silver.sp_Validar_Calidad_Camas`
- Persistencia del `Bridge_Geografia_Cama`
- Ejecucion condicional del paso 6 solo cuando entran tablas con cama.

---

## 4) Componentes estabilizados y su estado

### 4.1 Bronce: normalizacion de encabezados consolidada
Se estabilizo el comportamiento del cargador de Bronce para evitar que columnas validas del Excel queden mal catalogadas como extras o terminen en `NULL` en tablas fisicas.

Cambios funcionales ya consolidados:
- Normalizacion Unicode real (`NFKD`).
- Eliminacion de tildes y simbolos operativos como `°`, `º`, `#`.
- Colapso de separadores inconsistentes.
- Resolucion case-insensitive de alias.
- Consolidacion de columnas duplicadas tras renombrado, tomando el primer valor no nulo por fila.
- Mejora del reporte de `columnas extras` en consola.

Resultado validado:
- `Bronce.Evaluacion_Pesos` ya pobla correctamente `Fecha_Raw` y `Cama_Raw`.
- `Bronce.Evaluacion_Vegetativa` ya pobla correctamente `Descripcion_Raw`, `Evaluacion_Raw`, `N_Plantas_Evaluadas_Raw`, `N_Plantas_en_Floracion_Raw` y `Cama_Raw`.
- `Bronce.Maduracion` usa `Valores_Raw` de forma consistente para los campos del layout real.
- `Bronce.Evaluacion_Calidad_Poda` ya promueve correctamente desde el layout real de Excel hacia columnas fisicas como `Tipo_Evaluacion_Raw`, `TallosPlanta_Raw`, `LongitudTallo_Raw`, `DiametroTallo_Raw`, `RamillaPlanta_Raw`, `ToconesPlanta_Raw`, `CortesDefectuosos_Raw` y `AlturaPoda_Raw`.

### 4.2 Bloqueo de archivos mal ubicados o incompatibles
El ETL ya no permite que archivos mal ubicados contaminen Bronce.

Estado consolidado:
- Si un archivo cae en carpeta incorrecta pero su contenido corresponde claramente a otra ruta, el pipeline lo bloquea.
- Si un layout no cumple la firma esperada, el pipeline genera `LAYOUT_INCOMPATIBLE`.
- Si la carpeta y el contenido se contradicen, el pipeline genera `RUTA_CONTENIDO_INCOMPATIBLE`.
- Los archivos rechazados se mueven a `data/rechazados/<carpeta>/`.
- El pipeline se detiene antes de Silver/Gold cuando el incidente es estructural.

Esto ya evita falsos positivos operativos como el caso del archivo de vegetativa colocado dentro de `ciclos_fenologicos`.

### 4.3 Normalizacion global de geografia
La resolucion geografica dejo de depender del formato exacto crudo del Excel.

Comportamiento vigente:
- `MODULO 2 -> 2`
- `TURNO 04 -> 4`
- `NROVALVULA 15 -> 15`
- `Valvula=57 -> 57`
- `Modulo=9.1` conserva submodulo cuando corresponde.

Esto aplica de forma transversal al flujo previo a `obtener_id_geografia()` y ya no solo a un fact aislado.

### 4.4 Reglas activas de modulo
Queda congelado como estado valido el siguiente catalogo operativo de `Modulo_Raw`:
- `VI` => caso especial Test Block.
- `9.1` => `Modulo=9`, `SubModulo=1`, `Tipo_Conduccion='SUELO'`.
- `9.2` => `Modulo=9`, `SubModulo=2`, `Tipo_Conduccion='MACETA'`.
- `11.1` => `Modulo=11`, `SubModulo=1`, `Tipo_Conduccion='SUELO'`.
- `11.2` => `Modulo=11`, `SubModulo=2`, `Tipo_Conduccion='MACETA'`.
- `13.1` => `Modulo=13`, `SubModulo=1`, `Tipo_Conduccion='SUELO'`.
- `13.2` => `Modulo=13`, `SubModulo=2`, `Tipo_Conduccion='MACETA'`.
- `14.1` => `Modulo=14`, `SubModulo=1`, `Tipo_Conduccion='SUELO'`.
- `14.2` => `Modulo=14`, `SubModulo=2`, `Tipo_Conduccion='MACETA'`.

Restriccion vigente:
- `9.` sin submodulo sigue y debe seguir yendo a cuarentena.
- No se permite inferencia automatica de `9.` hacia `9.1` o `9.2`.

### 4.5 Gobernanza de camas y bridge
El incidente estructural mas critico de cama/bridge queda cerrado en este checkpoint.

Estado estable confirmado:
- El paso 6 del pipeline persiste correctamente usando transaccion con commit real.
- El SP de upsert de cama solo corre cuando ingresan tablas Bronce con cama.
- El bridge ya no queda en `0` cuando existen camas aptas.

Corrida de referencia validada:
- `SP_Cama filas evaluadas`: `29089`
- `SP_Cama aptas`: `3866`
- `SP_Cama insert bridge`: `3866`
- `Bridge camas antes`: `0`
- `Bridge camas despues`: `3866`
- `SP_Cama estado calidad`: `OK_OPERATIVO`

Criterio congelado:
- Si `SP_Cama aptas > 0`, entonces `Bridge camas despues > 0`.
- Si esa condicion no se cumple, no se considera corrida publicable a Gold.

### 4.6 Homologacion tipografica segura de variedades
Se fortalecio el match de variedades sin introducir merges semanticos inseguros.

Cobertura consolidada:
- Guiones tipograficos y normales.
- Espacios inconsistentes entre letras y numeros.
- Apostrofes tipograficos.
- Compactacion segura para equivalencias puramente tipograficas.

Ejemplos que ahora deben resolver mejor:
- `FCM15 – 005`
- `FCM15 - 005`
- `FCM15-005`
- `FL 19-006`
- `FL19 - 006`
- `MEGA CRISP`
- `Megacrisp`
- `O'Neal`
- `O’Neal`

Restriccion congelada:
- No se auto-fusionan casos ambiguos como diferencias que puedan representar geneticas distintas.
- Lo ambiguo sigue yendo a MDM.

### 4.7 Alias seguros de cinta
La resolucion de `Dim_Cinta` queda ampliada con alias seguros aprobados en codigo.

Alias activos al corte:
- `Amarillo -> Amarilla`
- `Blanco -> Blanca`
- `Rojo -> Roja`

Adicionalmente, el catalogo de `Dim_Cinta` fue ampliado operativamente para soportar colores reales usados en campo como `Rosado`, `Celeste`, `Plomo` y `Negro`.

### 4.8 Fact_Conteo_Fenologico
Estado: estable.

Condiciones congeladas:
- Soporta layout ancho y parseo desde `Valores_Raw`.
- Ya no usa `ID_Cinta`.
- No requiere parche adicional en este checkpoint.

### 4.9 Fact_Evaluacion_Pesos
Estado: estable con residual funcional controlado.

Corridas validadas:
- referencia limpia: `5658` insertados, `14` rechazados.
- referencia posterior estabilizada: `5665` insertados, `7` rechazados.

Conclusiones congeladas:
- El problema estructural de columnas en `NULL` por encabezados ya fue corregido.
- El residual restante corresponde a reglas biologicas y/o geografia puntual, no a falla del cargador.

### 4.10 Fact_Evaluacion_Vegetativa
Estado: estable con residual funcional conocido.

Corrida validada:
- `22872` insertados
- `545` rechazados

Residual dominante aceptado al corte:
- `Geografia no encontrada en Silver.Dim_Geografia`: componente mayoritario.
- Casos menores de `Plantas en floracion invalida`, `Cantidad de plantas evaluadas invalida` y `Fecha invalida o fuera de campana`.

Conclusiones congeladas:
- La tabla ya carga correctamente en Silver.
- El residual actual es backlog de catalogacion/calidad, no un problema estructural del ETL.

### 4.11 Fact_Ciclo_Poda
Estado: estable.

Corrida validada de referencia:
- `5205` insertados
- `0` rechazados

La geografia por `Modulo + Turno + Valvula` queda alineada y no requiere accion tecnica adicional en este checkpoint.
Adicionalmente, queda congelado que las metricas estructuradas de poda ya no deben quedar relegadas a `Valores_Raw` cuando el layout soportado se recarga correctamente en Bronce.

### 4.12 Fact_Maduracion
Estado: estabilizado y funcional.

Diseno operativo aprobado al corte:
- Una fila por organo observado.
- Usa `ID_Personal`, `ID_Geografia`, `ID_Tiempo`, `ID_Variedad`, `ID_Estado_Fenologico`, `ID_Cinta`, `ID_Organo`, `Dias_Pasados_Del_Marcado`, `Fecha_Evento`, `Fecha_Sistema`, `Estado_DQ`.
- `ID_Organo` es identificador degenerado del seguimiento; no existe `Dim_Organo`.

Comportamiento consolidado:
- Toma variedad desde `Valores_Raw` y la pasa por homologacion robusta.
- Resuelve cinta por color con alias seguros.
- Resuelve estado fenologico desde el payload real.
- Usa `Valores_Raw` como fuente operativa legitima del layout real de maduracion.

Evidencia de estabilizacion:
- corrida intermedia estable: `25193` insertados y residual reducido solo a cinta.
- corrida final validada por el usuario: sin nuevas filas en `MDM.Cuarentena` para `Bronce.Maduracion`.

Conclusiones congeladas:
- `Fact_Maduracion` deja de ser gap estructural.
- Los rechazos masivos previos se explicaron por mezcla de archivo incorrecto y alias faltantes de cinta, ambos ya cerrados.

### 4.13 Dim_Personal
Estado: estable en su implementacion actual, pero funcionalmente conservador.

Situacion congelada:
- La dimension existe y se mantiene operativa.
- Su poblamiento actual desde Excel es limitado porque la fuente fuerte sigue siendo `Tareos`.
- No se considera un frente cerrado funcionalmente hasta probar con un Excel real de `Consolidado_Tareos`.

Esto NO invalida el checkpoint del ETL estable; simplemente delimita una expansion pendiente.

### 4.14 Ajuste minimo de Clima
Estado: definido como ajuste transitorio controlado.

Decision consolidada:
- `Fact_Telemetria_Clima` deja de depender de `Dim_Geografia`.
- El grano operativo pasa a ser `Fecha/Hora + Sector_Climatico`.
- `Gold.Mart_Clima` se agrega por `ID_Tiempo + Sector_Climatico`.

Motivo tecnico:
- El sector de clima no representa `Fundo/Modulo/Turno/Valvula/Cama`.
- Forzarlo a `Dim_Geografia` mezcla geografia agronomica con sector meteorologico.

Alcance del ajuste:
- Se modifica el loader Python de clima.
- Se deja script SQL de ajuste para `Silver.Fact_Telemetria_Clima` y `Gold.Mart_Clima`.
- La futura `Dim_Estacion_Climatica` queda postergada y fuera del checkpoint estable.

---

## 5) Corridas de referencia que sustentan el checkpoint

### 5.1 Corrida limpia general de referencia
Resultado validado como evidencia fuerte de estabilidad:
- `Fact_Conteo_Fenologico`: `67860` insertados, `67` rechazados.
- `Fact_Evaluacion_Pesos`: `5658` insertados, `14` rechazados.
- `Fact_Evaluacion_Vegetativa`: `22872` insertados, `545` rechazados.
- `Fact_Ciclo_Poda`: `5205` insertados, `0` rechazados.
- `SP_Cama aptas`: `3866`
- `SP_Cama insert bridge`: `3866`
- `Bridge camas despues`: `3866`
- `SP_Cama estado calidad`: `OK_OPERATIVO`

### 5.2 Corrida focalizada de Maduracion
Resultado validado del frente nuevo:
- `Bronce.Maduracion`: `26800` filas insertadas.
- `Fact_Maduracion`: `25193` insertados.
- Residual final por `Bronce.Maduracion`: cerrado en la ultima verificacion mostrada.

### 5.3 Corrida de validacion post-fix de encabezados
Resultado validado tras reforzar `normalizar_columnas()`:
- `Fact_Evaluacion_Pesos`: `5665` insertados, `7` rechazados.
- `Fact_Evaluacion_Vegetativa`: `22872` insertados, `545` rechazados.
- `SP_Cama aptas`: `3866`
- `Bridge camas despues`: `3866`
- `OK_OPERATIVO` mantenido.

---

## 6) Residual aceptado al corte
El siguiente residual se considera aceptado y NO rompe el checkpoint.

### 6.1 Evaluacion_Pesos
Residual tipico observado:
- geografia puntual no catalogada,
- peso promedio no calculable,
- peso fuera de rango biologico.

### 6.2 Evaluacion_Vegetativa
Residual tipico observado:
- backlog geografico real,
- plantas en floracion invalidas,
- total evaluado invalido,
- fechas fuera de campana.

### 6.3 Conteo_Fenologico
Residual menor ya conocido:
- geografia no encontrada,
- algun caso aislado de cinta en historico.

### 6.4 Maduracion
Al corte final del cierre, no queda residual abierto nuevo mostrado por el usuario para `Bronce.Maduracion`.

Interpretacion operativa:
- El residual aceptado actual es principalmente backlog de negocio/catalogo/DQ.
- No es evidencia de inestabilidad estructural del ETL.

---

## 7) Lo que NO forma parte del estado estable congelado
Queda explicitamente fuera del checkpoint estable:
1. Poblamiento fuerte de `Dim_Personal` sin Excel real de `Tareos`.
2. Portal Streamlit MDM productivo.
3. API de operacion con FastAPI.
4. Orquestacion con Airflow.
5. Frontend con Next.js.
6. Clasificacion automatica total de archivos por contenido antes de Bronce.
7. Explotacion analitica formal de `Valores_Raw` como capa sidecar separada.

Estos frentes pueden iniciarse despues, pero no deben confundirse con el ETL base ya estabilizado.

---

## 8) Archivos y modulos clave que definen este checkpoint
Los siguientes archivos quedan como parte del estado tecnico de referencia:
- `pipeline.py`
- `bronce/cargador.py`
- `bronce/rutas.py`
- `utils/texto.py`
- `mdm/lookup.py`
- `mdm/homologador.py`
- `silver/facts/fact_conteo_fenologico.py`
- `silver/facts/fact_evaluacion_pesos.py`
- `silver/facts/fact_evaluacion_vegetativa.py`
- `silver/facts/fact_ciclo_poda.py`
- `silver/facts/fact_maduracion.py`
- `silver/dims/dim_personal.py` en su estado actual
- scripts SQL de cama/modulo/cinta ya aplicados en base

---

## 9) Procedimiento de restauracion recomendado
Si en una fase posterior se rompe el ETL y se necesita volver a un estado sano, usar esta secuencia como punto de restauracion conceptual y tecnico.

### 9.1 Restauracion logica
1. Restaurar codigo de los modulos listados en la seccion 8 a este checkpoint.
2. Confirmar que se mantiene el comportamiento de normalizacion de encabezados.
3. Confirmar que `lookup.py` conserva:
   - saneamiento global de geografia,
   - alias de cinta seguros,
   - resolvedor de geografia previo a los facts.
4. Confirmar que `fact_maduracion.py` sigue leyendo del payload real y resuelve `ID_Organo`, `ID_Cinta`, `ID_Estado_Fenologico` y `ID_Variedad`.

### 9.2 Restauracion operativa
1. Ejecutar pipeline con un lote controlado de referencia.
2. Validar estos 7 semaforos:
   - `Fact_Evaluacion_Pesos > 0`
   - `Fact_Evaluacion_Vegetativa > 0`
   - `Fact_Conteo_Fenologico > 0`
   - si hubo lote de maduracion, `Fact_Maduracion > 0`
   - `VI` sigue resolviendo `RESUELTA_TEST_BLOCK`
   - `sp_Validar_Calidad_Camas = OK_OPERATIVO`
   - si `SP_Cama aptas > 0`, entonces `Bridge camas despues > 0`

### 9.3 Restauracion de criterio
Si tras una futura modificacion reaparecen estos sintomas, asumir regresion estructural:
- `Bronce` vuelve a dejar columnas clave en `NULL` por encabezados validos.
- `SP_Cama aptas > 0` pero `Bridge camas despues = 0`.
- `Fact_Maduracion` vuelve a rechazar masivamente por cinta, organo o variedad ya resueltos.
- reaparecen archivos mal ubicados contaminando Bronce sin bloqueo.

---

## 10) Reglas de no regresion
Quedan definidas como reglas de no regresion del checkpoint:
1. No retirar el bloqueo de `LAYOUT_INCOMPATIBLE` / `RUTA_CONTENIDO_INCOMPATIBLE`.
2. No relajar la cuarentena de `9.` sin submodulo.
3. No volver a hacer match crudo de variedad sin pasar por normalizacion y homologacion segura.
4. No eliminar los alias seguros de cinta ya aprobados.
5. No romper la consolidacion de encabezados y columnas duplicadas en Bronce.
6. No volver a ejecutar el upsert de cama sin commit real.
7. No sacar `Fact_Maduracion` del flujo ni volver al diseno viejo previo a fila por organo.
8. No convertir `ID_Organo` en una dimension separada mientras el negocio lo mantenga como identificador degenerado.

---

## 11) Frontera de la siguiente fase
A partir de este cierre, la recomendacion tecnica es:
1. no seguir parchando el ETL base salvo errores reales nuevos,
2. pasar a pruebas faltantes de `Dim_Personal` con Excel de `Tareos`,
3. luego evaluar capa operativa superior (`Streamlit`, `FastAPI`, `Next.js`, `Airflow`) sobre una base ya estable.

Este documento deja formalmente establecido que el ETL base ya alcanzo un estado de operacion controlada suficiente para convertirse en punto de restauracion.

---

## 12) Estado final del checkpoint
Estado del ETL al corte `2026-03-30`:
- `ETL BASE`: `ESTABLE`
- `MADURACION`: `ESTABLE`
- `BRIDGE CAMA`: `ESTABLE`
- `NORMALIZACION DE HEADERS`: `ESTABLE`
- `NORMALIZACION GLOBAL DE GEOGRAFIA`: `ESTABLE`
- `HOMOLOGACION TIPOGRAFICA SEGURA`: `ESTABLE`
- `DIM_PERSONAL CON TAREOS`: `PENDIENTE DE VALIDACION`
- `PORTAL/API/ORQUESTACION`: `FUERA DE ESTE CHECKPOINT`

Checkpoint operativo oficial recomendado: `ACTIVO`

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


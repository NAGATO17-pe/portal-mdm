# ACTUALIZACION OPERATIVA - INDUCCION FLORAL Y TASA DE CRECIMIENTO - 2026-04-01

## 1) Proposito
Este documento deja un checkpoint tecnico especifico para los nuevos dominios:
- `Bronce.Induccion_Floral`
- `Bronce.Tasa_Crecimiento_Brotes`
- `Silver.Fact_Induccion_Floral`
- `Silver.Fact_Tasa_Crecimiento_Brotes`

Su objetivo es fijar:
1. el layout real ya validado,
2. el estado estable actual de Bronce,
3. el estado funcional actual de Silver,
4. los riesgos y recomendaciones finales antes de abrir Gold o modelado.

---

## 2) Dictamen ejecutivo
Estado al `2026-04-01`:
- `Bronce.Induccion_Floral`: `ESTABLE`
- `Bronce.Tasa_Crecimiento_Brotes`: `ESTABLE`
- `Silver.Fact_Induccion_Floral`: `FUNCIONAL`
- `Silver.Fact_Tasa_Crecimiento_Brotes`: `FUNCIONAL`

Conclusiones aprobadas:
1. `Induccion_Floral` debe mantenerse como fact separado.
2. `Tasa_Crecimiento_Brotes` debe mantenerse como fact separado.
3. Ambos dominios deben seguir pasando primero por Bronce estructurado; no van directo a Silver.
4. No crear Gold nuevo para features del modelo en esta fase.
5. El modelo futuro debe consumir `Silver`, no `Gold`.

---

## 3) Hallazgos del layout real

### 3.1 Induccion Floral
Archivo real validado:
- Excel de entrada en carpeta `data/entrada/induccion_floral/`

Campos reales relevantes:
- `Fecha`
- `DNI`
- `Fecha Subida`
- `Nombres`
- `Consumidor`
- `Modulo`
- `Turno`
- `Valvula`
- `Evaluacion`
- `N° de cama`
- `Descripcion`
- `Plantas por Cama`
- `Plantas con Induccion`
- `Brotes con Induccion`
- `Brotes Totales`
- `Brotes con Flor`

Conclusion de grano:
- evento agregado por fecha + geografia + evaluador + variedad/descripcion.

### 3.2 Tasa de Crecimiento Brotes
Archivo real validado:
- hoja fuente correcta: `BD_General`
- las hojas resumen no forman parte del raw operativo

Campos reales relevantes:
- `Codigo_Origen`
- `Semana`
- `Dia`
- `Fecha Evaluacion`
- `DNI`
- `Evaluador`
- `Modulo`
- `Turno`
- `Valvula`
- `Condicion`
- `Estado Vegetativo`
- `Variedad`
- `Cama`
- `Tipo de Tallo`
- `Ensayo`
- `Medida`
- `Fecha Poda Aux`
- `Campana`
- `Observacion`
- `Evaluacion`

Conclusion de grano:
- observacion por ensayo/medida con contexto biologico propio.

---

## 4) Estado final de Bronce

### 4.1 Bronce.Induccion_Floral
La tabla ya quedó ampliada y cargando fisicamente:
- `Fecha_Raw`
- `DNI_Raw`
- `Fecha_Subida_Raw`
- `Nombres_Raw`
- `Evaluador_Raw`
- `Consumidor_Raw`
- `Modulo_Raw`
- `Turno_Raw`
- `Valvula_Raw`
- `Tipo_Evaluacion_Raw`
- `Cama_Raw`
- `Descripcion_Raw`
- `Variedad_Raw`
- `PlantasPorCama_Raw`
- `PlantasConInduccion_Raw`
- `BrotesConInduccion_Raw`
- `BrotesTotales_Raw`
- `BrotesConFlor_Raw`
- `Estado_Carga`

Estado validado:
- `Valores_Raw` vacio para el layout estructural actual.
- no debe usarse el loader generico para este dominio.

### 4.2 Bronce.Tasa_Crecimiento_Brotes
La tabla ya quedó ampliada y cargando fisicamente:
- `Codigo_Origen_Raw`
- `Semana_Raw`
- `Dia_Raw`
- `Fecha_Raw`
- `DNI_Raw`
- `Evaluador_Raw`
- `Modulo_Raw`
- `Turno_Raw`
- `Valvula_Raw`
- `Condicion_Raw`
- `Estado_Vegetativo_Raw`
- `Variedad_Raw`
- `Cama_Raw`
- `Tipo_Tallo_Raw`
- `Ensayo_Raw`
- `Medida_Raw`
- `Fecha_Poda_Aux_Raw`
- `Campana_Raw`
- `Observacion_Raw`
- `Tipo_Evaluacion_Raw`
- `Estado_Carga`

Estado validado:
- se procesa solo la hoja `BD_General`.
- `Valores_Raw` queda vacio para el layout estructural actual.
- el cargador lee como texto para evitar DNIs con `.0`.

---

## 5) Estado final de Silver

### 5.1 Silver.Fact_Induccion_Floral
Columnas clave activas:
- `ID_Geografia`
- `ID_Tiempo`
- `ID_Variedad`
- `ID_Personal`
- `Tipo_Evaluacion`
- `Codigo_Consumidor`
- `Cantidad_Plantas_Por_Cama`
- `Cantidad_Plantas_Con_Induccion`
- `Cantidad_Brotes_Con_Induccion`
- `Cantidad_Brotes_Totales`
- `Cantidad_Brotes_Con_Flor`
- `Pct_Plantas_Con_Induccion`
- `Pct_Brotes_Con_Induccion`
- `Pct_Brotes_Con_Flor`
- `Fecha_Evento`
- `Estado_DQ`

Reglas funcionales activas:
- no valida campaña global (`validar_campana=False`)
- resuelve geografia con `Modulo + Turno + Valvula + Cama`
- calcula porcentajes desde componentes
- manda a cuarentena conteos invalidos o geografia no resuelta

Estado operativo:
- carga correcta validada
- sin cuarentena nueva en la ultima corrida observada

Observacion importante:
- se detectaron duplicados en `Silver.Fact_Induccion_Floral`, pero el origen fue operativo:
  - el mismo archivo se cargó dos veces en Bronce durante pruebas.
- dictamen:
  - no es bug del fact;
  - no abrir parche anti-duplicado definitivo hasta definir politica de recarga.

### 5.2 Silver.Fact_Tasa_Crecimiento_Brotes
Columnas clave activas:
- `ID_Geografia`
- `ID_Tiempo`
- `ID_Variedad`
- `ID_Personal`
- `Tipo_Evaluacion`
- `Condicion`
- `Estado_Vegetativo`
- `Tipo_Tallo`
- `Codigo_Ensayo`
- `Codigo_Origen`
- `Campana`
- `Observacion`
- `Fecha_Poda_Aux`
- `Dias_Desde_Poda`
- `Medida_Crecimiento`
- `Fecha_Evento`
- `Estado_DQ`

Reglas funcionales activas:
- no valida campaña global (`validar_campana=False`)
- exige `Codigo_Ensayo` no vacio
- exige `Medida_Crecimiento >= 0`
- calcula `Dias_Desde_Poda`
- manda a cuarentena si la fecha de poda es posterior al evento

Estado operativo:
- carga correcta validada
- sin cuarentena nueva en la ultima corrida observada

---

## 6) Hallazgos pendientes que NO son falla estructural

### 6.1 ID_Personal = -1
Estado actual:
- esperado por ahora

Motivo:
- `Silver.Dim_Personal` aun no está poblada de forma suficiente para estos dominios.

Dictamen:
- no considerar esto como error del fact mientras `Dim_Personal` siga vacia o incompleta.

### 6.2 Duplicados en Induccion
Estado actual:
- explicados por recarga doble del mismo archivo de prueba

Dictamen:
- no deduplicar a ciegas en Silver mientras no se defina la regla oficial de recarga.
- primero distinguir siempre:
  1. archivo repetido,
  2. lote repetido,
  3. duplicado real de fuente.

---

## 7) Recomendacion final de arquitectura
La recomendacion final consolidada es:

1. Mantener `Induccion_Floral` y `Tasa_Crecimiento_Brotes` como facts separados en `Silver`.
2. No ensanchar `Fact_Evaluacion_Vegetativa` con estas medidas.
3. No crear por ahora marts Gold nuevos solo para estos dominios.
4. Si mas adelante se requiere dashboard especifico, recien evaluar un mart nuevo.
5. Para modelo predictivo, consumir estas facts desde `Silver`, no desde `Gold`.

Motivo tecnico:
- `Gold` debe seguir reservado a presentacion y BI.
- `Silver` ya contiene el detalle tipado y gobernado que necesita el modelo.
- Crear Gold de features ahora mezclaria consumo humano con preparacion de ML.

---

## 8) Recomendaciones finales aprobadas
1. No abrir mas cambios de esquema en estas tablas salvo evidencia fuerte.
2. No tocar `Fact_Induccion_Floral` ni `Fact_Tasa_Crecimiento_Brotes` por el tema de `ID_Personal=-1` mientras `Dim_Personal` siga vacia.
3. No abrir parche anti-duplicado permanente hasta definir politica de reingesta por archivo.
4. Si se va a seguir probando con el mismo Excel, limpiar Bronce y Silver del dominio antes de cada corrida comparativa.
5. Cuando se abra la fase de modelado, construir dataset desde `Silver` mediante vista o query consolidada; no desde `Gold`.

---

## 9) Punto de partida recomendado para la siguiente fase
Cuando se retome este frente, el orden correcto es:
1. definir politica operativa de recarga y deduplicacion por archivo,
2. poblar `Dim_Personal` para salir de `ID_Personal = -1`,
3. recien despues evaluar dataset de features para modelo desde `Silver`,
4. y solo si aparece necesidad real de visualizacion, disenar Gold especifico.

Checkpoint recomendado: `ACTIVO`

## 10) Addendum 2026-04-01 - Fisiologia

### Estado validado con corrida real
- `Fact_Fisiologia = 43900`
- `Bronce.Fisiologia.PROCESADO = 43900`
- `Bronce.Fisiologia.CARGADO = 1655`

### Dictamen vigente
- El baseline sano de `Fisiologia` sigue en `43900 / 1655`.
- El residual actual se concentra solo en `Modulo_Raw = '9.'`.
- `Modulo 11` debe permanecer sin regla por turno mientras `SubModulo` siga incompleto en catalogo geografico.
- Las reglas finales de `9.` quedan pendientes hasta que negocio y MDM cierren el criterio completo.

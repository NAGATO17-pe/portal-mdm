# README Operativo - ETL ACP (estado estable)

## 1) Objetivo
Operacion oficial del ETL con:
- Resolucion geografica por SP.
- Gobernanza de cama por catalogo y bridge.
- Normalizacion de `Modulo_Raw` via reglas MDM.
- Carga de `Maduracion` como base del analisis Six Week.
- Normalizacion global de componentes geograficos antes del resolvedor.
- Homologacion tipografica segura de variedades antes de MDM.

## 2) Archivos operativos (raiz)
- `pipeline.py`
- `lookup.py`
- `texto.py`
- `homologador.py`
- `fact_maduracion.py`
- `fact_evaluacion_pesos.py`
- `fact_evaluacion_vegetativa.py`
- `fase10_diagnostico_calidad_camas.sql`
- `fase11_sp_geografia_cama.sql`
- `fase12_regla_modulo_raw_y_sp.sql`
- `fase17_reglas_modulo_raw_11_13_14.sql`
- `fase18_fact_maduracion_y_cinta.sql`
- `fase11_limpieza_controlada_legacy.sql`

Archivos historicos/descartados:
- `legacy_no_usar/`

## 3) Secuencia diaria recomendada
1. Ejecutar `py pipeline.py`.
2. Revisar resumen final en consola.
3. Confirmar contexto SQL del resumen:
   - `Servidor SQL`
   - `Base SQL`
4. Validar semaforo de camas:
   - `EXEC Silver.sp_Validar_Calidad_Camas @Cama_Max_Permitida=100, @Max_Camas_Por_Geografia=100;`
5. Verificar bridge:
   - `SP_Cama aptas > 0` y `Bridge camas despues > 0`
6. Si estado <> `OK_OPERATIVO`, o si hay aptas y el bridge queda en `0`, detener publicacion de Gold.
7. Si `Fact_Maduracion` rechaza filas, revisar `MDM.Cuarentena` con foco en:
   - `ID_Organo invalido o ausente en maduracion`
   - `Estado fenologico no reconocido en maduracion`
   - `Cinta no reconocida o ausente en maduracion`

## 4) Reglas activas de cama y modulo
- Catalogo permitido de cama: `1..100`.
- `Cama_Raw` nula/vacia/0: se resuelve solo geografia base.
- `Cama_Raw` fuera de `1..100`: `CAMA_NO_VALIDA`.
- `Modulo_Raw`:
  - `9.1 -> Modulo=9, SubModulo=1, Tipo_Conduccion='SUELO'`
  - `9.2 -> Modulo=9, SubModulo=2, Tipo_Conduccion='MACETA'`
  - `11.1 -> Modulo=11, SubModulo=1, Tipo_Conduccion='SUELO'`
  - `11.2 -> Modulo=11, SubModulo=2, Tipo_Conduccion='MACETA'`
  - `13.1 -> Modulo=13, SubModulo=1, Tipo_Conduccion='SUELO'`
  - `13.2 -> Modulo=13, SubModulo=2, Tipo_Conduccion='MACETA'`
  - `14.1 -> Modulo=14, SubModulo=1, Tipo_Conduccion='SUELO'`
  - `14.2 -> Modulo=14, SubModulo=2, Tipo_Conduccion='MACETA'`
  - `VI -> CASO_ESPECIAL_MODULO (test block)`
- Saneamiento global de geografia:
  - `MODULO 2 -> 2`
  - `TURNO 04 -> 4`
  - `NROVALVULA 15 -> 15`
  - `Valvula=57 -> 57`
- `Fact_Conteo_Fenologico` ya no usa `ID_Cinta`.
- `Fact_Maduracion` si usa `ID_Cinta`, `ID_Organo` y `ID_Estado_Fenologico`.
- `Fact_Telemetria_Clima` usa `Sector_Climatico` directo; no depende de `Dim_Geografia`.

## 5) Comandos utiles
Resolver un caso puntual:
```sql
EXEC Silver.sp_Resolver_Geografia_Cama
    @Modulo_Raw='9.1',
    @Turno_Raw='05',
    @Valvula_Raw='19',
    @Cama_Raw='1';
```

Upsert cama desde Bronce:
```sql
EXEC Silver.sp_Upsert_Cama_Desde_Bronce
    @Modo_Aplicar=1,
    @Cama_Min_Permitida=1,
    @Cama_Max_Permitida=100;
```

Verificar bridge real:
```sql
SELECT COUNT(*) AS Bridge_Geografia_Cama
FROM Silver.Bridge_Geografia_Cama;
```

Validar calidad:
```sql
EXEC Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida=100,
    @Max_Camas_Por_Geografia=100;
```

## 6) Criterios de exito
- `Estado_Calidad_Cama = 'OK_OPERATIVO'`
- `Cama_Fuera_Regla = 0`
- `Geografias_Saturadas = 0`
- Si `SP_Cama aptas > 0`, entonces `Bridge_Geografia_Cama > 0`
- Reduccion sostenida de cuarentena geografia/cama.
- Reduccion de cuarentena tipografica de variedades sin mezclar geneticas ambiguas.
- `Fact_Maduracion > 0` cuando exista lote real en `Bronce.Maduracion`.

## 7) Incidentes operativos que ya no se deben mezclar
- `LAYOUT_INCOMPATIBLE`: archivo mal ubicado o con estructura no soportada. Se mueve a `data/rechazados/<carpeta>/`.
- `rechazados` de un fact: problema funcional de mapeo/DQ del loader correspondiente.
- `backlog historico CARGADO`: lote viejo pendiente que puede distorsionar el resumen del dia si no se acota al lote actual.
- `bridge en 0`: inconsistencia del paso 6; no publicar Gold aunque la calidad de camas salga `OK_OPERATIVO`.
- `maduracion sin match`: falta `ID_Organo`, `COLOR_Raw`, `DESCRIPCIONESTADOCICLO_Raw` o geografia resoluble; no es fallo de `Conteo_Fruta`.
- `variedad tipografica`: diferencia de guiones/apostrofes/espacios/codigos; primero normalizacion segura, luego MDM.

## 8) Ajuste confirmado en Poda
El layout real de `Evaluacion_Calidad_Poda` puede traer metricas validas bajo nombres distintos a las columnas fisicas de Bronce.

Mapeos consolidados en el cargador:
- `Evaluacion_Raw -> Tipo_Evaluacion_Raw`
- `Tallos_Planta_Raw -> TallosPlanta_Raw`
- `Longitud_de_Tallo_Raw -> LongitudTallo_Raw`
- `Diametro_de_Tallo_Raw -> DiametroTallo_Raw`
- `Ramilla_Planta_Raw -> RamillaPlanta_Raw`
- `Tocones_Planta_Raw -> ToconesPlanta_Raw`
- `N_Cortes_Defect_Planta_Raw -> CortesDefectuosos_Raw`
- `Altura_de_Planta_Raw -> AlturaPoda_Raw`

Criterio operativo:
- Estas metricas ya no deben quedar en `Valores_Raw` cuando el archivo corresponde al layout de poda hoy soportado.
- `Valores_Raw` debe quedar solo para payload extra real como `AUXILIAR_Raw`, `Consumidor_Raw` o campos no estructurados del Excel.


## 9) Ajuste transitorio confirmado en Clima
El dominio de clima queda desacoplado de la geografia agronomica operativa.

Criterio consolidado:
- `Fact_Telemetria_Clima` usa `Sector_Climatico` directo.
- No intenta resolver `ID_Geografia` desde `Sector_Raw`.
- `Gold.Mart_Clima` agrega por `ID_Tiempo + Sector_Climatico`.

Implicancia operativa:
- Si el Excel de clima trae identificadores como `F07`, estos se cargan como sector climatico operativo.
- La futura `Dim_Estacion_Climatica` queda postergada; no forma parte de este ajuste minimo.

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

## Addendum 2026-04-01 - Fisiologia

### Estado operativo validado hoy
- `Fact_Fisiologia = 43900`
- `Bronce.Fisiologia.PROCESADO = 43900`
- `Bronce.Fisiologia.CARGADO = 1655`

### Criterio de operacion
1. Mantener baseline actual como referencia.
2. No reactivar regla por turno de `Modulo 11` sin catalogo completo.
3. Tratar `9.` como backlog controlado mientras negocio no cierre regla final.
4. No cerrar cambios de este frente sin rerun real y evidencia SQL.


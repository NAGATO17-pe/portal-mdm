# Contexto Maestro Para Otro Chat

Fecha de corte: 2026-04-14  
Proyecto: DWH Geographic Phenology ACP  
Ruta de trabajo: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL`

## Objetivo de este documento
Este archivo sirve para retomar continuidad técnica exacta en otro chat sin reiniciar análisis ni volver a abrir frentes ya cerrados. Resume el estado operativo real después del bloque de trabajo de 2026-04-13 y 2026-04-14, incluyendo atomicidad, Bronce Peladas, Maduración, auditoría, Gold y el frente abierto de geografía con submódulos.

## Estado general validado al cierre
- La suite `pytest tests -q` quedó verde en el baseline previo y se mantuvo como referencia de estabilidad.
- `py pipeline.py` volvió a correr completo después de los ajustes de Bronce Peladas y Maduración.
- Gold publica otra vez y los marts principales vuelven a refrescar sin el error de codificación de consola Windows.
- El refactor transaccional ya existe y ya está aplicado en varias facts críticas.
- Bronce Peladas ya admite el layout real del archivo `Peladas_V2.xlsx`, priorizando la hoja `BD_LT`.
- Maduración ya no reingiere histórico cuando no entra archivo nuevo en Bronce.
- Peladas ya persiste en cuarentena el motivo fino de todos sus rechazos, no solo el caso de muestras.

## Lo que ya no se debe reabrir sin evidencia nueva
1. El patrón de atomicidad compartida con `ContextoTransaccionalETL` ya quedó aprobado y es el camino correcto.
2. El bug real de `Fact_Evaluacion_Vegetativa` por `continue` mal indentado y por llamada vieja a `marcar_estado_carga` ya quedó cerrado.
3. `auditoria/log.py` ya quedó compatible con el esquema real desplegado de `Auditoria.Log_Carga`.
4. `gold/marts.py` ya no usa emojis y no debe volver al patrón anterior.
5. El layout real de Peladas ya no debe tratarse como un Excel genérico; el cargador debe seguir priorizando `BD_LT`.
6. Los submódulos `9.1`, `9.2`, `11.1`, `11.2`, `13.1`, `13.2`, `14.1`, `14.2` son válidos y no deben considerarse error de captura.
7. El SP `Silver.sp_Resolver_Geografia_Cama` ya interpreta correctamente los submódulos; el problema abierto no está en el parser del módulo.
8. `Bronce.Maduracion` ya fue normalizada para tener `Estado_Carga`; no debe volver a quedar sin esa columna.

## Estado del refactor transaccional
### Helpers base ya consolidados
- `utils/contexto_transaccional.py`
- `utils/sql_lotes.py`
- `dq/cuarentena.py`
- `mdm/homologador.py`

### Facts ya migradas al patrón nuevo
- `fact_evaluacion_pesos.py`
- `fact_tasa_crecimiento_brotes.py`
- `fact_telemetria_clima.py`
- `fact_conteo_fenologico.py`
- `fact_evaluacion_vegetativa.py`
- `fact_cosecha_sap.py`

### Facts con trabajo reciente, pero todavía sujetas a revisión operativa adicional
- `fact_maduracion.py`
- `fact_peladas.py`

### Facts aún pendientes de barrido equivalente
- `fact_tareo.py`
- `fact_fisiologia.py`
- `fact_induccion_floral.py`
- `fact_sanidad_activo.py`
- `fact_ciclo_poda.py`

## Correcciones principales ya aplicadas
### 1. Fact Evaluacion Vegetativa
Se cerraron dos bugs reales:
- filas inválidas por `Plantas_en_Floracion` quedaban rechazadas pero igual podían entrar al payload de insert;
- la llamada vieja a `contexto.marcar_estado_carga(engine, ...)` rompía la firma nueva del helper.

Resultado real validado:
- 23096 insertados
- 321 rechazados
- sin excepción

### 2. Auditoría
`auditoria/log.py` quedó endurecido para detectar columnas reales del esquema de `Auditoria.Log_Carga` y mapear alias de nombres de columna. El dashboard deja de depender de un nombre fijo que no coincidía con el despliegue real.

### 3. Gold
`gold/marts.py` quedó en ASCII puro. Se eliminó el uso de emojis en logs por el error `charmap codec can't encode`.

### 4. Bronce Peladas
El cargador ya no trata Peladas como layout genérico. Ahora:
- prioriza hoja `BD_LT`;
- si no existe, usa hoja `BD` filtrando `Tipo_Evaluacion = PELADAS`;
- proyecta columnas reales a la estructura física de `Bronce.Peladas`;
- guarda el resto del payload en `Valores_Raw`.

### 5. Archivo Excel bloqueado en Bronce
Se cerró el problema de `WinError 32` en Peladas:
- el ETL trabaja sobre una copia temporal del Excel;
- si Windows bloquea el archivo original al archivar, se guarda una copia en procesados o rechazados;
- el original queda marcado con un `.procesado.json` para que no se vuelva a tomar en la siguiente corrida.

### 6. Fact Peladas
Se adaptó al layout real:
- usa `Turno_Raw`, `Valvula_Raw`, `Evaluador_Raw` y `Valores_Raw`;
- resuelve geografía con módulo, turno y válvula;
- usa el payload serializado cuando faltan columnas físicas.

Además, quedó corregido el punto más importante de trazabilidad:
- antes rechazaba varias filas sin dejar motivo persistido;
- ahora registra en cuarentena todos los rechazos con `columna`, `valor`, `motivo`, `tipo_regla` e `id_registro_origen`.

Motivos cubiertos explícitamente:
- fecha inválida o fuera de campaña;
- geografía no resuelta;
- variedad sin match en dimensión;
- muestras inválidas.

### 7. Fact Maduración
Se mejoró el rendimiento y la visibilidad:
- pasó de `INSERT` por fila a inserción en lotes;
- ahora imprime progreso periódico en consola;
- usa caches locales para geografía, cinta, tiempo y personal;
- sigue dentro de la transacción compartida.

### 8. Bronce Maduración
Se detectó que el reproceso histórico ocurría porque `Bronce.Maduracion` no tenía `Estado_Carga`.  
Se dejó un script de normalización que:
- agrega la columna si no existe;
- crea default `CARGADO`;
- cierra histórico actual como `PROCESADO`;
- deja la columna `NOT NULL`.

El histórico quedó validado en:
- `PROCESADO = 26800`

## Estado operativo de submódulos y geografía
### Lo que está bien
- `MDM.Regla_Modulo_Raw` contiene reglas activas para `9.1`, `9.2`, `11.1`, `11.2`, `13.1`, `13.2`, `14.1`, `14.2`.
- `MDM.Regla_Modulo_Turno_SubModulo` contiene reglas activas por turno para `9.`.
- `Silver.sp_Resolver_Geografia_Cama` interpreta correctamente `Modulo_Int` y `SubModulo_Int`.
- Los rechazos de Peladas ya muestran que el ETL está interpretando submódulo de forma correcta.

### Lo que sigue abierto
El problema actual ya no es de parseo ni de regla MDM. El problema real está en `Silver.Dim_Geografia`.

Hallazgo técnico consolidado:
- `Dim_Geografia` sigue mostrando registros vigentes con `SubModulo = NULL` donde ya deberían existir combinaciones submódulo, turno y válvula;
- por eso el resolvedor oficial encuentra regla, pero no encuentra coincidencia materializada en dimensión;
- el backlog actual de Peladas no debe resolverse inventando geografía sin acuerdo de negocio.

### Decisión vigente
Este frente queda pausado hasta revisión con equipo.

No se debe avanzar aún con:
- backfill masivo de `Dim_Geografia`;
- inserciones manuales de submódulo;
- reinterpretación automática de submódulos.

Primero se debe cerrar con el equipo:
1. la clave geográfica oficial exacta;
2. cuándo existe `SubModulo` y cuándo no;
3. cómo se materializa eso en `Dim_Geografia`;
4. cuándo entra `Cama` como parte de la clave operativa.

## Situación específica de Peladas al cierre
- El cargador de Bronce ya está cerrado.
- El fact ya deja motivos detallados.
- Los submódulos no son error.
- Los rechazos actuales de Peladas son evidencia útil para definir la geografía correcta.

Interpretación correcta:
- si rechaza `9.1`, `9.2`, `11.1`, `11.2`, no es porque el ETL esté mal;
- rechaza porque `Dim_Geografia` todavía no tiene la forma operativa completa para esas combinaciones.

## Validaciones y pruebas ya tocadas
### Nuevos tests o ajustes recientes
- `tests/test_bronce_cargador_peladas.py`
- `tests/test_fact_peladas.py`
- `tests/test_bronce_archivo_bloqueado.py`

### Qué cubren
- priorización de `BD_LT`;
- mapeo correcto del layout real de Peladas;
- uso de `Turno` y `Valvula` desde el payload;
- persistencia de motivos detallados en rechazos de Peladas;
- fallback correcto cuando el Excel original está bloqueado.

## Riesgos abiertos reales
1. La definición final de geografía para submódulos sigue pendiente de negocio y operación.
2. Aún falta barrido homogéneo de atomicidad y trazabilidad en facts no migradas completamente.
3. `Fact_Peladas` y `Fact_Maduracion` mejoraron fuerte, pero todavía conviene tratarlas como frentes recientes bajo observación operativa.

## Próximo paso único recomendado
Cuando el equipo retome el frente de geografía, continuar solo en este orden:
1. acordar regla operativa exacta de `Dim_Geografia`;
2. preparar script de carga o backfill de geografía;
3. reprocesar rechazados de Peladas;
4. validar caída del backlog en cuarentena;
5. seguir con el barrido de atomicidad en facts pendientes.


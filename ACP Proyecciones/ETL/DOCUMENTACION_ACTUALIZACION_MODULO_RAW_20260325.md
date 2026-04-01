# Actualizacion Tecnica - Modulo_Raw, SubModulo y Cama (corte 2026-03-30)

## 1) Problema de negocio que corrige
- `9.1`, `9.2`, `11.1`, `11.2`, `13.1`, `13.2`, `14.1` y `14.2` no son errores de captura: representan conducciones distintas.
- `VI` corresponde a Test Block.
- Los archivos reales tambien llegan con ruido tipografico en geografia:
  - `MODULO 2`
  - `TURNO 04`
  - `NROVALVULA 15`
- Sin reglas + normalizacion global, esos casos caen como `caso especial` o `geografia no encontrada`, elevando cuarentena y frenando facts.

## 2) Diseno vigente
Se consolida una capa de normalizacion en MDM:
- Tabla: `MDM.Regla_Modulo_Raw`
- Uso: convertir `Modulo_Raw` a claves operativas de geografia.

Reglas canonicas activas:
- `9.1  -> Modulo=9,  SubModulo=1, Tipo_Conduccion='SUELO'`
- `9.2  -> Modulo=9,  SubModulo=2, Tipo_Conduccion='MACETA'`
- `11.1 -> Modulo=11, SubModulo=1, Tipo_Conduccion='SUELO'`
- `11.2 -> Modulo=11, SubModulo=2, Tipo_Conduccion='MACETA'`
- `13.1 -> Modulo=13, SubModulo=1, Tipo_Conduccion='SUELO'`
- `13.2 -> Modulo=13, SubModulo=2, Tipo_Conduccion='MACETA'`
- `14.1 -> Modulo=14, SubModulo=1, Tipo_Conduccion='SUELO'`
- `14.2 -> Modulo=14, SubModulo=2, Tipo_Conduccion='MACETA'`
- `VI   -> Es_Test_Block=1`

Regla de negocio no negociable:
- `9.` sin submodulo se queda en cuarentena.

## 3) Cambios de modelo de datos
Columnas relevantes ya consideradas en el modelo:
- `MDM.Catalogo_Geografia.SubModulo`
- `MDM.Catalogo_Geografia.Tipo_Conduccion`
- `Silver.Dim_Geografia.SubModulo`
- `Silver.Dim_Geografia.Tipo_Conduccion`

## 4) Cambios de logica en SQL
Scripts clave:
- `fase12_regla_modulo_raw_y_sp.sql`
- `fase17_reglas_modulo_raw_11_13_14.sql`

Procedimientos afectados:
- `Silver.sp_Resolver_Geografia_Cama`
- `Silver.sp_Upsert_Cama_Desde_Bronce`
- `Silver.sp_Validar_Calidad_Camas`

Puntos clave:
- La resolucion geografica usa `(Modulo, SubModulo, Turno, Valvula)`.
- Rango de cama operativo: `1..100`.
- `Cama=0` se considera sin cama explicita; resuelve geografia base, no relacion cama-geografia.

## 5) Cambios de logica en Python
### Normalizacion global de geografia
Se incorporo saneamiento comun antes del resolvedor:
- `MODULO 2 -> 2`
- `TURNO 04 -> 4`
- `NROVALVULA 15 -> 15`
- `Valvula=57 -> 57`
- `9.1 -> 9.1`
- `VI -> VI`

Archivos impactados:
- `utils/texto.py`
- `mdm/lookup.py`
- loaders que consumen `obtener_id_geografia()`

### Comportamiento esperado
- El ETL deja de depender de la forma exacta en que venga escrito `Modulo/Turno/Valvula`.
- El match geografico se vuelve reutilizable para todos los facts.

## 6) Operacion de cama
Regla vigente:
- `SP_Cama` no corre para cualquier archivo.
- Solo corre si en la corrida ingresaron `Bronce.Evaluacion_Pesos` o `Bronce.Evaluacion_Vegetativa`.

Control obligatorio:
- Si `SP_Cama aptas > 0`, entonces `Bridge_Geografia_Cama > 0`.
- Si no se cumple, la corrida entra en `RIESGO`.

## 7) Orden recomendado de ejecucion
1. Validar reglas activas en `MDM.Regla_Modulo_Raw`.
2. Ejecutar `py "D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\pipeline.py"`.
3. Validar:
```sql
EXEC Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida=100,
    @Max_Camas_Por_Geografia=100;
```
4. Si hubo fuentes con cama, verificar:
```sql
SELECT COUNT(*) AS Bridge_Geografia_Cama
FROM Silver.Bridge_Geografia_Cama;
```

## 8) Resultado esperado
- Menos rechazos por `GEOGRAFIA_NO_ENCONTRADA` asociados a variantes tipograficas.
- Separacion explicita por `SubModulo` y `Tipo_Conduccion`.
- Menor cuarentena repetitiva por ruido operacional.
- `VI` controlado como Test Block sin mezclarlo con geografia operativa.

## 9) Addendum 2026-04-01 - Regla por Turno y Baseline Real

- Baseline real validado de `Fisiologia`: `43900` insertados / `1655` pendientes.
- La regla por turno de `Modulo 11` se probo y quedo desactivada por regresion real contra catalogo incompleto.
- La regla final de `9.` sigue pendiente; no debe declararse cerrada todavia.
- Mantener saneamiento tipografico en Python como fuente principal de limpieza antes del resolvedor.

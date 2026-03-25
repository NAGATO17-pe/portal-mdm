# Actualizacion Tecnica - Modulo_Raw, SubModulo y Cama (2026-03-25)

## 1) Problema de negocio que corrige
- `9.1` y `9.2` no son errores de captura: representan conducciones distintas.
- `VI` corresponde a Test_Block.
- Sin una tabla de reglas, esos casos caen como "caso especial" o "geografia no encontrada", elevando cuarentena y frenando carga de facts.

## 2) Diseno aplicado
Se incorpora una capa de normalizacion en MDM:
- Tabla: `MDM.Regla_Modulo_Raw`
- Uso: convertir `Modulo_Raw` a claves operativas de geografia.

Reglas semilla:
- `9.1 -> Modulo=9, SubModulo=1, Tipo_Conduccion='SUELO', Es_Test_Block=0`
- `9.2 -> Modulo=9, SubModulo=2, Tipo_Conduccion='MACETA', Es_Test_Block=0`
- `VI  -> Es_Test_Block=1` (queda como caso especial controlado)

## 3) Cambios de modelo de datos
Se agregan columnas:
- `MDM.Catalogo_Geografia.SubModulo`
- `MDM.Catalogo_Geografia.Tipo_Conduccion`
- `Silver.Dim_Geografia.SubModulo`
- `Silver.Dim_Geografia.Tipo_Conduccion`

## 4) Cambios de logica en SQL
Script nuevo:
- `fase12_regla_modulo_raw_y_sp.sql`

Procedimientos actualizados:
- `Silver.sp_Resolver_Geografia_Cama`
- `Silver.sp_Upsert_Cama_Desde_Bronce`
- `Silver.sp_Validar_Calidad_Camas`

Puntos clave:
- Resolucion geografia ahora usa `(Modulo, SubModulo, Turno, Valvula)`.
- Rango de cama operativo: `1..100`.
- `Cama=0` se considera sin cama especifica (no valida para relacion cama-geografia).

## 5) Cambios de logica en Python
Archivo actualizado:
- `dim_geografia.py`

Comportamiento:
- Sincroniza `Silver.Dim_Geografia` desde `MDM.Catalogo_Geografia`.
- Aplica reglas de `MDM.Regla_Modulo_Raw` antes de insertar.
- Ejecuta SCD2 cuando hay cambio en atributos no-clave.
- Omite registros con modulo no resoluble (evita contaminacion).

Archivo actualizado:
- `pipeline.py`

Comportamiento:
- Ejecuta SP de cama con parametros `1..100`.
- Ejecuta validacion de calidad de cama con limite `100`.

## 6) Orden recomendado de ejecucion
1. Ejecutar `fase12_regla_modulo_raw_y_sp.sql` en SSMS.
2. Ejecutar limpieza operativa de facts/bronze/cuarentena (si corresponde a prueba limpia).
3. Ejecutar `py pipeline.py`.
4. Validar:
```sql
EXEC Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida=100,
    @Max_Camas_Por_Geografia=100;
```

## 7) Resultado esperado
- Menos rechazos por `GEOGRAFIA_NO_ENCONTRADA` asociados a `9.1` y `9.2`.
- Menor volumen de cuarentena repetitiva.
- Mejor trazabilidad para prediccion futura (separacion explicita suelo vs maceta).
- `VI` controlado como caso especial de test block, sin mezclarlo con geografia operativa.

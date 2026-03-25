# README Operativo - ETL ACP (estado estable)

## 1) Objetivo
Operacion oficial del ETL con:
- Resolucion geografica por SP.
- Gobernanza de cama por catalogo y bridge.
- Normalizacion de `Modulo_Raw` via reglas MDM.

## 2) Archivos operativos (raiz)
- `pipeline.py`
- `lookup.py`
- `fact_evaluacion_pesos.py`
- `fact_evaluacion_vegetativa.py`
- `fase10_diagnostico_calidad_camas.sql`
- `fase11_sp_geografia_cama.sql`
- `fase12_regla_modulo_raw_y_sp.sql`
- `fase11_limpieza_controlada_legacy.sql`

Archivos historicos/descartados:
- `legacy_no_usar/`

## 3) Secuencia diaria recomendada
1. Ejecutar `py pipeline.py`.
2. Revisar resumen final en consola.
3. Validar semaforo de camas:
   - `EXEC Silver.sp_Validar_Calidad_Camas @Cama_Max_Permitida=100, @Max_Camas_Por_Geografia=100;`
4. Si estado <> `OK_OPERATIVO`, detener publicacion de Gold.

## 4) Reglas activas de cama y modulo
- Catalogo permitido de cama: `1..100`.
- `Cama_Raw` nula/vacia/0: se resuelve solo geografia base.
- `Cama_Raw` fuera de `1..100`: `CAMA_NO_VALIDA`.
- `Modulo_Raw`:
  - `9.1 -> Modulo=9, SubModulo=1, Tipo_Conduccion='SUELO'`
  - `9.2 -> Modulo=9, SubModulo=2, Tipo_Conduccion='MACETA'`
  - `VI -> CASO_ESPECIAL_MODULO (test block)`

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
- Reduccion sostenida de cuarentena geografia/cama.

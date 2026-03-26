# README Operativo - ETL ACP (estado estable)

## 1) Objetivo
Operacion oficial del ETL con:
- Resolucion geografica por SP.
- Gobernanza de cama por catalogo y bridge.
- Normalizacion de `Modulo_Raw` via reglas MDM.
- Carga de `Maduracion` como base del analisis Six Week.

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
3. Confirmar contexto SQL del resumen:
   - `Servidor SQL`
   - `Base SQL`
4. Validar semaforo de camas:
   - `EXEC Silver.sp_Validar_Calidad_Camas @Cama_Max_Permitida=100, @Max_Camas_Por_Geografia=100;`
5. Verificar bridge:
   - `SP_Cama aptas > 0` y `Bridge camas despues > 0`
6. Si estado <> `OK_OPERATIVO`, o si hay aptas y el bridge queda en `0`, detener publicacion de Gold.
7. Si `Fact_Maduracion` rechaza filas, revisar `MDM.Cuarentena` con foco en:
   - `Cinta no reconocida o ausente en maduracion`
   - `No se encontraron mediciones de maduracion con semana 1..6 en Valores_Raw`

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

## 7) Incidentes operativos que ya no se deben mezclar
- `LAYOUT_INCOMPATIBLE`: archivo mal ubicado o con estructura no soportada. Se mueve a `data/rechazados/<carpeta>/`.
- `rechazados` de un fact: problema funcional de mapeo/DQ del loader correspondiente.
- `backlog historico CARGADO`: lote viejo pendiente que puede distorsionar el resumen del dia si no se acota al lote actual.
- `bridge en 0`: inconsistencia del paso 6; no publicar Gold aunque la calidad de camas salga `OK_OPERATIVO`.
- `maduracion sin parser`: ausencia de `Cinta` o de semanas `1..6` en `Valores_Raw`; no es fallo de `Conteo_Fruta`.

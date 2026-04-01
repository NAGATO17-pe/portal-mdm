# GUIA REVISION MANUAL FASE 16 - corte operativo 2026-03-30

## Objetivo de esta guia
Que puedas verificar manualmente, sin depender solo del pipeline log, que el estado operativo vigente sigue sano.

Piensalo como revisar un auto despues de mantenimiento:
1. Arranca (facts cargan > 0).
2. No prende luces rojas (calidad cama OK).
3. Frenos responden (VI smoke tests correctos).
4. Quedan pendientes reales identificados (residual claro).

## Paso 0 - Preparacion
En SSMS usa la base:
```sql
USE ACP_DataWarehose_Proyecciones;
```

## Paso 1 - Verificar que los facts criticos cargaron
```sql
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

SELECT 'Silver.Fact_Evaluacion_Pesos' AS Tabla,
       COUNT(*) AS Filas_Hoy,
       MAX(Fecha_Sistema) AS Ultimo_Registro
FROM Silver.Fact_Evaluacion_Pesos
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Silver.Fact_Evaluacion_Vegetativa',
       COUNT(*),
       MAX(Fecha_Sistema)
FROM Silver.Fact_Evaluacion_Vegetativa
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Silver.Fact_Maduracion',
       COUNT(*),
       MAX(Fecha_Sistema)
FROM Silver.Fact_Maduracion
WHERE CAST(Fecha_Sistema AS DATE) = @hoy;
```
Esperado de referencia:
- Pesos: `5658`
- Vegetativa: `22872`
- Maduracion: `> 0` solo si hubo lote valido y loader alineado

## Paso 2 - Verificar VI como Test Block
```sql
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='0';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='1';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='2';
```
Esperado:
- Los 3 casos con `Estado_Resolucion = RESUELTA_TEST_BLOCK`.

## Paso 3 - Verificar salud operativa de cama
```sql
EXEC Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida = 100,
    @Max_Camas_Por_Geografia = 100;
```
Esperado:
- `Estado_Calidad_Cama = OK_OPERATIVO`
- `Cama_Fuera_Regla = 0`
- `Geografias_Saturadas = 0`

## Paso 4 - Verificar bridge y trazabilidad
### 4.1 Bridge de camas
```sql
SELECT COUNT(*) AS Bridge_Geografia_Cama
FROM Silver.Bridge_Geografia_Cama;
```
Esperado de corrida limpia con cama:
- `Bridge_Geografia_Cama = 3866`

Si la corrida del dia no tuvo `Evaluacion_Pesos` ni `Evaluacion_Vegetativa`, el paso 6 del pipeline puede quedar como `OMITIDO`.

### 4.2 Trazabilidad por ID en cuarentena nueva
```sql
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

SELECT
    Tabla_Origen,
    COUNT(*) AS Nuevas_Cuarentenas_Hoy,
    SUM(CASE WHEN ID_Registro_Origen IS NOT NULL THEN 1 ELSE 0 END) AS Con_ID_Registro_Origen,
    CAST(100.0 * SUM(CASE WHEN ID_Registro_Origen IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS DECIMAL(5,2)) AS Pct_Con_ID
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen
ORDER BY Tabla_Origen;
```
Esperado:
- Cobertura alta, idealmente `>= 98%`

## Paso 5 - Ver residual real
```sql
SELECT
    Tabla_Origen,
    Motivo,
    COUNT(*) AS Filas_Pendientes
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
  AND Tabla_Origen IN ('Bronce.Conteo_Fruta','Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa','Bronce.Maduracion')
GROUP BY Tabla_Origen, Motivo
ORDER BY Tabla_Origen, Filas_Pendientes DESC;
```
Esperado de referencia:
- Pesos total rechazado: `14`
  - Geografia especial/catalogacion: `8`
  - Peso no calculable: `3`
  - Fuera de rango: `3`
- Vegetativa total rechazado: `545`
  - Geografia no encontrada: `455`
  - Floracion invalida: `75`
  - Plantas evaluadas invalida: `13`
  - Fecha invalida: `2`
- Conteo total rechazado: `67`
  - Geografia no encontrada: `66`
  - Cinta no reconocida: `1`

## Paso 6 - Verificar KPI geografia puntual
```sql
SELECT
    Tabla_Origen,
    SUM(CASE WHEN Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.' THEN 1 ELSE 0 END) AS KPI_Geografia_Especial,
    SUM(CASE WHEN Motivo = N'Geografia no encontrada en Silver.Dim_Geografia.' THEN 1 ELSE 0 END) AS KPI_Geografia_No_Encontrada,
    SUM(CASE WHEN Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
              AND Valor_Recibido LIKE 'Modulo=9.%' THEN 1 ELSE 0 END) AS KPI_Modulo_9_Punto
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen
ORDER BY Tabla_Origen;
```
Esperado de referencia:
- Pesos: `KPI_Geografia_Especial=8`, `KPI_Modulo_9_Punto=8`
- Vegetativa: `KPI_Geografia_No_Encontrada=455`

## Paso 7 - Verificar Maduracion solo si hubo lote
```sql
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

SELECT COUNT(*) AS Fact_Maduracion_Hoy,
       MAX(Fecha_Sistema) AS Ultimo_Registro
FROM Silver.Fact_Maduracion
WHERE CAST(Fecha_Sistema AS DATE) = @hoy;
```
Interpretacion:
- Si hubo carga en `Bronce.Maduracion`, el fact debe cargar o dejar cuarentena accionable.
- Si falla, revisar primero:
  - `ID_Organo`
  - `DESCRIPCIONESTADOCICLO_Raw`
  - `COLOR_Raw`
  - geografia resuelta por `Modulo + Turno + Valvula`

## Check final de aceptacion
1. Pesos > 0 hoy: SI
2. Vegetativa > 0 hoy: SI
3. VI resuelve 0/1/2: SI
4. Calidad cama OK_OPERATIVO: SI
5. Si hubo aptas, bridge > 0: SI
6. % con ID origen alto y coherente: SI
7. Si hubo lote de Maduracion, el comportamiento es coherente: SI

Si los semaforos criticos son SI, la operacion sigue estable.

## Addendum 2026-03-30 - Clima, Tareo y Regla de Campana

## Paso adicional - Validar clima historico
```sql
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

SELECT
    Campo_Origen,
    Motivo,
    COUNT(*) AS Filas
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen = 'Bronce.Clima'
GROUP BY Campo_Origen, Motivo
ORDER BY Filas DESC, Campo_Origen;
```

Interpretacion vigente:
- Si el residual se concentra en `Fecha invalida o fuera de campana`, confirmar si el lote contiene historico meteorologico previo a `2025-03-01`.
- No tratarlo como falla estructural del loader si `Bronce.Reporte_Clima` ya muestra `Fecha_Raw`, `Hora_Raw`, `Sector_Raw`, `TempMax_Raw`, `TempMin_Raw` y `Humedad_Raw` correctamente poblados.
- `Fact_Telemetria_Clima` opera con `Sector_Climatico`; no depende de `Dim_Geografia`.

## Nota sobre Tareo
- `Fact_Tareo` sigue pendiente por ausencia de `Fundo/Modulo` en la fuente actual.
- Si el archivo no trae geografia resoluble, el estado correcto es `diagnosticado y pendiente`, no `bug oculto`.

## Addendum 2026-04-01 - Revision de Fisiologia

Usar como referencia operativa:
- baseline sano: `43900`
- residual controlado: `1655`
- foco residual: `Modulo_Raw = '9.'`

Si reaparece residual fuerte fuera de `9.`:
1. revisar reglas activas de modulo;
2. revisar catalogo geográfico y `SubModulo`;
3. comparar contra baseline real antes de abrir nuevo parche.


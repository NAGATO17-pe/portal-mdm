# GUIA REVISION MANUAL FASE 16 - 2026-03-25

## Objetivo de esta guia
Que puedas verificar manualmente, sin depender del pipeline log, que los avances de hoy estan correctos.

Piensalo como revisar un auto despues de mantenimiento:
1. Arranca (facts cargan > 0).
2. No prende luces rojas (calidad cama OK).
3. Frenos responden (VI smoke tests correctos).
4. Quedan pendientes menores identificados (residual claro).

## Paso 0 - Preparacion
En SSMS usa la base:
```sql
USE ACP_DataWarehose_Proyecciones;
```

## Paso 1 - Verificar que los dos facts cargaron (criterio minimo)
```sql
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

SELECT 'Silver.Fact_Evaluacion_Pesos' AS Tabla,
       COUNT(*) AS Filas_Hoy,
       MAX(Fecha_Sistema) AS Ultimo_Registro
FROM Silver.Fact_Evaluacion_Pesos
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Silver.Fact_Evaluacion_Vegetativa' AS Tabla,
       COUNT(*) AS Filas_Hoy,
       MAX(Fecha_Sistema) AS Ultimo_Registro
FROM Silver.Fact_Evaluacion_Vegetativa
WHERE CAST(Fecha_Sistema AS DATE) = @hoy;
```
Esperado hoy:
- Pesos: 5658
- Vegetativa: 22833

Ejemplo cotidiano:
- Es como contar cuantas cajas llegaron hoy a almacen. Si llega 0, la ruta fallo.

## Paso 2 - Verificar VI como Test Block
```sql
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='0';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='1';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='2';
```
Esperado hoy:
- Los 3 casos con `Estado_Resolucion = RESUELTA_TEST_BLOCK`.

Ejemplo cotidiano:
- Es validar que una llave especial abre siempre la puerta VIP, sin importar el asiento.

## Paso 3 - Verificar salud operativa de cama
```sql
EXEC Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida = 100,
    @Max_Camas_Por_Geografia = 100;
```
Esperado hoy:
- `Estado_Calidad_Cama = OK_OPERATIVO`
- `Cama_Fuera_Regla = 0`
- `Geografias_Saturadas = 0`

Ejemplo cotidiano:
- Como semaforo de planta: verde para producir, rojo para parar.

## Paso 4 - Verificar trazabilidad por ID en cuarentena nueva
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
Esperado hoy:
- Pesos: 14/14 (100%)
- Vegetativa: 584/584 (100%)

Ejemplo cotidiano:
- Cada reclamo debe tener numero de ticket. Sin ticket no hay seguimiento serio.

## Paso 5 - Ver residual real (donde atacar manana)
```sql
SELECT
    Tabla_Origen,
    Motivo,
    COUNT(*) AS Filas_Pendientes
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen, Motivo
ORDER BY Tabla_Origen, Filas_Pendientes DESC;
```
Esperado hoy:
- Pesos total rechazado: 14
  - Geografia especial: 8
  - Peso no calculable: 3
  - Fuera de rango: 3
- Vegetativa total rechazado: 584
  - Geografia no encontrada: 497
  - Floracion invalida: 72
  - Plantas evaluadas invalida: 13
  - Fecha invalida: 2

Ejemplo cotidiano:
- Es como clasificar devoluciones de una tienda por causa. El mayor lote te dice donde actuar primero.

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
Esperado hoy:
- Pesos: `KPI_Geografia_Especial=8`, `KPI_Modulo_9_Punto=8`
- Vegetativa: `KPI_Geografia_No_Encontrada=497`

## Check final de aceptacion (si/no)
1. Pesos > 0 hoy: SI
2. Vegetativa > 0 hoy: SI
3. VI resuelve 0/1/2: SI
4. Calidad cama OK_OPERATIVO: SI
5. % con ID origen alto y coherente: SI (100%)

Si los 5 son SI, la fase esta estable y congelada.

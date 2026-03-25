/*
Fase 16 - Limpieza controlada de hechos del dia (baseline limpio)
Fecha objetivo: hoy (SYSDATETIME)

Alcance:
- Silver.Fact_Evaluacion_Pesos (solo filas de hoy)
- Silver.Fact_Evaluacion_Vegetativa (solo filas de hoy)
- MDM.Cuarentena (solo hoy y solo tablas Bronce.Evaluacion_Pesos/Vegetativa)
- Auditoria.Log_Carga (solo hoy y solo logs de ambos facts)

No toca historico de dias anteriores.
No toca esquema ni SPs.
*/

SET NOCOUNT ON;
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

/* Pre-check */
SELECT 'Precheck_Fact_Pesos_Hoy' AS Control, COUNT(*) AS Filas
FROM Silver.Fact_Evaluacion_Pesos
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Precheck_Fact_Vegetativa_Hoy', COUNT(*)
FROM Silver.Fact_Evaluacion_Vegetativa
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Precheck_Cuarentena_Hoy_Pesos', COUNT(*)
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen = 'Bronce.Evaluacion_Pesos'
UNION ALL
SELECT 'Precheck_Cuarentena_Hoy_Vegetativa', COUNT(*)
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
UNION ALL
SELECT 'Precheck_LogCarga_Hoy_Pesos', COUNT(*)
FROM Auditoria.Log_Carga
WHERE CAST(Fecha_Inicio AS DATE) = @hoy
  AND Tabla_Destino = 'Silver.Fact_Evaluacion_Pesos'
UNION ALL
SELECT 'Precheck_LogCarga_Hoy_Vegetativa', COUNT(*)
FROM Auditoria.Log_Carga
WHERE CAST(Fecha_Inicio AS DATE) = @hoy
  AND Tabla_Destino = 'Silver.Fact_Evaluacion_Vegetativa';

/* Limpieza */
DELETE FROM Silver.Fact_Evaluacion_Pesos
WHERE CAST(Fecha_Sistema AS DATE) = @hoy;
DECLARE @del_fact_pesos INT = @@ROWCOUNT;

DELETE FROM Silver.Fact_Evaluacion_Vegetativa
WHERE CAST(Fecha_Sistema AS DATE) = @hoy;
DECLARE @del_fact_veg INT = @@ROWCOUNT;

DELETE FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa');
DECLARE @del_cuar INT = @@ROWCOUNT;

DELETE FROM Auditoria.Log_Carga
WHERE CAST(Fecha_Inicio AS DATE) = @hoy
  AND Tabla_Destino IN ('Silver.Fact_Evaluacion_Pesos','Silver.Fact_Evaluacion_Vegetativa');
DECLARE @del_log INT = @@ROWCOUNT;

/* Post-check */
SELECT
    @del_fact_pesos AS Filas_Borradas_Fact_Pesos,
    @del_fact_veg AS Filas_Borradas_Fact_Vegetativa,
    @del_cuar AS Filas_Borradas_Cuarentena,
    @del_log AS Filas_Borradas_Log_Carga;

SELECT 'Postcheck_Fact_Pesos_Hoy' AS Control, COUNT(*) AS Filas
FROM Silver.Fact_Evaluacion_Pesos
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Postcheck_Fact_Vegetativa_Hoy', COUNT(*)
FROM Silver.Fact_Evaluacion_Vegetativa
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Postcheck_Cuarentena_Hoy_Pesos', COUNT(*)
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen = 'Bronce.Evaluacion_Pesos'
UNION ALL
SELECT 'Postcheck_Cuarentena_Hoy_Vegetativa', COUNT(*)
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
UNION ALL
SELECT 'Postcheck_LogCarga_Hoy_Pesos', COUNT(*)
FROM Auditoria.Log_Carga
WHERE CAST(Fecha_Inicio AS DATE) = @hoy
  AND Tabla_Destino = 'Silver.Fact_Evaluacion_Pesos'
UNION ALL
SELECT 'Postcheck_LogCarga_Hoy_Vegetativa', COUNT(*)
FROM Auditoria.Log_Carga
WHERE CAST(Fecha_Inicio AS DATE) = @hoy
  AND Tabla_Destino = 'Silver.Fact_Evaluacion_Vegetativa';

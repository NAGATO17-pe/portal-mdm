/*
Fase 16 - Snapshot Baseline Post-Corrida (2026-03-25)
Ejecutar despues de:
1) fase16_limpieza_hechos_hoy.sql
2) py pipeline.py
*/

SET NOCOUNT ON;
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

/* 1) Carga de hoy por fact */
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

/* 2) Cuarentena pendiente por motivo (dos tablas objetivo) */
SELECT
    Tabla_Origen,
    Motivo,
    COUNT(*) AS Filas_Pendientes
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen, Motivo
ORDER BY Tabla_Origen, Filas_Pendientes DESC;

/* 3) KPI Geografia */
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

/* 4) Check estructural: nuevas cuarentenas de hoy con ID origen */
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

/* 5) Smoke tests VI */
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='0';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='1';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='2';

/* 6) Calidad cama */
EXEC Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida = 100,
    @Max_Camas_Por_Geografia = 100;

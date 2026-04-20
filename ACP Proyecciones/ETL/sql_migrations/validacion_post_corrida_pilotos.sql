SET NOCOUNT ON;

PRINT '============================================================';
PRINT 'VALIDACION POST CORRIDA — TABLAS PILOTO';
PRINT '============================================================';

PRINT '--- Bronce.Evaluacion_Pesos ---';
SELECT Estado_Carga, COUNT(*) AS Filas
FROM Bronce.Evaluacion_Pesos
GROUP BY Estado_Carga;

PRINT '--- Silver.Fact_Evaluacion_Pesos ---';
SELECT COUNT(*) AS Filas_Fact_Pesos
FROM Silver.Fact_Evaluacion_Pesos;

PRINT '--- Cuarentena Pesos ---';
SELECT TOP 20
    Campo_Origen,
    Valor_Recibido,
    Motivo,
    Estado,
    Fecha_Ingreso
FROM MDM.Cuarentena
WHERE Tabla_Origen = 'Bronce.Evaluacion_Pesos'
ORDER BY Fecha_Ingreso DESC;

PRINT '--- Bronce.Evaluacion_Vegetativa ---';
SELECT Estado_Carga, COUNT(*) AS Filas
FROM Bronce.Evaluacion_Vegetativa
GROUP BY Estado_Carga;

PRINT '--- Silver.Fact_Evaluacion_Vegetativa ---';
SELECT COUNT(*) AS Filas_Fact_Vegetativa
FROM Silver.Fact_Evaluacion_Vegetativa;

PRINT '--- Cuarentena Vegetativa ---';
SELECT TOP 20
    Campo_Origen,
    Valor_Recibido,
    Motivo,
    Estado,
    Fecha_Ingreso
FROM MDM.Cuarentena
WHERE Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
ORDER BY Fecha_Ingreso DESC;

PRINT '--- Bronce.Maduracion ---';
SELECT Estado_Carga, COUNT(*) AS Filas
FROM Bronce.Maduracion
GROUP BY Estado_Carga;

PRINT '--- Silver.Fact_Maduracion ---';
SELECT COUNT(*) AS Filas_Fact_Maduracion
FROM Silver.Fact_Maduracion;

PRINT '--- Cuarentena Maduracion ---';
SELECT TOP 20
    Campo_Origen,
    Valor_Recibido,
    Motivo,
    Estado,
    Fecha_Ingreso
FROM MDM.Cuarentena
WHERE Tabla_Origen = 'Bronce.Maduracion'
ORDER BY Fecha_Ingreso DESC;

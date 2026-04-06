SET NOCOUNT ON;

PRINT '=== Contexto SQL ===';
SELECT
    @@SERVERNAME AS servidor_sql,
    DB_NAME() AS base_sql,
    SUSER_SNAME() AS usuario_sql,
    SYSDATETIME() AS fecha_sql;

PRINT '=== Conteos Silver ===';
SELECT 'Silver.Dim_Geografia' AS tabla, COUNT(*) AS total FROM Silver.Dim_Geografia
UNION ALL SELECT 'Silver.Dim_Personal', COUNT(*) FROM Silver.Dim_Personal
UNION ALL SELECT 'Silver.Fact_Conteo_Fenologico', COUNT(*) FROM Silver.Fact_Conteo_Fenologico
UNION ALL SELECT 'Silver.Fact_Evaluacion_Pesos', COUNT(*) FROM Silver.Fact_Evaluacion_Pesos
UNION ALL SELECT 'Silver.Fact_Evaluacion_Vegetativa', COUNT(*) FROM Silver.Fact_Evaluacion_Vegetativa
UNION ALL SELECT 'Silver.Fact_Ciclo_Poda', COUNT(*) FROM Silver.Fact_Ciclo_Poda
UNION ALL SELECT 'Silver.Fact_Maduracion', COUNT(*) FROM Silver.Fact_Maduracion
UNION ALL SELECT 'Silver.Fact_Telemetria_Clima', COUNT(*) FROM Silver.Fact_Telemetria_Clima
UNION ALL SELECT 'Silver.Fact_Induccion_Floral', COUNT(*) FROM Silver.Fact_Induccion_Floral
UNION ALL SELECT 'Silver.Fact_Tasa_Crecimiento_Brotes', COUNT(*) FROM Silver.Fact_Tasa_Crecimiento_Brotes
UNION ALL SELECT 'Silver.Fact_Tareo', COUNT(*) FROM Silver.Fact_Tareo
UNION ALL SELECT 'Silver.Fact_Fisiologia', COUNT(*) FROM Silver.Fact_Fisiologia
ORDER BY tabla;

PRINT '=== Cuarentena pendiente por Tabla_Origen ===';
SELECT Tabla_Origen, COUNT(*) AS total_pendiente
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
GROUP BY Tabla_Origen
ORDER BY total_pendiente DESC, Tabla_Origen;

PRINT '=== Semáforo geografía / cama ===';
EXEC Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida = 100,
    @Max_Camas_Por_Geografia = 100;

SELECT COUNT(*) AS bridge_geografia_cama
FROM Silver.Bridge_Geografia_Cama;

PRINT '=== Residual Modulo_Raw = 9. ===';
SELECT Tabla, Total
FROM (
    SELECT 'Bronce.Fisiologia' AS Tabla, COUNT(*) AS Total
    FROM Bronce.Fisiologia
    WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
    UNION ALL
    SELECT 'Bronce.Evaluacion_Pesos', COUNT(*)
    FROM Bronce.Evaluacion_Pesos
    WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
    UNION ALL
    SELECT 'Bronce.Evaluacion_Vegetativa', COUNT(*)
    FROM Bronce.Evaluacion_Vegetativa
    WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
    UNION ALL
    SELECT 'Bronce.Induccion_Floral', COUNT(*)
    FROM Bronce.Induccion_Floral
    WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
    UNION ALL
    SELECT 'Bronce.Tasa_Crecimiento_Brotes', COUNT(*)
    FROM Bronce.Tasa_Crecimiento_Brotes
    WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
) x
WHERE Total > 0
ORDER BY Total DESC, Tabla;

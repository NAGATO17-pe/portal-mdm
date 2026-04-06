SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @modo_aplicar BIT = 0;

SELECT
    'Bronce.Tasa_Crecimiento_Brotes' AS Objeto,
    Estado_Carga,
    COUNT(*) AS Filas
FROM Bronce.Tasa_Crecimiento_Brotes
GROUP BY Estado_Carga

UNION ALL

SELECT
    'Silver.Fact_Tasa_Crecimiento_Brotes' AS Objeto,
    NULL AS Estado_Carga,
    COUNT(*) AS Filas
FROM Silver.Fact_Tasa_Crecimiento_Brotes

UNION ALL

SELECT
    'MDM.Cuarentena[Bronce.Tasa_Crecimiento_Brotes]' AS Objeto,
    Estado,
    COUNT(*) AS Filas
FROM MDM.Cuarentena
WHERE Tabla_Origen = 'Bronce.Tasa_Crecimiento_Brotes'
GROUP BY Estado
ORDER BY Objeto, Estado_Carga;

IF @modo_aplicar = 1
BEGIN
    BEGIN TRANSACTION;

    DELETE FROM Silver.Fact_Tasa_Crecimiento_Brotes;

    DELETE FROM MDM.Cuarentena
    WHERE Tabla_Origen = 'Bronce.Tasa_Crecimiento_Brotes';

    UPDATE Bronce.Tasa_Crecimiento_Brotes
    SET Estado_Carga = 'CARGADO'
    WHERE Estado_Carga IN ('PROCESADO', 'RECHAZADO');

    COMMIT TRANSACTION;
END;

SELECT
    'Bronce.Tasa_Crecimiento_Brotes' AS Objeto,
    Estado_Carga,
    COUNT(*) AS Filas
FROM Bronce.Tasa_Crecimiento_Brotes
GROUP BY Estado_Carga

UNION ALL

SELECT
    'Silver.Fact_Tasa_Crecimiento_Brotes' AS Objeto,
    NULL AS Estado_Carga,
    COUNT(*) AS Filas
FROM Silver.Fact_Tasa_Crecimiento_Brotes

UNION ALL

SELECT
    'MDM.Cuarentena[Bronce.Tasa_Crecimiento_Brotes]' AS Objeto,
    Estado,
    COUNT(*) AS Filas
FROM MDM.Cuarentena
WHERE Tabla_Origen = 'Bronce.Tasa_Crecimiento_Brotes'
GROUP BY Estado
ORDER BY Objeto, Estado_Carga;

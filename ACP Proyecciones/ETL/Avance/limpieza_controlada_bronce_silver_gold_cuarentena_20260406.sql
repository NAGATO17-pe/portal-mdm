SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @modo_aplicar BIT = 0;

/*
LIMPIEZA CONTROLADA
Alcance:
  - Bronce.*
  - Silver.* EXCEPTO Silver.Dim*
  - Gold.*
  - MDM.Cuarentena

Notas:
  - Esta limpieza SI incluye Silver.Bridge_Geografia_Cama.
  - Esta limpieza SI incluye Silver.Fact_Proyecciones.
  - Esta limpieza NO toca Silver.Dim*.
  - Esta limpieza NO toca otros objetos MDM.
*/

IF OBJECT_ID('tempdb..#objetivos') IS NOT NULL
    DROP TABLE #objetivos;

IF OBJECT_ID('tempdb..#conteos_previos') IS NOT NULL
    DROP TABLE #conteos_previos;

IF OBJECT_ID('tempdb..#resultado_delete') IS NOT NULL
    DROP TABLE #resultado_delete;

CREATE TABLE #objetivos (
    Orden_Limpieza INT NOT NULL,
    Esquema SYSNAME NOT NULL,
    Tabla SYSNAME NOT NULL
);

CREATE TABLE #conteos_previos (
    Esquema SYSNAME NOT NULL,
    Tabla SYSNAME NOT NULL,
    Filas_Antes BIGINT NOT NULL
);

CREATE TABLE #resultado_delete (
    Orden_Limpieza INT NOT NULL,
    Esquema SYSNAME NOT NULL,
    Tabla SYSNAME NOT NULL,
    Filas_Eliminadas BIGINT NOT NULL
);

INSERT INTO #objetivos (Orden_Limpieza, Esquema, Tabla)
SELECT
    CASE
        WHEN s.name = 'Gold' THEN 10
        WHEN s.name = 'Silver' THEN 20
        WHEN s.name = 'Bronce' THEN 30
        WHEN s.name = 'MDM' THEN 40
    END AS Orden_Limpieza,
    s.name AS Esquema,
    t.name AS Tabla
FROM sys.tables t
INNER JOIN sys.schemas s
    ON s.schema_id = t.schema_id
WHERE
    s.name IN ('Bronce', 'Silver', 'Gold', 'MDM')
    AND (
        s.name = 'Bronce'
        OR s.name = 'Gold'
        OR (s.name = 'Silver' AND t.name NOT LIKE 'Dim[_]%')
        OR (s.name = 'MDM' AND t.name = 'Cuarentena')
    )
ORDER BY
    CASE
        WHEN s.name = 'Gold' THEN 10
        WHEN s.name = 'Silver' THEN 20
        WHEN s.name = 'Bronce' THEN 30
        WHEN s.name = 'MDM' THEN 40
    END,
    s.name,
    t.name;

DECLARE
    @esquema SYSNAME,
    @tabla SYSNAME,
    @sql NVARCHAR(MAX);

DECLARE cursor_conteo CURSOR LOCAL FAST_FORWARD FOR
SELECT Esquema, Tabla
FROM #objetivos
ORDER BY Orden_Limpieza, Esquema, Tabla;

OPEN cursor_conteo;
FETCH NEXT FROM cursor_conteo INTO @esquema, @tabla;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
        INSERT INTO #conteos_previos (Esquema, Tabla, Filas_Antes)
        SELECT
            @p_esquema,
            @p_tabla,
            COUNT_BIG(*)
        FROM ' + QUOTENAME(@esquema) + N'.' + QUOTENAME(@tabla) + N';';

    EXEC sp_executesql
        @sql,
        N'@p_esquema SYSNAME, @p_tabla SYSNAME',
        @p_esquema = @esquema,
        @p_tabla = @tabla;

    FETCH NEXT FROM cursor_conteo INTO @esquema, @tabla;
END;

CLOSE cursor_conteo;
DEALLOCATE cursor_conteo;

SELECT
    o.Orden_Limpieza,
    o.Esquema,
    o.Tabla,
    c.Filas_Antes
FROM #objetivos o
INNER JOIN #conteos_previos c
    ON c.Esquema = o.Esquema
   AND c.Tabla = o.Tabla
ORDER BY o.Orden_Limpieza, o.Esquema, o.Tabla;

SELECT
    SUM(c.Filas_Antes) AS Total_Filas_A_Limpiar
FROM #conteos_previos c;

IF @modo_aplicar = 1
BEGIN
    BEGIN TRANSACTION;

    DECLARE
        @orden INT,
        @filas_eliminadas BIGINT;

    DECLARE cursor_delete CURSOR LOCAL FAST_FORWARD FOR
    SELECT Orden_Limpieza, Esquema, Tabla
    FROM #objetivos
    ORDER BY Orden_Limpieza, Esquema, Tabla;

    OPEN cursor_delete;
    FETCH NEXT FROM cursor_delete INTO @orden, @esquema, @tabla;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = N'
            DELETE FROM ' + QUOTENAME(@esquema) + N'.' + QUOTENAME(@tabla) + N';
            SELECT @p_filas_eliminadas = @@ROWCOUNT;';

        SET @filas_eliminadas = 0;

        EXEC sp_executesql
            @sql,
            N'@p_filas_eliminadas BIGINT OUTPUT',
            @p_filas_eliminadas = @filas_eliminadas OUTPUT;

        INSERT INTO #resultado_delete (Orden_Limpieza, Esquema, Tabla, Filas_Eliminadas)
        VALUES (@orden, @esquema, @tabla, ISNULL(@filas_eliminadas, 0));

        FETCH NEXT FROM cursor_delete INTO @orden, @esquema, @tabla;
    END;

    CLOSE cursor_delete;
    DEALLOCATE cursor_delete;

    COMMIT TRANSACTION;

    SELECT
        Orden_Limpieza,
        Esquema,
        Tabla,
        Filas_Eliminadas
    FROM #resultado_delete
    ORDER BY Orden_Limpieza, Esquema, Tabla;

    SELECT
        SUM(Filas_Eliminadas) AS Total_Filas_Eliminadas
    FROM #resultado_delete;
END;

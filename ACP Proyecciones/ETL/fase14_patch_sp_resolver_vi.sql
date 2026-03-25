/*
Fase 14B - Patch resolver VI/Test Block
=======================================
Actualiza Silver.sp_Resolver_Geografia_Cama para que:
  - Si la regla de modulo raw indica test block (ej. VI),
    resuelva contra Dim_Geografia con Es_Test_Block = 1.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID('Silver.sp_Resolver_Geografia_Cama', 'P') IS NULL
    EXEC ('CREATE PROCEDURE Silver.sp_Resolver_Geografia_Cama AS BEGIN SET NOCOUNT ON; END');
GO

ALTER PROCEDURE Silver.sp_Resolver_Geografia_Cama
    @Modulo_Raw NVARCHAR(100) = NULL,
    @Turno_Raw NVARCHAR(100) = NULL,
    @Valvula_Raw NVARCHAR(100) = NULL,
    @Cama_Raw NVARCHAR(100) = NULL,
    @Cama_Min_Permitida INT = 1,
    @Cama_Max_Permitida INT = 100
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @Modulo_Token NVARCHAR(50),
        @Turno_Token NVARCHAR(50),
        @Valvula_Token NVARCHAR(50),
        @Cama_Token NVARCHAR(50),
        @Modulo_Int INT = NULL,
        @SubModulo_Int INT = NULL,
        @Turno_Int INT = NULL,
        @Cama_Int INT = NULL,
        @Es_Modulo_Especial BIT = 0,
        @Es_Test_Block_Regla BIT = 0,
        @Coincidencias_Geo INT = 0,
        @ID_Geografia INT = NULL,
        @ID_Cama_Catalogo INT = NULL,
        @Estado_Resolucion NVARCHAR(50),
        @Detalle NVARCHAR(300);

    SELECT
        @Modulo_Token = NULLIF(LTRIM(RTRIM(@Modulo_Raw)), ''),
        @Turno_Token = NULLIF(LTRIM(RTRIM(@Turno_Raw)), ''),
        @Valvula_Token = NULLIF(LTRIM(RTRIM(@Valvula_Raw)), ''),
        @Cama_Token = NULLIF(LTRIM(RTRIM(@Cama_Raw)), '');

    IF @Modulo_Token IS NOT NULL AND @Modulo_Token NOT LIKE '%[^0-9]%'
        SET @Modulo_Token = CONVERT(NVARCHAR(50), CONVERT(INT, @Modulo_Token));

    IF @Turno_Token IS NOT NULL AND @Turno_Token NOT LIKE '%[^0-9]%'
        SET @Turno_Token = CONVERT(NVARCHAR(50), CONVERT(INT, @Turno_Token));

    IF @Valvula_Token IS NOT NULL AND @Valvula_Token NOT LIKE '%[^0-9]%'
        SET @Valvula_Token = CONVERT(NVARCHAR(50), CONVERT(INT, @Valvula_Token));

    IF @Cama_Token IS NOT NULL AND @Cama_Token NOT LIKE '%[^0-9]%'
        SET @Cama_Token = CONVERT(NVARCHAR(50), CONVERT(INT, @Cama_Token));

    IF OBJECT_ID('MDM.Regla_Modulo_Raw', 'U') IS NOT NULL AND @Modulo_Token IS NOT NULL
    BEGIN
        SELECT TOP (1)
            @Modulo_Int = r.Modulo_Int,
            @SubModulo_Int = r.SubModulo_Int,
            @Es_Test_Block_Regla = ISNULL(r.Es_Test_Block, 0)
        FROM MDM.Regla_Modulo_Raw r
        WHERE r.Es_Activa = 1
          AND UPPER(LTRIM(RTRIM(r.Modulo_Raw))) = UPPER(@Modulo_Token);
    END;

    IF @Modulo_Int IS NULL
       AND @Modulo_Token IS NOT NULL
       AND @Modulo_Token NOT LIKE '%[^0-9]%'
    BEGIN
        SET @Modulo_Int = CONVERT(INT, @Modulo_Token);
    END;

    IF @Turno_Token IS NOT NULL AND @Turno_Token NOT LIKE '%[^0-9]%'
        SET @Turno_Int = CONVERT(INT, @Turno_Token);

    IF @Cama_Token IS NOT NULL AND @Cama_Token NOT LIKE '%[^0-9]%'
        SET @Cama_Int = CONVERT(INT, @Cama_Token);

    IF @Es_Test_Block_Regla = 1
    BEGIN
        IF @Turno_Int IS NULL OR @Valvula_Token IS NULL
        BEGIN
            SET @Estado_Resolucion = 'CLAVE_GEOGRAFICA_INCOMPLETA';
            SET @Detalle = 'Test block sin turno o valvula.';
        END
        ELSE
        BEGIN
            ;WITH GeoTB AS (
                SELECT g.ID_Geografia
                FROM Silver.Dim_Geografia g
                WHERE ISNULL(g.Es_Vigente, 1) = 1
                  AND ISNULL(g.Es_Test_Block, 0) = 1
                  AND g.Turno = @Turno_Int
                  AND (
                        CASE
                            WHEN g.Valvula IS NULL THEN NULL
                            WHEN LTRIM(RTRIM(g.Valvula)) = '' THEN NULL
                            WHEN LTRIM(RTRIM(g.Valvula)) NOT LIKE '%[^0-9]%' THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(g.Valvula))))
                            ELSE LTRIM(RTRIM(g.Valvula))
                        END
                      ) = @Valvula_Token
            )
            SELECT
                @Coincidencias_Geo = COUNT(*),
                @ID_Geografia = MIN(ID_Geografia)
            FROM GeoTB;

            IF @Coincidencias_Geo = 1
            BEGIN
                SET @Estado_Resolucion = 'RESUELTA_TEST_BLOCK';
                SET @Detalle = 'Test block resuelto por Turno/Valvula.';
            END
            ELSE IF @Coincidencias_Geo = 0
            BEGIN
                SET @Estado_Resolucion = 'TEST_BLOCK_NO_MAPEADO';
                SET @Detalle = 'No existe geografia test block para Turno/Valvula.';
                SET @ID_Geografia = NULL;
            END
            ELSE
            BEGIN
                SET @Estado_Resolucion = 'TEST_BLOCK_AMBIGUO';
                SET @Detalle = 'Mas de una geografia test block para Turno/Valvula.';
                SET @ID_Geografia = NULL;
            END
        END;

        SELECT
            @Modulo_Token AS Modulo_Token,
            @Turno_Token AS Turno_Token,
            @Valvula_Token AS Valvula_Token,
            @Cama_Token AS Cama_Token,
            @Modulo_Int AS Modulo_Int,
            @SubModulo_Int AS SubModulo_Int,
            @Turno_Int AS Turno_Int,
            @Cama_Int AS Cama_Int,
            @ID_Geografia AS ID_Geografia,
            @ID_Cama_Catalogo AS ID_Cama_Catalogo,
            @Estado_Resolucion AS Estado_Resolucion,
            @Detalle AS Detalle;
        RETURN;
    END;

    IF @Modulo_Int IS NULL
        SET @Es_Modulo_Especial = 1;

    IF @Es_Modulo_Especial = 1
    BEGIN
        SET @Estado_Resolucion = 'CASO_ESPECIAL_MODULO';
        SET @Detalle = 'Modulo especial (sin regla operativa).';
    END
    ELSE IF @Turno_Int IS NULL OR @Valvula_Token IS NULL
    BEGIN
        SET @Estado_Resolucion = 'CLAVE_GEOGRAFICA_INCOMPLETA';
        SET @Detalle = 'Falta turno o valvula.';
    END
    ELSE
    BEGIN
        ;WITH Geo AS (
            SELECT
                g.ID_Geografia
            FROM Silver.Dim_Geografia g
            WHERE ISNULL(g.Es_Vigente, 1) = 1
              AND ISNULL(g.Es_Test_Block, 0) = 0
              AND g.Modulo = @Modulo_Int
              AND ISNULL(g.SubModulo, -1) = ISNULL(@SubModulo_Int, -1)
              AND g.Turno = @Turno_Int
              AND (
                    CASE
                        WHEN g.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(g.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(g.Valvula)) NOT LIKE '%[^0-9]%' THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(g.Valvula))))
                        ELSE LTRIM(RTRIM(g.Valvula))
                    END
                  ) = @Valvula_Token
        )
        SELECT
            @Coincidencias_Geo = COUNT(*),
            @ID_Geografia = MIN(ID_Geografia)
        FROM Geo;

        IF @Coincidencias_Geo = 0
        BEGIN
            SET @Estado_Resolucion = 'GEOGRAFIA_NO_ENCONTRADA';
            SET @Detalle = 'No existe geografia vigente para modulo/submodulo/turno/valvula.';
        END
        ELSE IF @Coincidencias_Geo > 1
        BEGIN
            SET @Estado_Resolucion = 'GEOGRAFIA_AMBIGUA';
            SET @Detalle = 'Existe mas de una geografia vigente para modulo/submodulo/turno/valvula.';
            SET @ID_Geografia = NULL;
        END
        ELSE
        BEGIN
            IF @Cama_Token IS NULL OR @Cama_Token = '0'
            BEGIN
                SET @Estado_Resolucion = 'RESUELTA_BASE_SIN_CAMA';
                SET @Detalle = 'Geografia resuelta sin cama especifica.';
            END
            ELSE IF @Cama_Int IS NULL OR @Cama_Int < @Cama_Min_Permitida OR @Cama_Int > @Cama_Max_Permitida
            BEGIN
                SET @Estado_Resolucion = 'CAMA_NO_VALIDA';
                SET @Detalle = 'Cama fuera de rango permitido.';
            END
            ELSE
            BEGIN
                SELECT
                    @ID_Cama_Catalogo = c.ID_Cama_Catalogo
                FROM Silver.Dim_Cama_Catalogo c
                WHERE c.Es_Activa = 1
                  AND c.Cama_Normalizada = CONVERT(NVARCHAR(50), @Cama_Int);

                IF @ID_Cama_Catalogo IS NULL
                BEGIN
                    SET @Estado_Resolucion = 'CAMA_NO_CATALOGO';
                    SET @Detalle = 'Cama valida, pero no existe en catalogo.';
                END
                ELSE IF EXISTS (
                    SELECT 1
                    FROM Silver.Bridge_Geografia_Cama b
                    WHERE b.ID_Geografia = @ID_Geografia
                      AND b.ID_Cama_Catalogo = @ID_Cama_Catalogo
                      AND b.Es_Vigente = 1
                      AND b.Fecha_Fin_Vigencia IS NULL
                )
                BEGIN
                    SET @Estado_Resolucion = 'RESUELTA_BASE_Y_CAMA';
                    SET @Detalle = 'Geografia y cama resueltas.';
                END
                ELSE
                BEGIN
                    SET @Estado_Resolucion = 'CAMA_NO_RELACION';
                    SET @Detalle = 'Cama existe en catalogo, pero no esta relacionada a la geografia.';
                END
            END
        END
    END

    SELECT
        @Modulo_Token AS Modulo_Token,
        @Turno_Token AS Turno_Token,
        @Valvula_Token AS Valvula_Token,
        @Cama_Token AS Cama_Token,
        @Modulo_Int AS Modulo_Int,
        @SubModulo_Int AS SubModulo_Int,
        @Turno_Int AS Turno_Int,
        @Cama_Int AS Cama_Int,
        @ID_Geografia AS ID_Geografia,
        @ID_Cama_Catalogo AS ID_Cama_Catalogo,
        @Estado_Resolucion AS Estado_Resolucion,
        @Detalle AS Detalle;
END;
GO

PRINT 'Patch VI/Test Block aplicado en Silver.sp_Resolver_Geografia_Cama';
GO

/*
Fase 12 - Normalizacion de Modulo_Raw (9.1/9.2/VI) + ajuste SP geografia/cama
===============================================================================
Objetivo:
  1) Crear tabla de reglas de modulo crudo.
  2) Soportar SubModulo y Tipo_Conduccion en MDM/Silver.
  3) Actualizar SP de resolucion y upsert para usar reglas.
*/

SET NOCOUNT ON;
GO

/* =========================================================
   1) Tabla de reglas Modulo_Raw
   ========================================================= */

IF OBJECT_ID('MDM.Regla_Modulo_Raw', 'U') IS NULL
BEGIN
    CREATE TABLE MDM.Regla_Modulo_Raw (
        ID_Regla_Modulo            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Modulo_Raw                 NVARCHAR(100) NOT NULL,
        Modulo_Int                 INT NULL,
        SubModulo_Int              INT NULL,
        Tipo_Conduccion            NVARCHAR(50) NULL,
        Es_Test_Block              BIT NOT NULL CONSTRAINT DF_Regla_Modulo_Raw_Es_Test_Block DEFAULT (0),
        Es_Activa                  BIT NOT NULL CONSTRAINT DF_Regla_Modulo_Raw_Es_Activa DEFAULT (1),
        Fecha_Creacion             DATETIME2(0) NOT NULL CONSTRAINT DF_Regla_Modulo_Raw_Fecha_Creacion DEFAULT (SYSDATETIME()),
        Fecha_Modificacion         DATETIME2(0) NULL,
        Observacion                NVARCHAR(300) NULL
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('MDM.Regla_Modulo_Raw')
      AND name = 'UX_Regla_Modulo_Raw_Activa'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_Regla_Modulo_Raw_Activa
    ON MDM.Regla_Modulo_Raw (Modulo_Raw)
    WHERE Es_Activa = 1;
END;
GO

/* =========================================================
   2) Columnas nuevas en catalogo/dim geografia
   ========================================================= */

IF COL_LENGTH('MDM.Catalogo_Geografia', 'SubModulo') IS NULL
BEGIN
    ALTER TABLE MDM.Catalogo_Geografia
    ADD SubModulo INT NULL;
END;
GO

IF COL_LENGTH('MDM.Catalogo_Geografia', 'Tipo_Conduccion') IS NULL
BEGIN
    ALTER TABLE MDM.Catalogo_Geografia
    ADD Tipo_Conduccion NVARCHAR(50) NULL;
END;
GO

IF COL_LENGTH('Silver.Dim_Geografia', 'SubModulo') IS NULL
BEGIN
    ALTER TABLE Silver.Dim_Geografia
    ADD SubModulo INT NULL;
END;
GO

IF COL_LENGTH('Silver.Dim_Geografia', 'Tipo_Conduccion') IS NULL
BEGIN
    ALTER TABLE Silver.Dim_Geografia
    ADD Tipo_Conduccion NVARCHAR(50) NULL;
END;
GO

/* =========================================================
   3) Seed controlado reglas 9.1 / 9.2 / VI
   ========================================================= */

DECLARE @usuario NVARCHAR(20);
SET @usuario = LEFT(COALESCE(SUSER_SNAME(), 'SISTEMA'), 20);

UPDATE MDM.Regla_Modulo_Raw
SET
    Modulo_Int         = 9,
    SubModulo_Int      = 1,
    Tipo_Conduccion    = N'SUELO',
    Es_Test_Block      = 0,
    Es_Activa          = 1,
    Fecha_Modificacion = SYSDATETIME(),
    Observacion        = N'Normalizacion oficial ACP: 9.1 = Suelo'
WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.1';

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO MDM.Regla_Modulo_Raw (
        Modulo_Raw,
        Modulo_Int,
        SubModulo_Int,
        Tipo_Conduccion,
        Es_Test_Block,
        Es_Activa,
        Observacion
    )
    VALUES (
        N'9.1',
        9,
        1,
        N'SUELO',
        0,
        1,
        N'Normalizacion oficial ACP: 9.1 = Suelo'
    );
END;

UPDATE MDM.Regla_Modulo_Raw
SET
    Modulo_Int         = 9,
    SubModulo_Int      = 2,
    Tipo_Conduccion    = N'MACETA',
    Es_Test_Block      = 0,
    Es_Activa          = 1,
    Fecha_Modificacion = SYSDATETIME(),
    Observacion        = N'Normalizacion oficial ACP: 9.2 = Maceta'
WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.2';

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO MDM.Regla_Modulo_Raw (
        Modulo_Raw,
        Modulo_Int,
        SubModulo_Int,
        Tipo_Conduccion,
        Es_Test_Block,
        Es_Activa,
        Observacion
    )
    VALUES (
        N'9.2',
        9,
        2,
        N'MACETA',
        0,
        1,
        N'Normalizacion oficial ACP: 9.2 = Maceta'
    );
END;

UPDATE MDM.Regla_Modulo_Raw
SET
    Modulo_Int         = NULL,
    SubModulo_Int      = NULL,
    Tipo_Conduccion    = N'TEST_BLOCK',
    Es_Test_Block      = 1,
    Es_Activa          = 1,
    Fecha_Modificacion = SYSDATETIME(),
    Observacion        = N'Normalizacion oficial ACP: VI = Test_Block'
WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = 'VI';

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO MDM.Regla_Modulo_Raw (
        Modulo_Raw,
        Modulo_Int,
        SubModulo_Int,
        Tipo_Conduccion,
        Es_Test_Block,
        Es_Activa,
        Observacion
    )
    VALUES (
        N'VI',
        NULL,
        NULL,
        N'TEST_BLOCK',
        1,
        1,
        N'Normalizacion oficial ACP: VI = Test_Block'
    );
END;
GO

/* =========================================================
   4) SP Resolver Geografia + Cama con regla Modulo_Raw
   ========================================================= */

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
        SET @Es_Modulo_Especial = 1;
    ELSE IF @Modulo_Int IS NULL
        SET @Es_Modulo_Especial = 1;

    IF @Es_Modulo_Especial = 1
    BEGIN
        SET @Estado_Resolucion = 'CASO_ESPECIAL_MODULO';
        SET @Detalle = 'Modulo especial (test block o no numerico sin regla operativa).';
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

/* =========================================================
   5) SP Upsert Cama desde Bronce con regla Modulo_Raw
   ========================================================= */

IF OBJECT_ID('Silver.sp_Upsert_Cama_Desde_Bronce', 'P') IS NULL
    EXEC ('CREATE PROCEDURE Silver.sp_Upsert_Cama_Desde_Bronce AS BEGIN SET NOCOUNT ON; END');
GO

ALTER PROCEDURE Silver.sp_Upsert_Cama_Desde_Bronce
    @Modo_Aplicar BIT = 0,
    @Cama_Min_Permitida INT = 1,
    @Cama_Max_Permitida INT = 100
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('Silver.Dim_Cama_Catalogo', 'U') IS NULL
    BEGIN
        RAISERROR('No existe Silver.Dim_Cama_Catalogo.', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID('Silver.Bridge_Geografia_Cama', 'U') IS NULL
    BEGIN
        RAISERROR('No existe Silver.Bridge_Geografia_Cama.', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID('Bronce.Evaluacion_Pesos', 'U') IS NULL
       OR OBJECT_ID('Bronce.Evaluacion_Vegetativa', 'U') IS NULL
    BEGIN
        RAISERROR('No existen tablas Bronce requeridas.', 16, 1);
        RETURN;
    END;

    IF OBJECT_ID('tempdb..#BronceRaw') IS NOT NULL DROP TABLE #BronceRaw;
    IF OBJECT_ID('tempdb..#Eval') IS NOT NULL DROP TABLE #Eval;
    IF OBJECT_ID('tempdb..#Aptos') IS NOT NULL DROP TABLE #Aptos;

    CREATE TABLE #BronceRaw (
        Tabla_Origen NVARCHAR(50) NOT NULL,
        Modulo_Raw NVARCHAR(100) NULL,
        Turno_Raw NVARCHAR(100) NULL,
        Valvula_Raw NVARCHAR(100) NULL,
        Cama_Raw NVARCHAR(100) NULL
    );

    ;WITH LotePesos AS (
        SELECT TOP (1) Fecha_Sistema, Nombre_Archivo
        FROM Bronce.Evaluacion_Pesos
        ORDER BY Fecha_Sistema DESC, ID_Evaluacion_Pesos DESC
    )
    INSERT INTO #BronceRaw (Tabla_Origen, Modulo_Raw, Turno_Raw, Valvula_Raw, Cama_Raw)
    SELECT
        'Bronce.Evaluacion_Pesos',
        p.Modulo_Raw,
        p.Turno_Raw,
        p.Valvula_Raw,
        p.Cama_Raw
    FROM Bronce.Evaluacion_Pesos p
    INNER JOIN LotePesos l
        ON p.Fecha_Sistema = l.Fecha_Sistema
       AND p.Nombre_Archivo = l.Nombre_Archivo;

    ;WITH LoteVeg AS (
        SELECT TOP (1) Fecha_Sistema, Nombre_Archivo
        FROM Bronce.Evaluacion_Vegetativa
        ORDER BY Fecha_Sistema DESC, ID_Evaluacion_Vegetativa DESC
    )
    INSERT INTO #BronceRaw (Tabla_Origen, Modulo_Raw, Turno_Raw, Valvula_Raw, Cama_Raw)
    SELECT
        'Bronce.Evaluacion_Vegetativa',
        v.Modulo_Raw,
        v.Turno_Raw,
        v.Valvula_Raw,
        v.Cama_Raw
    FROM Bronce.Evaluacion_Vegetativa v
    INNER JOIN LoteVeg l
        ON v.Fecha_Sistema = l.Fecha_Sistema
       AND v.Nombre_Archivo = l.Nombre_Archivo;

    CREATE TABLE #Eval (
        Estado_Resolucion NVARCHAR(50) NOT NULL,
        ID_Geografia INT NULL,
        Cama_Int INT NULL
    );

    INSERT INTO #Eval (Estado_Resolucion, ID_Geografia, Cama_Int)
    SELECT
        CASE
            WHEN x.Es_Modulo_Especial = 1 THEN 'CASO_ESPECIAL_MODULO'
            WHEN x.Modulo_Int IS NULL OR x.Turno_Int IS NULL OR x.Valvula_Token IS NULL THEN 'CLAVE_GEOGRAFICA_INCOMPLETA'
            WHEN x.Cama_Int IS NULL OR x.Cama_Int < @Cama_Min_Permitida OR x.Cama_Int > @Cama_Max_Permitida THEN 'CAMA_NO_VALIDA'
            WHEN x.Coincidencias_Geo = 0 THEN 'GEOGRAFIA_NO_ENCONTRADA'
            WHEN x.Coincidencias_Geo > 1 THEN 'GEOGRAFIA_AMBIGUA'
            ELSE 'APTO_PARA_INSERT'
        END AS Estado_Resolucion,
        CASE WHEN x.Coincidencias_Geo = 1 THEN x.ID_Geografia_Unica ELSE NULL END AS ID_Geografia,
        x.Cama_Int
    FROM (
        SELECT
            t.Modulo_Int,
            t.SubModulo_Int,
            t.Turno_Int,
            t.Valvula_Token,
            t.Cama_Int,
            t.Es_Modulo_Especial,
            g.Coincidencias_Geo,
            g.ID_Geografia_Unica
        FROM #BronceRaw r
        CROSS APPLY (
            SELECT
                NULLIF(LTRIM(RTRIM(r.Modulo_Raw)), '') AS Modulo_Token_Raw,
                NULLIF(LTRIM(RTRIM(r.Turno_Raw)), '') AS Turno_Token_Raw,
                NULLIF(LTRIM(RTRIM(r.Valvula_Raw)), '') AS Valvula_Token_Raw,
                NULLIF(LTRIM(RTRIM(r.Cama_Raw)), '') AS Cama_Token_Raw
        ) raw
        OUTER APPLY (
            SELECT TOP (1)
                rr.Modulo_Int,
                rr.SubModulo_Int,
                ISNULL(rr.Es_Test_Block, 0) AS Es_Test_Block_Regla
            FROM MDM.Regla_Modulo_Raw rr
            WHERE rr.Es_Activa = 1
              AND raw.Modulo_Token_Raw IS NOT NULL
              AND UPPER(LTRIM(RTRIM(rr.Modulo_Raw))) = UPPER(raw.Modulo_Token_Raw)
        ) regla
        CROSS APPLY (
            SELECT
                CASE
                    WHEN raw.Turno_Token_Raw IS NULL THEN NULL
                    WHEN raw.Turno_Token_Raw NOT LIKE '%[^0-9]%' THEN CONVERT(INT, raw.Turno_Token_Raw)
                    ELSE NULL
                END AS Turno_Int,
                CASE
                    WHEN raw.Valvula_Token_Raw IS NULL THEN NULL
                    WHEN raw.Valvula_Token_Raw NOT LIKE '%[^0-9]%' THEN CONVERT(NVARCHAR(50), CONVERT(INT, raw.Valvula_Token_Raw))
                    ELSE raw.Valvula_Token_Raw
                END AS Valvula_Token,
                CASE
                    WHEN raw.Cama_Token_Raw IS NULL THEN NULL
                    WHEN raw.Cama_Token_Raw NOT LIKE '%[^0-9]%' THEN CONVERT(INT, raw.Cama_Token_Raw)
                    ELSE NULL
                END AS Cama_Int,
                CASE
                    WHEN raw.Modulo_Token_Raw IS NOT NULL AND raw.Modulo_Token_Raw NOT LIKE '%[^0-9]%' THEN CONVERT(INT, raw.Modulo_Token_Raw)
                    ELSE NULL
                END AS Modulo_Int_Raw
        ) base
        CROSS APPLY (
            SELECT
                COALESCE(regla.Modulo_Int, base.Modulo_Int_Raw) AS Modulo_Int,
                regla.SubModulo_Int AS SubModulo_Int,
                CASE
                    WHEN ISNULL(regla.Es_Test_Block_Regla, 0) = 1 THEN 1
                    WHEN COALESCE(regla.Modulo_Int, base.Modulo_Int_Raw) IS NULL THEN 1
                    ELSE 0
                END AS Es_Modulo_Especial,
                base.Turno_Int,
                base.Valvula_Token,
                base.Cama_Int
        ) t
        OUTER APPLY (
            SELECT
                COUNT(*) AS Coincidencias_Geo,
                MIN(gv.ID_Geografia) AS ID_Geografia_Unica
            FROM Silver.Dim_Geografia gv
            WHERE ISNULL(gv.Es_Vigente, 1) = 1
              AND ISNULL(gv.Es_Test_Block, 0) = 0
              AND gv.Modulo = t.Modulo_Int
              AND ISNULL(gv.SubModulo, -1) = ISNULL(t.SubModulo_Int, -1)
              AND gv.Turno = t.Turno_Int
              AND (
                    CASE
                        WHEN gv.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(gv.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(gv.Valvula)) NOT LIKE '%[^0-9]%' THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(gv.Valvula))))
                        ELSE LTRIM(RTRIM(gv.Valvula))
                    END
                  ) = t.Valvula_Token
        ) g
    ) x;

    CREATE TABLE #Aptos (
        ID_Geografia INT NOT NULL,
        Cama_Normalizada NVARCHAR(50) NOT NULL,
        CONSTRAINT PK_Aptos PRIMARY KEY (ID_Geografia, Cama_Normalizada)
    );

    INSERT INTO #Aptos (ID_Geografia, Cama_Normalizada)
    SELECT DISTINCT
        e.ID_Geografia,
        CONVERT(NVARCHAR(50), e.Cama_Int)
    FROM #Eval e
    WHERE e.Estado_Resolucion = 'APTO_PARA_INSERT'
      AND e.ID_Geografia IS NOT NULL
      AND e.Cama_Int BETWEEN @Cama_Min_Permitida AND @Cama_Max_Permitida;

    DECLARE
        @Insert_Catalogo_Real INT = 0,
        @Insert_Bridge_Real INT = 0;

    IF @Modo_Aplicar = 1
    BEGIN
        BEGIN TRANSACTION;

        INSERT INTO Silver.Dim_Cama_Catalogo (Cama_Normalizada)
        SELECT DISTINCT a.Cama_Normalizada
        FROM #Aptos a
        WHERE NOT EXISTS (
            SELECT 1
            FROM Silver.Dim_Cama_Catalogo c
            WHERE c.Cama_Normalizada = a.Cama_Normalizada
        );
        SET @Insert_Catalogo_Real = @@ROWCOUNT;

        INSERT INTO Silver.Bridge_Geografia_Cama (
            ID_Geografia,
            ID_Cama_Catalogo,
            Fecha_Inicio_Vigencia,
            Fecha_Fin_Vigencia,
            Es_Vigente,
            Fuente_Registro,
            Observacion
        )
        SELECT
            a.ID_Geografia,
            c.ID_Cama_Catalogo,
            CAST(GETDATE() AS DATE),
            NULL,
            1,
            'SP_UPSERT_CAMA_BRONCE',
            'Insercion via Silver.sp_Upsert_Cama_Desde_Bronce'
        FROM #Aptos a
        INNER JOIN Silver.Dim_Cama_Catalogo c
            ON c.Cama_Normalizada = a.Cama_Normalizada
        WHERE NOT EXISTS (
            SELECT 1
            FROM Silver.Bridge_Geografia_Cama b
            WHERE b.ID_Geografia = a.ID_Geografia
              AND b.ID_Cama_Catalogo = c.ID_Cama_Catalogo
              AND b.Es_Vigente = 1
              AND b.Fecha_Fin_Vigencia IS NULL
        );
        SET @Insert_Bridge_Real = @@ROWCOUNT;

        COMMIT TRANSACTION;
    END;

    SELECT
        @Modo_Aplicar AS Modo_Aplicar,
        (SELECT COUNT(*) FROM #BronceRaw) AS Filas_Bronce_Leidas,
        (SELECT COUNT(*) FROM #Eval) AS Filas_Evaluadas,
        (SELECT COUNT(*) FROM #Aptos) AS Combinaciones_Aptas_Distintas,
        @Insert_Catalogo_Real AS Insert_Catalogo_Real,
        @Insert_Bridge_Real AS Insert_Bridge_Real;

    SELECT
        Estado_Resolucion,
        COUNT(*) AS Filas
    FROM #Eval
    GROUP BY Estado_Resolucion
    ORDER BY COUNT(*) DESC;
END;
GO

/* =========================================================
   6) SP Validar calidad camas (default 1..100)
   ========================================================= */

IF OBJECT_ID('Silver.sp_Validar_Calidad_Camas', 'P') IS NULL
    EXEC ('CREATE PROCEDURE Silver.sp_Validar_Calidad_Camas AS BEGIN SET NOCOUNT ON; END');
GO

ALTER PROCEDURE Silver.sp_Validar_Calidad_Camas
    @Cama_Max_Permitida INT = 100,
    @Max_Camas_Por_Geografia INT = 100
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH Catalogo AS (
        SELECT
            TRY_CONVERT(INT, Cama_Normalizada) AS Cama_Int
        FROM Silver.Dim_Cama_Catalogo
        WHERE Es_Activa = 1
    ),
    Metricas AS (
        SELECT
            SUM(CASE WHEN Cama_Int IS NULL OR Cama_Int <= 0 OR Cama_Int > @Cama_Max_Permitida THEN 1 ELSE 0 END) AS Cama_Fuera_Regla
        FROM Catalogo
    ),
    GeoSaturada AS (
        SELECT COUNT(*) AS Geografias_Saturadas
        FROM (
            SELECT
                ID_Geografia,
                COUNT(*) AS Cantidad_Camas
            FROM Silver.Bridge_Geografia_Cama
            WHERE Es_Vigente = 1
              AND Fecha_Fin_Vigencia IS NULL
            GROUP BY ID_Geografia
            HAVING COUNT(*) > @Max_Camas_Por_Geografia
        ) q
    )
    SELECT
        m.Cama_Fuera_Regla,
        gs.Geografias_Saturadas,
        CASE
            WHEN m.Cama_Fuera_Regla = 0 AND gs.Geografias_Saturadas = 0 THEN 'OK_OPERATIVO'
            WHEN m.Cama_Fuera_Regla <= 5 AND gs.Geografias_Saturadas <= 5 THEN 'REVISAR_PUNTUAL'
            ELSE 'RIESGO_CONTAMINACION'
        END AS Estado_Calidad_Cama
    FROM Metricas m
    CROSS JOIN GeoSaturada gs;
END;
GO

PRINT 'Fase 12 aplicada: reglas Modulo_Raw + SP actualizados.';
GO

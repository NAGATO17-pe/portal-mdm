/*
Fase 14A - Seed VI como Test Block en MDM.Catalogo_Geografia
==============================================================
Toma VI del ultimo lote de Bronce (Pesos + Vegetativa) y crea
geografias test block por Turno/Valvula.

Uso:
  @modo_aplicar = 0  -> preview
  @modo_aplicar = 1  -> apply
*/

SET NOCOUNT ON;
GO

DECLARE @modo_aplicar BIT = 0;
DECLARE @fundo NVARCHAR(100) = N'ARANDANO ACP';
DECLARE @sector_test_block NVARCHAR(100) = N'TEST_BLOCK';

IF OBJECT_ID('tempdb..#BronceRaw') IS NOT NULL DROP TABLE #BronceRaw;
IF OBJECT_ID('tempdb..#VI') IS NOT NULL DROP TABLE #VI;

CREATE TABLE #BronceRaw (
    Tabla_Origen NVARCHAR(50) NOT NULL,
    Modulo_Raw NVARCHAR(100) NULL,
    Turno_Raw NVARCHAR(100) NULL,
    Valvula_Raw NVARCHAR(100) NULL
);

;WITH LotePesos AS (
    SELECT TOP (1) Fecha_Sistema, Nombre_Archivo
    FROM Bronce.Evaluacion_Pesos
    ORDER BY Fecha_Sistema DESC, ID_Evaluacion_Pesos DESC
)
INSERT INTO #BronceRaw (Tabla_Origen, Modulo_Raw, Turno_Raw, Valvula_Raw)
SELECT
    'Bronce.Evaluacion_Pesos',
    p.Modulo_Raw,
    p.Turno_Raw,
    p.Valvula_Raw
FROM Bronce.Evaluacion_Pesos p
INNER JOIN LotePesos l
    ON p.Fecha_Sistema = l.Fecha_Sistema
   AND p.Nombre_Archivo = l.Nombre_Archivo;

;WITH LoteVeg AS (
    SELECT TOP (1) Fecha_Sistema, Nombre_Archivo
    FROM Bronce.Evaluacion_Vegetativa
    ORDER BY Fecha_Sistema DESC, ID_Evaluacion_Vegetativa DESC
)
INSERT INTO #BronceRaw (Tabla_Origen, Modulo_Raw, Turno_Raw, Valvula_Raw)
SELECT
    'Bronce.Evaluacion_Vegetativa',
    v.Modulo_Raw,
    v.Turno_Raw,
    v.Valvula_Raw
FROM Bronce.Evaluacion_Vegetativa v
INNER JOIN LoteVeg l
    ON v.Fecha_Sistema = l.Fecha_Sistema
   AND v.Nombre_Archivo = l.Nombre_Archivo;

;WITH Normalizada AS (
    SELECT
        NULLIF(LTRIM(RTRIM(Modulo_Raw)), '') AS Modulo_Token,
        CASE
            WHEN NULLIF(LTRIM(RTRIM(Turno_Raw)), '') IS NOT NULL
             AND NULLIF(LTRIM(RTRIM(Turno_Raw)), '') NOT LIKE '%[^0-9]%'
                THEN CONVERT(INT, NULLIF(LTRIM(RTRIM(Turno_Raw)), ''))
            ELSE NULL
        END AS Turno_Int,
        CASE
            WHEN NULLIF(LTRIM(RTRIM(Valvula_Raw)), '') IS NULL THEN NULL
            WHEN NULLIF(LTRIM(RTRIM(Valvula_Raw)), '') NOT LIKE '%[^0-9]%'
                THEN CONVERT(NVARCHAR(50), CONVERT(INT, NULLIF(LTRIM(RTRIM(Valvula_Raw)), '')))
            ELSE NULLIF(LTRIM(RTRIM(Valvula_Raw)), '')
        END AS Valvula_Token
    FROM #BronceRaw
),
Filtrada AS (
    SELECT
        Turno_Int,
        Valvula_Token,
        COUNT(*) AS Filas_Afectadas
    FROM Normalizada
    WHERE UPPER(ISNULL(Modulo_Token, '')) = 'VI'
      AND Turno_Int IS NOT NULL
      AND Valvula_Token IS NOT NULL
    GROUP BY
        Turno_Int,
        Valvula_Token
)
SELECT
    ROW_NUMBER() OVER (ORDER BY Filas_Afectadas DESC, Turno_Int, Valvula_Token) AS Prioridad,
    Turno_Int,
    Valvula_Token,
    Filas_Afectadas
INTO #VI
FROM Filtrada;

DECLARE @usa_submodulo BIT = CASE WHEN COL_LENGTH('MDM.Catalogo_Geografia', 'SubModulo') IS NULL THEN 0 ELSE 1 END;
DECLARE @usa_tipo BIT = CASE WHEN COL_LENGTH('MDM.Catalogo_Geografia', 'Tipo_Conduccion') IS NULL THEN 0 ELSE 1 END;
DECLARE @insert_real INT = 0;

IF @modo_aplicar = 1
BEGIN
    BEGIN TRANSACTION;

    IF @usa_submodulo = 1 AND @usa_tipo = 1
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, SubModulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Tipo_Conduccion, Es_Activa, Fecha_Creacion
        )
        SELECT
            @fundo,
            @sector_test_block,
            -1,
            NULL,
            v.Turno_Int,
            v.Valvula_Token,
            NULL,
            1,
            NULL,
            N'TEST_BLOCK',
            1,
            SYSDATETIME()
        FROM #VI v
        WHERE NOT EXISTS (
            SELECT 1
            FROM MDM.Catalogo_Geografia c
            WHERE c.Es_Activa = 1
              AND ISNULL(c.Es_Test_Block, 0) = 1
              AND c.Turno = v.Turno_Int
              AND (
                    CASE
                        WHEN c.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) NOT LIKE '%[^0-9]%'
                            THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(c.Valvula))))
                        ELSE LTRIM(RTRIM(c.Valvula))
                    END
                  ) = v.Valvula_Token
        );
    END
    ELSE IF @usa_submodulo = 1 AND @usa_tipo = 0
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, SubModulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Es_Activa, Fecha_Creacion
        )
        SELECT
            @fundo,
            @sector_test_block,
            -1,
            NULL,
            v.Turno_Int,
            v.Valvula_Token,
            NULL,
            1,
            NULL,
            1,
            SYSDATETIME()
        FROM #VI v
        WHERE NOT EXISTS (
            SELECT 1
            FROM MDM.Catalogo_Geografia c
            WHERE c.Es_Activa = 1
              AND ISNULL(c.Es_Test_Block, 0) = 1
              AND c.Turno = v.Turno_Int
              AND (
                    CASE
                        WHEN c.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) NOT LIKE '%[^0-9]%'
                            THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(c.Valvula))))
                        ELSE LTRIM(RTRIM(c.Valvula))
                    END
                  ) = v.Valvula_Token
        );
    END
    ELSE
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Es_Activa, Fecha_Creacion
        )
        SELECT
            @fundo,
            @sector_test_block,
            -1,
            v.Turno_Int,
            v.Valvula_Token,
            NULL,
            1,
            NULL,
            1,
            SYSDATETIME()
        FROM #VI v
        WHERE NOT EXISTS (
            SELECT 1
            FROM MDM.Catalogo_Geografia c
            WHERE c.Es_Activa = 1
              AND ISNULL(c.Es_Test_Block, 0) = 1
              AND c.Turno = v.Turno_Int
              AND (
                    CASE
                        WHEN c.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) NOT LIKE '%[^0-9]%'
                            THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(c.Valvula))))
                        ELSE LTRIM(RTRIM(c.Valvula))
                    END
                  ) = v.Valvula_Token
        );
    END;

    SET @insert_real = @@ROWCOUNT;
    COMMIT TRANSACTION;
END;

SELECT
    @modo_aplicar AS Modo_Aplicar,
    (SELECT COUNT(*) FROM #VI) AS Combinaciones_VI_Distintas,
    (SELECT ISNULL(SUM(Filas_Afectadas), 0) FROM #VI) AS Filas_Afectadas_VI,
    @insert_real AS Insert_MDM_Real;

SELECT
    Prioridad,
    Turno_Int,
    Valvula_Token,
    Filas_Afectadas
FROM #VI
ORDER BY Prioridad;
GO

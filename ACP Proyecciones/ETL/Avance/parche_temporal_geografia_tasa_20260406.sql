SET NOCOUNT ON;

DECLARE @modo_aplicar BIT = 0;
DECLARE @fundo_default NVARCHAR(100) = N'Arandano Acp';
DECLARE @sector_default NVARCHAR(100) = N'Sin_Sector_Mapa';
DECLARE @fecha_creacion DATETIME2 = SYSDATETIME();

IF OBJECT_ID('tempdb..#faltantes') IS NOT NULL
    DROP TABLE #faltantes;

/*
PARCHE TEMPORAL:
Inserta combinaciones faltantes de Modulo/SubModulo/Turno/Valvula para Tasa
en MDM.Catalogo_Geografia usando Fundo/Sector genéricos.
No aplica a Test Block.
*/

WITH base AS (
    SELECT
        Modulo_Raw,
        Turno_Raw,
        Valvula_Raw,
        Cama_Raw
    FROM Bronce.Tasa_Crecimiento_Brotes
    WHERE Estado_Carga = 'RECHAZADO'
),
tokens AS (
    SELECT
        NULLIF(LTRIM(RTRIM(Modulo_Raw)), '') AS Modulo_Token_Raw,
        NULLIF(LTRIM(RTRIM(Turno_Raw)), '') AS Turno_Token_Raw,
        NULLIF(LTRIM(RTRIM(Valvula_Raw)), '') AS Valvula_Token_Raw,
        NULLIF(LTRIM(RTRIM(Cama_Raw)), '') AS Cama_Token_Raw,
        Modulo_Raw
    FROM base
),
base_norm AS (
    SELECT
        Modulo_Raw,
        CASE
            WHEN Modulo_Token_Raw IS NOT NULL AND Modulo_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(NVARCHAR(50), CONVERT(INT, Modulo_Token_Raw))
            ELSE Modulo_Token_Raw
        END AS Modulo_Token,
        CASE
            WHEN Turno_Token_Raw IS NOT NULL AND Turno_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(NVARCHAR(50), CONVERT(INT, Turno_Token_Raw))
            ELSE Turno_Token_Raw
        END AS Turno_Token,
        CASE
            WHEN Valvula_Token_Raw IS NOT NULL AND Valvula_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(NVARCHAR(50), CONVERT(INT, Valvula_Token_Raw))
            ELSE Valvula_Token_Raw
        END AS Valvula_Token,
        CASE
            WHEN Cama_Token_Raw IS NOT NULL AND Cama_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(NVARCHAR(50), CONVERT(INT, Cama_Token_Raw))
            ELSE Cama_Token_Raw
        END AS Cama_Token,
        CASE
            WHEN Turno_Token_Raw IS NOT NULL AND Turno_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(INT, Turno_Token_Raw)
            ELSE NULL
        END AS Turno_Int,
        CASE
            WHEN Modulo_Token_Raw IS NOT NULL AND Modulo_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(INT, Modulo_Token_Raw)
            ELSE NULL
        END AS Modulo_Int_Raw
    FROM tokens
),
regla_exacta AS (
    SELECT
        b.*,
        r.Modulo_Int AS Modulo_Int_Exacto,
        r.SubModulo_Int AS SubModulo_Exacto,
        r.Tipo_Conduccion AS Tipo_Conduccion_Exacto,
        ISNULL(r.Es_Test_Block, 0) AS Es_Test_Block_Exacto
    FROM base_norm b
    LEFT JOIN MDM.Regla_Modulo_Raw r
        ON r.Es_Activa = 1
       AND b.Modulo_Token IS NOT NULL
       AND UPPER(LTRIM(RTRIM(r.Modulo_Raw))) = UPPER(b.Modulo_Token)
),
regla_turno AS (
    SELECT
        b.*,
        r.Modulo_Int AS Modulo_Int_Turno,
        r.SubModulo_Int AS SubModulo_Turno,
        r.Tipo_Conduccion AS Tipo_Conduccion_Turno,
        ISNULL(r.Es_Test_Block, 0) AS Es_Test_Block_Turno,
        r.Prioridad
    FROM regla_exacta b
    OUTER APPLY (
        SELECT TOP (1)
            rt.Modulo_Int,
            rt.SubModulo_Int,
            rt.Es_Test_Block,
            rt.Tipo_Conduccion,
            rt.Prioridad
        FROM MDM.Regla_Modulo_Turno_SubModulo rt
        WHERE rt.Es_Activa = 1
          AND b.Modulo_Int_Exacto IS NULL
          AND b.Modulo_Token IS NOT NULL
          AND b.Turno_Int IS NOT NULL
          AND UPPER(LTRIM(RTRIM(rt.Modulo_Raw_Base))) = UPPER(b.Modulo_Token)
          AND b.Turno_Int BETWEEN rt.Turno_Desde AND rt.Turno_Hasta
        ORDER BY rt.Prioridad ASC, rt.Turno_Desde ASC, rt.ID_Regla_Modulo_Turno ASC
    ) r
),
final AS (
    SELECT
        Modulo_Raw,
        Turno_Int,
        Valvula_Token,
        COALESCE(Modulo_Int_Exacto, Modulo_Int_Turno, Modulo_Int_Raw) AS Modulo_Int,
        COALESCE(SubModulo_Exacto, SubModulo_Turno) AS SubModulo_Int,
        COALESCE(Tipo_Conduccion_Exacto, Tipo_Conduccion_Turno) AS Tipo_Conduccion,
        CASE
            WHEN ISNULL(Es_Test_Block_Exacto, 0) = 1 THEN 1
            WHEN ISNULL(Es_Test_Block_Turno, 0) = 1 THEN 1
            ELSE 0
        END AS Es_Test_Block
    FROM regla_turno
),
faltantes AS (
    SELECT DISTINCT
        @fundo_default AS Fundo,
        @sector_default AS Sector,
        f.Modulo_Int AS Modulo,
        f.SubModulo_Int AS SubModulo,
        f.Turno_Int AS Turno,
        f.Valvula_Token AS Valvula,
        CAST(NULL AS NVARCHAR(50)) AS Cama,
        CAST(NULL AS NVARCHAR(50)) AS Codigo_SAP_Campo,
        CAST(0 AS BIT) AS Es_Test_Block,
        CAST(1 AS BIT) AS Es_Activa,
        @fecha_creacion AS Fecha_Creacion,
        f.Tipo_Conduccion AS Tipo_Conduccion
    FROM final f
    WHERE f.Es_Test_Block = 0
      AND f.Modulo_Int IS NOT NULL
      AND f.Turno_Int IS NOT NULL
      AND f.Valvula_Token IS NOT NULL
      AND NOT EXISTS (
            SELECT 1
            FROM Silver.Dim_Geografia g
            WHERE ISNULL(g.Es_Vigente, 1) = 1
              AND ISNULL(g.Es_Test_Block, 0) = 0
              AND g.Modulo = f.Modulo_Int
              AND ISNULL(g.SubModulo, -1) = ISNULL(f.SubModulo_Int, -1)
              AND g.Turno = f.Turno_Int
              AND (
                    CASE
                        WHEN g.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(g.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(g.Valvula)) NOT LIKE '%[^0-9]%'
                            THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(g.Valvula))))
                        ELSE LTRIM(RTRIM(g.Valvula))
                    END
                  ) = f.Valvula_Token
      )
      AND NOT EXISTS (
            SELECT 1
            FROM MDM.Catalogo_Geografia c
            WHERE c.Es_Activa = 1
              AND c.Fundo = @fundo_default
              AND ISNULL(c.Sector, '') = ISNULL(@sector_default, '')
              AND c.Modulo = f.Modulo_Int
              AND ISNULL(c.SubModulo, -1) = ISNULL(f.SubModulo_Int, -1)
              AND ISNULL(c.Turno, -1) = f.Turno_Int
              AND ISNULL(c.Valvula, '') = f.Valvula_Token
      )
)
SELECT *
INTO #faltantes
FROM faltantes;

SELECT
    COUNT(*) AS Total_Faltantes_A_Insertar
FROM #faltantes;

SELECT TOP (200)
    *
FROM #faltantes
ORDER BY Modulo, SubModulo, Turno, Valvula;

IF @modo_aplicar = 1
BEGIN
    INSERT INTO MDM.Catalogo_Geografia (
        Fundo,
        Sector,
        Modulo,
        Turno,
        Valvula,
        Cama,
        Codigo_SAP_Campo,
        Es_Test_Block,
        Es_Activa,
        Fecha_Creacion,
        SubModulo,
        Tipo_Conduccion
    )
    SELECT
        Fundo,
        Sector,
        Modulo,
        Turno,
        Valvula,
        Cama,
        Codigo_SAP_Campo,
        Es_Test_Block,
        Es_Activa,
        Fecha_Creacion,
        SubModulo,
        Tipo_Conduccion
    FROM #faltantes;
END;

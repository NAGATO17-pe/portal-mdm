/*
Fase 13 - Plan 1 ejecutable
===========================
Top N combinaciones NO especiales (Modulo/Turno/Valvula) sin match en Dim_Geografia.
Inserta en MDM.Catalogo_Geografia para que Dim_Geografia las sincronice en la siguiente corrida.

Uso:
  1) Ejecutar en PREVIEW: @modo_aplicar = 0
  2) Revisar resultados
  3) Ejecutar en APPLY:   @modo_aplicar = 1
*/

SET NOCOUNT ON;
GO

DECLARE @modo_aplicar BIT = 0;   -- 0=preview, 1=apply
DECLARE @top_n INT = 100;

IF OBJECT_ID('tempdb..#BronceRaw') IS NOT NULL DROP TABLE #BronceRaw;
IF OBJECT_ID('tempdb..#BaseNoEncontrada') IS NOT NULL DROP TABLE #BaseNoEncontrada;
IF OBJECT_ID('tempdb..#TopN') IS NOT NULL DROP TABLE #TopN;

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

;WITH Base AS (
    SELECT
        b.Tabla_Origen,
        raw.Modulo_Token_Raw,
        raw.Turno_Token_Raw,
        raw.Valvula_Token_Raw,
        regla.Modulo_Int AS Modulo_Regla,
        regla.SubModulo_Int AS SubModulo_Regla,
        ISNULL(regla.Es_Test_Block, 0) AS Es_Test_Block_Regla,
        CASE
            WHEN raw.Modulo_Token_Raw IS NOT NULL AND raw.Modulo_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(INT, raw.Modulo_Token_Raw)
            ELSE NULL
        END AS Modulo_Int_Raw,
        CASE
            WHEN raw.Turno_Token_Raw IS NOT NULL AND raw.Turno_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(INT, raw.Turno_Token_Raw)
            ELSE NULL
        END AS Turno_Int,
        CASE
            WHEN raw.Valvula_Token_Raw IS NULL THEN NULL
            WHEN raw.Valvula_Token_Raw NOT LIKE '%[^0-9]%'
                THEN CONVERT(NVARCHAR(50), CONVERT(INT, raw.Valvula_Token_Raw))
            ELSE raw.Valvula_Token_Raw
        END AS Valvula_Token
    FROM #BronceRaw b
    CROSS APPLY (
        SELECT
            NULLIF(LTRIM(RTRIM(b.Modulo_Raw)), '') AS Modulo_Token_Raw,
            NULLIF(LTRIM(RTRIM(b.Turno_Raw)), '') AS Turno_Token_Raw,
            NULLIF(LTRIM(RTRIM(b.Valvula_Raw)), '') AS Valvula_Token_Raw
    ) raw
    OUTER APPLY (
        SELECT TOP (1)
            r.Modulo_Int,
            r.SubModulo_Int,
            r.Es_Test_Block
        FROM MDM.Regla_Modulo_Raw r
        WHERE r.Es_Activa = 1
          AND raw.Modulo_Token_Raw IS NOT NULL
          AND UPPER(LTRIM(RTRIM(r.Modulo_Raw))) = UPPER(raw.Modulo_Token_Raw)
    ) regla
),
Normalizada AS (
    SELECT
        Tabla_Origen,
        COALESCE(Modulo_Regla, Modulo_Int_Raw) AS Modulo_Int,
        SubModulo_Regla AS SubModulo_Int,
        Turno_Int,
        Valvula_Token,
        CASE
            WHEN Es_Test_Block_Regla = 1 THEN 1
            WHEN COALESCE(Modulo_Regla, Modulo_Int_Raw) IS NULL THEN 1
            ELSE 0
        END AS Es_Modulo_Especial
    FROM Base
),
NoEncontrada AS (
    SELECT
        n.Modulo_Int,
        n.SubModulo_Int,
        n.Turno_Int,
        n.Valvula_Token,
        COUNT(*) AS Filas_Afectadas
    FROM Normalizada n
    OUTER APPLY (
        SELECT
            COUNT(*) AS Coincidencias_Geo
        FROM Silver.Dim_Geografia g
        WHERE ISNULL(g.Es_Vigente, 1) = 1
          AND ISNULL(g.Es_Test_Block, 0) = 0
          AND g.Modulo = n.Modulo_Int
          AND ISNULL(g.SubModulo, -1) = ISNULL(n.SubModulo_Int, -1)
          AND g.Turno = n.Turno_Int
          AND (
                CASE
                    WHEN g.Valvula IS NULL THEN NULL
                    WHEN LTRIM(RTRIM(g.Valvula)) = '' THEN NULL
                    WHEN LTRIM(RTRIM(g.Valvula)) NOT LIKE '%[^0-9]%'
                        THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(g.Valvula))))
                    ELSE LTRIM(RTRIM(g.Valvula))
                END
              ) = n.Valvula_Token
    ) x
    WHERE n.Es_Modulo_Especial = 0
      AND n.Modulo_Int IS NOT NULL
      AND n.Turno_Int IS NOT NULL
      AND n.Valvula_Token IS NOT NULL
      AND ISNULL(x.Coincidencias_Geo, 0) = 0
    GROUP BY
        n.Modulo_Int,
        n.SubModulo_Int,
        n.Turno_Int,
        n.Valvula_Token
)
SELECT
    Modulo_Int,
    SubModulo_Int,
    Turno_Int,
    Valvula_Token,
    Filas_Afectadas
INTO #BaseNoEncontrada
FROM NoEncontrada;

SELECT TOP (@top_n)
    ROW_NUMBER() OVER (ORDER BY Filas_Afectadas DESC, Modulo_Int, Turno_Int, Valvula_Token) AS Prioridad,
    Modulo_Int,
    SubModulo_Int,
    Turno_Int,
    Valvula_Token,
    Filas_Afectadas
INTO #TopN
FROM #BaseNoEncontrada
ORDER BY Filas_Afectadas DESC, Modulo_Int, Turno_Int, Valvula_Token;

DECLARE @filas_insertadas INT = 0;
DECLARE @usa_submodulo_catalogo BIT = CASE WHEN COL_LENGTH('MDM.Catalogo_Geografia', 'SubModulo') IS NULL THEN 0 ELSE 1 END;
DECLARE @usa_tipo_catalogo BIT = CASE WHEN COL_LENGTH('MDM.Catalogo_Geografia', 'Tipo_Conduccion') IS NULL THEN 0 ELSE 1 END;

IF @modo_aplicar = 1
BEGIN
    BEGIN TRANSACTION;

    IF @usa_submodulo_catalogo = 1 AND @usa_tipo_catalogo = 1
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, SubModulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Tipo_Conduccion, Es_Activa, Fecha_Creacion
        )
        SELECT
            N'ARANDANO ACP',
            N'SIN_SECTOR_MAPA',
            t.Modulo_Int,
            t.SubModulo_Int,
            t.Turno_Int,
            t.Valvula_Token,
            NULL,
            0,
            NULL,
            CASE
                WHEN t.SubModulo_Int = 1 THEN N'SUELO'
                WHEN t.SubModulo_Int = 2 THEN N'MACETA'
                ELSE NULL
            END,
            1,
            SYSDATETIME()
        FROM #TopN t
        WHERE NOT EXISTS (
            SELECT 1
            FROM MDM.Catalogo_Geografia c
            WHERE c.Es_Activa = 1
              AND ISNULL(c.Es_Test_Block, 0) = 0
              AND c.Modulo = t.Modulo_Int
              AND ISNULL(c.SubModulo, -1) = ISNULL(t.SubModulo_Int, -1)
              AND c.Turno = t.Turno_Int
              AND (
                    CASE
                        WHEN c.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) NOT LIKE '%[^0-9]%'
                            THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(c.Valvula))))
                        ELSE LTRIM(RTRIM(c.Valvula))
                    END
                  ) = t.Valvula_Token
        );
    END
    ELSE IF @usa_submodulo_catalogo = 1 AND @usa_tipo_catalogo = 0
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, SubModulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Es_Activa, Fecha_Creacion
        )
        SELECT
            N'ARANDANO ACP',
            N'SIN_SECTOR_MAPA',
            t.Modulo_Int,
            t.SubModulo_Int,
            t.Turno_Int,
            t.Valvula_Token,
            NULL,
            0,
            NULL,
            1,
            SYSDATETIME()
        FROM #TopN t
        WHERE NOT EXISTS (
            SELECT 1
            FROM MDM.Catalogo_Geografia c
            WHERE c.Es_Activa = 1
              AND ISNULL(c.Es_Test_Block, 0) = 0
              AND c.Modulo = t.Modulo_Int
              AND ISNULL(c.SubModulo, -1) = ISNULL(t.SubModulo_Int, -1)
              AND c.Turno = t.Turno_Int
              AND (
                    CASE
                        WHEN c.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) NOT LIKE '%[^0-9]%'
                            THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(c.Valvula))))
                        ELSE LTRIM(RTRIM(c.Valvula))
                    END
                  ) = t.Valvula_Token
        );
    END
    ELSE
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Es_Activa, Fecha_Creacion
        )
        SELECT
            N'ARANDANO ACP',
            N'SIN_SECTOR_MAPA',
            t.Modulo_Int,
            t.Turno_Int,
            t.Valvula_Token,
            NULL,
            0,
            NULL,
            1,
            SYSDATETIME()
        FROM #TopN t
        WHERE NOT EXISTS (
            SELECT 1
            FROM MDM.Catalogo_Geografia c
            WHERE c.Es_Activa = 1
              AND ISNULL(c.Es_Test_Block, 0) = 0
              AND c.Modulo = t.Modulo_Int
              AND c.Turno = t.Turno_Int
              AND (
                    CASE
                        WHEN c.Valvula IS NULL THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) = '' THEN NULL
                        WHEN LTRIM(RTRIM(c.Valvula)) NOT LIKE '%[^0-9]%'
                            THEN CONVERT(NVARCHAR(50), CONVERT(INT, LTRIM(RTRIM(c.Valvula))))
                        ELSE LTRIM(RTRIM(c.Valvula))
                    END
                  ) = t.Valvula_Token
        );
    END;

    SET @filas_insertadas = @@ROWCOUNT;
    COMMIT TRANSACTION;
END;

SELECT
    @modo_aplicar AS Modo_Aplicar,
    (SELECT COUNT(*) FROM #BronceRaw) AS Filas_Bronce_Leidas,
    (SELECT COUNT(*) FROM #BaseNoEncontrada) AS Combinaciones_Base_No_Encontradas,
    (SELECT COUNT(*) FROM #TopN) AS Combinaciones_TopN,
    (SELECT ISNULL(SUM(Filas_Afectadas), 0) FROM #TopN) AS Filas_Afectadas_TopN,
    @filas_insertadas AS Insert_MDM_Real;

SELECT
    Prioridad,
    Modulo_Int,
    SubModulo_Int,
    Turno_Int,
    Valvula_Token,
    Filas_Afectadas
FROM #TopN
ORDER BY Prioridad;
GO

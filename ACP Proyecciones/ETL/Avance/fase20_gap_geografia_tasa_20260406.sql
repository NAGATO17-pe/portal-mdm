/*
Fase 20 - Gap geografia para Tasa de Crecimiento (2026-04-06)
Objetivo:
  1) Identificar combinaciones Modulo/Turno/Valvula en Bronce.Tasa_Crecimiento_Brotes
     que no tienen match en Silver.Dim_Geografia, aplicando reglas MDM existentes.
  2) Proponer un candidato de Fundo/Sector basado en otras fuentes Bronce.
Nota:
  - Este script NO inserta datos. Es diagnóstico para decidir el poblamiento de MDM.Catalogo_Geografia.
*/

SET NOCOUNT ON;

PRINT '=== 1) GAP de geografia en Tasa (solo RECHAZADO) ===';

WITH base AS (
    SELECT
        LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))) AS Modulo_Raw,
        TRY_CONVERT(INT, LTRIM(RTRIM(Replace(ISNULL(Turno_Raw,''), ' ', '')))) AS Turno_Int,
        TRY_CONVERT(INT, LTRIM(RTRIM(Replace(ISNULL(Valvula_Raw,''), ' ', '')))) AS Valvula_Int,
        NULLIF(LTRIM(RTRIM(CAST(Valvula_Raw AS NVARCHAR(100)))), '') AS Valvula_Raw,
        NULLIF(LTRIM(RTRIM(CAST(Cama_Raw AS NVARCHAR(100)))), '') AS Cama_Raw,
        Estado_Carga
    FROM Bronce.Tasa_Crecimiento_Brotes
),
regla_exacta AS (
    SELECT b.*, r.Modulo_Int AS Modulo_Regla, r.SubModulo_Int AS SubModulo_Regla, r.Es_Test_Block
    FROM base b
    LEFT JOIN MDM.Regla_Modulo_Raw r
      ON r.Es_Activa = 1
     AND UPPER(LTRIM(RTRIM(r.Modulo_Raw))) = UPPER(b.Modulo_Raw)
),
regla_turno AS (
    SELECT e.*, rt.Modulo_Int AS Modulo_Turno, rt.SubModulo_Int AS SubModulo_Turno
    FROM regla_exacta e
    LEFT JOIN MDM.Regla_Modulo_Turno_SubModulo rt
      ON rt.Es_Activa = 1
     AND e.Modulo_Regla IS NULL
     AND e.Turno_Int IS NOT NULL
     AND UPPER(LTRIM(RTRIM(rt.Modulo_Raw_Base))) = UPPER(e.Modulo_Raw)
     AND e.Turno_Int BETWEEN rt.Turno_Desde AND rt.Turno_Hasta
),
resuelto AS (
    SELECT
        r.*,
        COALESCE(
            r.Modulo_Regla,
            r.Modulo_Turno,
            CASE
                WHEN r.Modulo_Raw NOT LIKE '%[^0-9]%' AND r.Modulo_Raw <> ''
                    THEN TRY_CONVERT(INT, r.Modulo_Raw)
            END
        ) AS Modulo_Final,
        COALESCE(r.SubModulo_Regla, r.SubModulo_Turno) AS SubModulo_Final
    FROM regla_turno r
)
SELECT TOP 50
    r.Modulo_Raw,
    r.Turno_Int,
    r.Valvula_Int,
    r.Modulo_Final,
    r.SubModulo_Final,
    COUNT(*) AS Total
FROM resuelto r
LEFT JOIN Silver.Dim_Geografia g
  ON g.Modulo = r.Modulo_Final
 AND ISNULL(g.SubModulo, -1) = ISNULL(r.SubModulo_Final, -1)
 AND g.Turno = r.Turno_Int
 AND TRY_CONVERT(INT, g.Valvula) = r.Valvula_Int
 AND ISNULL(g.Es_Vigente, 1) = 1
WHERE r.Estado_Carga = 'RECHAZADO'
  AND r.Modulo_Raw <> ''
  AND g.ID_Geografia IS NULL
GROUP BY r.Modulo_Raw, r.Turno_Int, r.Valvula_Int, r.Modulo_Final, r.SubModulo_Final
ORDER BY Total DESC;

PRINT '=== 2) Resumen por Modulo_Raw (RECHAZADO sin geografia) ===';

WITH gap AS (
    SELECT
        r.Modulo_Raw,
        r.Turno_Int,
        r.Valvula_Int,
        r.Modulo_Final,
        r.SubModulo_Final
    FROM (
        SELECT
            LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))) AS Modulo_Raw,
            TRY_CONVERT(INT, LTRIM(RTRIM(Replace(ISNULL(Turno_Raw,''), ' ', '')))) AS Turno_Int,
            TRY_CONVERT(INT, LTRIM(RTRIM(Replace(ISNULL(Valvula_Raw,''), ' ', '')))) AS Valvula_Int,
            Estado_Carga,
            Modulo_Final,
            SubModulo_Final
        FROM (
            SELECT
                r.*,
                COALESCE(
                    r.Modulo_Regla,
                    r.Modulo_Turno,
                    CASE
                        WHEN r.Modulo_Raw NOT LIKE '%[^0-9]%' AND r.Modulo_Raw <> ''
                            THEN TRY_CONVERT(INT, r.Modulo_Raw)
                    END
                ) AS Modulo_Final,
                COALESCE(r.SubModulo_Regla, r.SubModulo_Turno) AS SubModulo_Final
            FROM regla_turno r
        ) x
    ) r
    LEFT JOIN Silver.Dim_Geografia g
      ON g.Modulo = r.Modulo_Final
     AND ISNULL(g.SubModulo, -1) = ISNULL(r.SubModulo_Final, -1)
     AND g.Turno = r.Turno_Int
     AND TRY_CONVERT(INT, g.Valvula) = r.Valvula_Int
     AND ISNULL(g.Es_Vigente, 1) = 1
    WHERE r.Estado_Carga = 'RECHAZADO'
      AND r.Modulo_Raw <> ''
      AND g.ID_Geografia IS NULL
)
SELECT Modulo_Raw, COUNT(*) AS Total_Filas
FROM gap
GROUP BY Modulo_Raw
ORDER BY Total_Filas DESC, Modulo_Raw;

PRINT '=== 3) Propuesta de Fundo/Sector por frecuencia (requiere confirmación de negocio) ===';

WITH fuentes AS (
    SELECT Modulo_Raw, Turno_Raw, Valvula_Raw, Fundo_Raw, Sector_Raw
    FROM Bronce.Conteo_Fruta
    WHERE Fundo_Raw IS NOT NULL
    UNION ALL
    SELECT Modulo_Raw, Turno_Raw, Valvula_Raw, Fundo_Raw, NULL AS Sector_Raw
    FROM Bronce.Evaluacion_Pesos
    WHERE Fundo_Raw IS NOT NULL
    UNION ALL
    SELECT Modulo_Raw, Turno_Raw, Valvula_Raw, Fundo_Raw, NULL AS Sector_Raw
    FROM Bronce.Evaluacion_Calidad_Poda
    WHERE Fundo_Raw IS NOT NULL
    UNION ALL
    SELECT Modulo_Raw, Turno_Raw, Valvula_Raw, Fundo_Raw, NULL AS Sector_Raw
    FROM Bronce.Fisiologia
    WHERE Fundo_Raw IS NOT NULL
),
fuentes_base AS (
    SELECT
        LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))) AS Modulo_Raw,
        TRY_CONVERT(INT, LTRIM(RTRIM(Replace(ISNULL(Turno_Raw,''), ' ', '')))) AS Turno_Int,
        TRY_CONVERT(INT, LTRIM(RTRIM(Replace(ISNULL(Valvula_Raw,''), ' ', '')))) AS Valvula_Int,
        LTRIM(RTRIM(Fundo_Raw)) AS Fundo_Raw,
        LTRIM(RTRIM(Sector_Raw)) AS Sector_Raw
    FROM fuentes
),
fuentes_regla_exacta AS (
    SELECT b.*, r.Modulo_Int AS Modulo_Regla, r.SubModulo_Int AS SubModulo_Regla
    FROM fuentes_base b
    LEFT JOIN MDM.Regla_Modulo_Raw r
      ON r.Es_Activa = 1
     AND UPPER(LTRIM(RTRIM(r.Modulo_Raw))) = UPPER(b.Modulo_Raw)
),
fuentes_regla_turno AS (
    SELECT e.*, rt.Modulo_Int AS Modulo_Turno, rt.SubModulo_Int AS SubModulo_Turno
    FROM fuentes_regla_exacta e
    LEFT JOIN MDM.Regla_Modulo_Turno_SubModulo rt
      ON rt.Es_Activa = 1
     AND e.Modulo_Regla IS NULL
     AND e.Turno_Int IS NOT NULL
     AND UPPER(LTRIM(RTRIM(rt.Modulo_Raw_Base))) = UPPER(e.Modulo_Raw)
     AND e.Turno_Int BETWEEN rt.Turno_Desde AND rt.Turno_Hasta
),
fuentes_norm AS (
    SELECT
        f.*,
        COALESCE(
            f.Modulo_Regla,
            f.Modulo_Turno,
            CASE
                WHEN f.Modulo_Raw NOT LIKE '%[^0-9]%' AND f.Modulo_Raw <> ''
                    THEN TRY_CONVERT(INT, f.Modulo_Raw)
            END
        ) AS Modulo_Final,
        COALESCE(f.SubModulo_Regla, f.SubModulo_Turno) AS SubModulo_Final
    FROM fuentes_regla_turno f
),
gap AS (
    SELECT
        r.Modulo_Raw,
        r.Turno_Int,
        r.Valvula_Int,
        r.Modulo_Final,
        r.SubModulo_Final
    FROM (
        SELECT
            r.*,
            COALESCE(
                r.Modulo_Regla,
                r.Modulo_Turno,
                CASE
                    WHEN r.Modulo_Raw NOT LIKE '%[^0-9]%' AND r.Modulo_Raw <> ''
                        THEN TRY_CONVERT(INT, r.Modulo_Raw)
                END
            ) AS Modulo_Final,
            COALESCE(r.SubModulo_Regla, r.SubModulo_Turno) AS SubModulo_Final
        FROM regla_turno r
    ) r
    LEFT JOIN Silver.Dim_Geografia g
      ON g.Modulo = r.Modulo_Final
     AND ISNULL(g.SubModulo, -1) = ISNULL(r.SubModulo_Final, -1)
     AND g.Turno = r.Turno_Int
     AND TRY_CONVERT(INT, g.Valvula) = r.Valvula_Int
     AND ISNULL(g.Es_Vigente, 1) = 1
    WHERE r.Estado_Carga = 'RECHAZADO'
      AND r.Modulo_Raw <> ''
      AND g.ID_Geografia IS NULL
),
fundo_sugerido AS (
    SELECT
        g.Modulo_Raw,
        g.Turno_Int,
        g.Valvula_Int,
        g.Modulo_Final,
        g.SubModulo_Final,
        f.Fundo_Raw,
        f.Sector_Raw,
        COUNT(*) AS Total
    FROM gap g
    JOIN fuentes_norm f
      ON f.Modulo_Final = g.Modulo_Final
     AND ISNULL(f.SubModulo_Final, -1) = ISNULL(g.SubModulo_Final, -1)
     AND f.Turno_Int = g.Turno_Int
     AND f.Valvula_Int = g.Valvula_Int
    GROUP BY g.Modulo_Raw, g.Turno_Int, g.Valvula_Int, g.Modulo_Final, g.SubModulo_Final, f.Fundo_Raw, f.Sector_Raw
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Modulo_Raw, Turno_Int, Valvula_Int
               ORDER BY Total DESC, Fundo_Raw
           ) AS rn
    FROM fundo_sugerido
)
SELECT TOP 50
    Modulo_Raw,
    Turno_Int,
    Valvula_Int,
    Modulo_Final,
    SubModulo_Final,
    Fundo_Raw AS Fundo_Sugerido,
    Sector_Raw AS Sector_Sugerido,
    Total AS Soporte_Filas
FROM ranked
WHERE rn = 1
ORDER BY Soporte_Filas DESC, Modulo_Raw, Turno_Int, Valvula_Int;

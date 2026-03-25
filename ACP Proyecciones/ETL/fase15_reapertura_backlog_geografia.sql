/*
Fase 15 - Reapertura Backlog Geografia (Pesos/Vegetativa)
Objetivo:
- Reabrir filas historicas para reproceso geografia en Bronce,
  sin depender de ID_Registro_Origen historico.
- Comparacion normalizada para cubrir 01 vs 1 en Turno/Valvula/Cama.

Notas:
- Alcance solo geografia especial.
- No incluye variedades en esta fase.
- Reabre por cuota (cantidad pendiente por firma) para evitar reapertura masiva.
*/

SET NOCOUNT ON;

DECLARE @Pesos_Reabiertos INT = 0;
DECLARE @Vegetativa_Reabiertos INT = 0;

/* =========================
   PESOS
   ========================= */
;WITH cuarentena_base AS (
    SELECT Q.Valor_Recibido
    FROM MDM.Cuarentena Q
    WHERE Q.Estado = 'PENDIENTE'
      AND Q.Tabla_Origen = 'Bronce.Evaluacion_Pesos'
      AND Q.Motivo IN (N'Geografia especial requiere catalogacion o regla en MDM_Geografia.', N'Geografia no encontrada en Silver.Dim_Geografia.')
),
cuarentena_parse AS (
    SELECT
        Modulo_Token = CASE
            WHEN CHARINDEX('Modulo=', Valor_Recibido) > 0
             AND CHARINDEX('Turno=', Valor_Recibido) > CHARINDEX('Modulo=', Valor_Recibido)
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Modulo=', Valor_Recibido) + LEN('Modulo='),
                    CHARINDEX('Turno=', Valor_Recibido) - (CHARINDEX('Modulo=', Valor_Recibido) + LEN('Modulo='))
                 )))
            ELSE NULL
        END,
        Turno_Token = CASE
            WHEN CHARINDEX('Turno=', Valor_Recibido) > 0
             AND CHARINDEX('Valvula=', Valor_Recibido) > CHARINDEX('Turno=', Valor_Recibido)
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Turno=', Valor_Recibido) + LEN('Turno='),
                    CHARINDEX('Valvula=', Valor_Recibido) - (CHARINDEX('Turno=', Valor_Recibido) + LEN('Turno='))
                 )))
            ELSE NULL
        END,
        Valvula_Token = CASE
            WHEN CHARINDEX('Valvula=', Valor_Recibido) > 0
             AND CHARINDEX('Cama=', Valor_Recibido) > CHARINDEX('Valvula=', Valor_Recibido)
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Valvula=', Valor_Recibido) + LEN('Valvula='),
                    CHARINDEX('Cama=', Valor_Recibido) - (CHARINDEX('Valvula=', Valor_Recibido) + LEN('Valvula='))
                 )))
            ELSE NULL
        END,
        Cama_Token = CASE
            WHEN CHARINDEX('Cama=', Valor_Recibido) > 0
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Cama=', Valor_Recibido) + LEN('Cama='),
                    LEN(Valor_Recibido)
                 )))
            ELSE NULL
        END
    FROM cuarentena_base
),
cuarentena_firma AS (
    SELECT
        Firma_Clave = CONCAT(
            COALESCE(UPPER(NULLIF(LTRIM(RTRIM(Modulo_Token)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Turno_Token)), ''))), UPPER(NULLIF(LTRIM(RTRIM(Turno_Token)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Valvula_Token)), ''))), UPPER(NULLIF(LTRIM(RTRIM(Valvula_Token)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Cama_Token)), ''))), UPPER(NULLIF(LTRIM(RTRIM(Cama_Token)), '')), N'∅')
        )
    FROM cuarentena_parse
),
cuarentena_cuota AS (
    SELECT Firma_Clave, COUNT(*) AS Pendientes
    FROM cuarentena_firma
    GROUP BY Firma_Clave
),
bronce_firma AS (
    SELECT
        B.ID_Evaluacion_Pesos,
        B.Estado_Carga,
        Firma_Clave = CONCAT(
            COALESCE(UPPER(NULLIF(LTRIM(RTRIM(B.Modulo_Raw)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(B.Turno_Raw)), ''))), UPPER(NULLIF(LTRIM(RTRIM(B.Turno_Raw)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(B.Valvula_Raw)), ''))), UPPER(NULLIF(LTRIM(RTRIM(B.Valvula_Raw)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(B.Cama_Raw)), ''))), UPPER(NULLIF(LTRIM(RTRIM(B.Cama_Raw)), '')), N'∅')
        )
    FROM Bronce.Evaluacion_Pesos B
    WHERE B.Estado_Carga <> 'CARGADO'
),
target_pesos AS (
    SELECT
        BF.ID_Evaluacion_Pesos,
        ROW_NUMBER() OVER (PARTITION BY BF.Firma_Clave ORDER BY BF.ID_Evaluacion_Pesos DESC) AS rn,
        QC.Pendientes
    FROM bronce_firma BF
    JOIN cuarentena_cuota QC
      ON QC.Firma_Clave = BF.Firma_Clave
)
UPDATE B
SET B.Estado_Carga = 'CARGADO'
FROM Bronce.Evaluacion_Pesos B
JOIN target_pesos T
  ON T.ID_Evaluacion_Pesos = B.ID_Evaluacion_Pesos
WHERE T.rn <= T.Pendientes;

SET @Pesos_Reabiertos = @@ROWCOUNT;

/* =========================
   VEGETATIVA
   ========================= */
;WITH cuarentena_base AS (
    SELECT Q.Valor_Recibido
    FROM MDM.Cuarentena Q
    WHERE Q.Estado = 'PENDIENTE'
      AND Q.Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
      AND Q.Motivo IN (N'Geografia especial requiere catalogacion o regla en MDM_Geografia.', N'Geografia no encontrada en Silver.Dim_Geografia.')
),
cuarentena_parse AS (
    SELECT
        Modulo_Token = CASE
            WHEN CHARINDEX('Modulo=', Valor_Recibido) > 0
             AND CHARINDEX('Turno=', Valor_Recibido) > CHARINDEX('Modulo=', Valor_Recibido)
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Modulo=', Valor_Recibido) + LEN('Modulo='),
                    CHARINDEX('Turno=', Valor_Recibido) - (CHARINDEX('Modulo=', Valor_Recibido) + LEN('Modulo='))
                 )))
            ELSE NULL
        END,
        Turno_Token = CASE
            WHEN CHARINDEX('Turno=', Valor_Recibido) > 0
             AND CHARINDEX('Valvula=', Valor_Recibido) > CHARINDEX('Turno=', Valor_Recibido)
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Turno=', Valor_Recibido) + LEN('Turno='),
                    CHARINDEX('Valvula=', Valor_Recibido) - (CHARINDEX('Turno=', Valor_Recibido) + LEN('Turno='))
                 )))
            ELSE NULL
        END,
        Valvula_Token = CASE
            WHEN CHARINDEX('Valvula=', Valor_Recibido) > 0
             AND CHARINDEX('Cama=', Valor_Recibido) > CHARINDEX('Valvula=', Valor_Recibido)
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Valvula=', Valor_Recibido) + LEN('Valvula='),
                    CHARINDEX('Cama=', Valor_Recibido) - (CHARINDEX('Valvula=', Valor_Recibido) + LEN('Valvula='))
                 )))
            ELSE NULL
        END,
        Cama_Token = CASE
            WHEN CHARINDEX('Cama=', Valor_Recibido) > 0
            THEN LTRIM(RTRIM(SUBSTRING(
                    Valor_Recibido,
                    CHARINDEX('Cama=', Valor_Recibido) + LEN('Cama='),
                    LEN(Valor_Recibido)
                 )))
            ELSE NULL
        END
    FROM cuarentena_base
),
cuarentena_firma AS (
    SELECT
        Firma_Clave = CONCAT(
            COALESCE(UPPER(NULLIF(LTRIM(RTRIM(Modulo_Token)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Turno_Token)), ''))), UPPER(NULLIF(LTRIM(RTRIM(Turno_Token)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Valvula_Token)), ''))), UPPER(NULLIF(LTRIM(RTRIM(Valvula_Token)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Cama_Token)), ''))), UPPER(NULLIF(LTRIM(RTRIM(Cama_Token)), '')), N'∅')
        )
    FROM cuarentena_parse
),
cuarentena_cuota AS (
    SELECT Firma_Clave, COUNT(*) AS Pendientes
    FROM cuarentena_firma
    GROUP BY Firma_Clave
),
bronce_firma AS (
    SELECT
        B.ID_Evaluacion_Vegetativa,
        B.Estado_Carga,
        Firma_Clave = CONCAT(
            COALESCE(UPPER(NULLIF(LTRIM(RTRIM(B.Modulo_Raw)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(B.Turno_Raw)), ''))), UPPER(NULLIF(LTRIM(RTRIM(B.Turno_Raw)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(B.Valvula_Raw)), ''))), UPPER(NULLIF(LTRIM(RTRIM(B.Valvula_Raw)), '')), N'∅'), N'|',
            COALESCE(CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(B.Cama_Raw)), ''))), UPPER(NULLIF(LTRIM(RTRIM(B.Cama_Raw)), '')), N'∅')
        )
    FROM Bronce.Evaluacion_Vegetativa B
    WHERE B.Estado_Carga <> 'CARGADO'
),
target_veg AS (
    SELECT
        BF.ID_Evaluacion_Vegetativa,
        ROW_NUMBER() OVER (PARTITION BY BF.Firma_Clave ORDER BY BF.ID_Evaluacion_Vegetativa DESC) AS rn,
        QC.Pendientes
    FROM bronce_firma BF
    JOIN cuarentena_cuota QC
      ON QC.Firma_Clave = BF.Firma_Clave
)
UPDATE B
SET B.Estado_Carga = 'CARGADO'
FROM Bronce.Evaluacion_Vegetativa B
JOIN target_veg T
  ON T.ID_Evaluacion_Vegetativa = B.ID_Evaluacion_Vegetativa
WHERE T.rn <= T.Pendientes;

SET @Vegetativa_Reabiertos = @@ROWCOUNT;

SELECT
    @Pesos_Reabiertos AS Pesos_Reabiertos,
    @Vegetativa_Reabiertos AS Vegetativa_Reabiertos;


/*
Fase 17 - Catalogar geografia faltante para 9.2 (Turno 10/11, Valvula 1/2/3)
=============================================================================
Objetivo:
- Insertar en MDM.Catalogo_Geografia las combinaciones faltantes de geografia
  que hoy quedan en cuarentena como GEOGRAFIA_NO_ENCONTRADA.

Importante:
- NO toca token 9. (sin submodulo). Ese caso sigue en cuarentena por diseno.
- Usa modo preview/apply.

Uso:
- @modo_aplicar = 0 -> preview
- @modo_aplicar = 1 -> apply
*/

SET NOCOUNT ON;

DECLARE @modo_aplicar BIT = 0;
DECLARE @solo_turnos_10_11 BIT = 1;
DECLARE @solo_valvulas_1_2_3 BIT = 1;

DECLARE @usa_submodulo BIT = CASE WHEN COL_LENGTH('MDM.Catalogo_Geografia', 'SubModulo') IS NULL THEN 0 ELSE 1 END;
DECLARE @usa_tipo BIT = CASE WHEN COL_LENGTH('MDM.Catalogo_Geografia', 'Tipo_Conduccion') IS NULL THEN 0 ELSE 1 END;

DECLARE @fundo_base NVARCHAR(100) = N'ARANDANO ACP';
DECLARE @sector_base NVARCHAR(100) = N'OPERATIVO';
DECLARE @codigo_sap_base NVARCHAR(50) = NULL;
DECLARE @tipo_conduccion_base NVARCHAR(50) = N'MACETA';

DECLARE @insert_real INT = 0;

/* Tomar defaults operativos desde catalogo vigente 9.2 si existe */
IF @usa_submodulo = 1
BEGIN
    SELECT TOP (1)
        @fundo_base = COALESCE(NULLIF(LTRIM(RTRIM(Fundo)), ''), @fundo_base),
        @sector_base = COALESCE(NULLIF(LTRIM(RTRIM(Sector)), ''), @sector_base),
        @codigo_sap_base = Codigo_SAP_Campo,
        @tipo_conduccion_base = CASE
            WHEN @usa_tipo = 1 THEN COALESCE(NULLIF(LTRIM(RTRIM(Tipo_Conduccion)), ''), @tipo_conduccion_base)
            ELSE @tipo_conduccion_base
        END
    FROM MDM.Catalogo_Geografia
    WHERE Es_Activa = 1
      AND ISNULL(Es_Test_Block, 0) = 0
      AND Modulo = 9
      AND SubModulo = 2;
END
ELSE
BEGIN
    SELECT TOP (1)
        @fundo_base = COALESCE(NULLIF(LTRIM(RTRIM(Fundo)), ''), @fundo_base),
        @sector_base = COALESCE(NULLIF(LTRIM(RTRIM(Sector)), ''), @sector_base),
        @codigo_sap_base = Codigo_SAP_Campo
    FROM MDM.Catalogo_Geografia
    WHERE Es_Activa = 1
      AND ISNULL(Es_Test_Block, 0) = 0
      AND Modulo = 9;
END

IF OBJECT_ID('tempdb..#Candidatos') IS NOT NULL DROP TABLE #Candidatos;
IF OBJECT_ID('tempdb..#Faltantes') IS NOT NULL DROP TABLE #Faltantes;

;WITH BaseQ AS (
    SELECT
        Q.Tabla_Origen,
        Q.Valor_Recibido,
        txt = REPLACE(REPLACE(Q.Valor_Recibido, ' ', ''), 'Módulo=', 'Modulo=')
    FROM MDM.Cuarentena Q
    WHERE Q.Estado = 'PENDIENTE'
      AND Q.Tabla_Origen IN ('Bronce.Evaluacion_Pesos', 'Bronce.Evaluacion_Vegetativa')
      AND Q.Motivo IN (
          N'Geografia especial requiere catalogacion o regla en MDM_Geografia.',
          N'Geografia no encontrada en Silver.Dim_Geografia.'
      )
), ParseQ AS (
    SELECT
        Tabla_Origen,
        Modulo_Token = CASE
            WHEN i.pos_mod > 0 AND i.pos_tur > i.pos_mod
            THEN SUBSTRING(txt, i.pos_mod + 7, i.pos_tur - (i.pos_mod + 7))
            ELSE NULL
        END,
        Turno_Token = CASE
            WHEN i.pos_tur > 0 AND i.pos_val > i.pos_tur
            THEN SUBSTRING(txt, i.pos_tur + 7, i.pos_val - (i.pos_tur + 7))
            ELSE NULL
        END,
        Valvula_Token = CASE
            WHEN i.pos_val > 0 AND i.pos_cam > i.pos_val
            THEN SUBSTRING(txt, i.pos_val + 9, i.pos_cam - (i.pos_val + 9))
            ELSE NULL
        END,
        Cama_Token = CASE
            WHEN i.pos_cam > 0
            THEN SUBSTRING(txt, i.pos_cam + 6, LEN(txt) - (i.pos_cam + 6) + 1)
            ELSE NULL
        END
    FROM BaseQ
    CROSS APPLY (
        SELECT
            pos_mod = CHARINDEX('Modulo=', txt),
            pos_tur = CHARINDEX('|Turno=', txt),
            pos_val = CHARINDEX('|Valvula=', txt),
            pos_cam = CHARINDEX('|Cama=', txt)
    ) i
), Normalizada AS (
    SELECT
        Modulo_Token = UPPER(LTRIM(RTRIM(Modulo_Token))),
        Turno_Int = TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Turno_Token)), '')),
        Valvula_Token = CASE
            WHEN TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Valvula_Token)), '')) IS NOT NULL
            THEN CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Valvula_Token)), '')))
            ELSE NULLIF(LTRIM(RTRIM(Valvula_Token)), '')
        END,
        Cama_Token = CASE
            WHEN TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Cama_Token)), '')) IS NOT NULL
            THEN CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(Cama_Token)), '')))
            ELSE NULLIF(LTRIM(RTRIM(Cama_Token)), '')
        END
    FROM ParseQ
)
SELECT
    Turno_Int,
    Valvula_Token,
    COUNT(*) AS Filas_Afectadas,
    COUNT(DISTINCT Cama_Token) AS Camas_Distintas
INTO #Candidatos
FROM Normalizada
WHERE Modulo_Token = '9.2'
  AND Turno_Int IS NOT NULL
  AND Valvula_Token IS NOT NULL
  AND (@solo_turnos_10_11 = 0 OR Turno_Int IN (10, 11))
  AND (@solo_valvulas_1_2_3 = 0 OR Valvula_Token IN ('1', '2', '3'))
GROUP BY Turno_Int, Valvula_Token;

;WITH Vigentes AS (
    SELECT
        Turno_Int = C.Turno,
        Valvula_Token = CASE
            WHEN TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(C.Valvula)), '')) IS NOT NULL
            THEN CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(C.Valvula)), '')))
            ELSE NULLIF(LTRIM(RTRIM(C.Valvula)), '')
        END
    FROM MDM.Catalogo_Geografia C
    WHERE C.Es_Activa = 1
      AND ISNULL(C.Es_Test_Block, 0) = 0
      AND C.Modulo = 9
      AND (
            @usa_submodulo = 0
            OR C.SubModulo = 2
          )
)
SELECT
    C.Turno_Int,
    C.Valvula_Token,
    C.Filas_Afectadas,
    C.Camas_Distintas
INTO #Faltantes
FROM #Candidatos C
LEFT JOIN Vigentes V
    ON V.Turno_Int = C.Turno_Int
   AND V.Valvula_Token = C.Valvula_Token
WHERE V.Turno_Int IS NULL;

/* Preview principal */
SELECT
    @modo_aplicar AS Modo_Aplicar,
    @solo_turnos_10_11 AS Solo_Turnos_10_11,
    @solo_valvulas_1_2_3 AS Solo_Valvulas_1_2_3,
    (SELECT COUNT(*) FROM #Candidatos) AS Combos_Candidatos,
    (SELECT ISNULL(SUM(Filas_Afectadas),0) FROM #Candidatos) AS Filas_Afectadas_Candidatas,
    (SELECT COUNT(*) FROM #Faltantes) AS Combos_Faltantes_Para_Insert,
    (SELECT ISNULL(SUM(Filas_Afectadas),0) FROM #Faltantes) AS Filas_Afectadas_Faltantes;

SELECT
    Turno_Int,
    Valvula_Token,
    Filas_Afectadas,
    Camas_Distintas
FROM #Faltantes
ORDER BY Filas_Afectadas DESC, Turno_Int, Valvula_Token;

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
            @fundo_base,
            @sector_base,
            9,
            2,
            F.Turno_Int,
            F.Valvula_Token,
            NULL,
            0,
            @codigo_sap_base,
            @tipo_conduccion_base,
            1,
            SYSDATETIME()
        FROM #Faltantes F;
    END
    ELSE IF @usa_submodulo = 1 AND @usa_tipo = 0
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, SubModulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Es_Activa, Fecha_Creacion
        )
        SELECT
            @fundo_base,
            @sector_base,
            9,
            2,
            F.Turno_Int,
            F.Valvula_Token,
            NULL,
            0,
            @codigo_sap_base,
            1,
            SYSDATETIME()
        FROM #Faltantes F;
    END
    ELSE
    BEGIN
        INSERT INTO MDM.Catalogo_Geografia (
            Fundo, Sector, Modulo, Turno, Valvula, Cama,
            Es_Test_Block, Codigo_SAP_Campo, Es_Activa, Fecha_Creacion
        )
        SELECT
            @fundo_base,
            @sector_base,
            9,
            F.Turno_Int,
            F.Valvula_Token,
            NULL,
            0,
            @codigo_sap_base,
            1,
            SYSDATETIME()
        FROM #Faltantes F;
    END;

    SET @insert_real = @@ROWCOUNT;

    COMMIT TRANSACTION;
END;

SELECT
    @insert_real AS Insert_MDM_Real,
    @fundo_base AS Fundo_Usado,
    @sector_base AS Sector_Usado,
    @tipo_conduccion_base AS Tipo_Conduccion_Usado;

/* Verificacion rapida post-insert */
SELECT
    C.Turno,
    C.Valvula,
    C.Modulo,
    CASE WHEN @usa_submodulo = 1 THEN C.SubModulo ELSE NULL END AS SubModulo,
    C.Es_Activa,
    C.Es_Test_Block
FROM MDM.Catalogo_Geografia C
WHERE C.Es_Activa = 1
  AND C.Modulo = 9
  AND (@usa_submodulo = 0 OR C.SubModulo = 2)
  AND C.Turno IN (10, 11)
  AND (
        CASE
            WHEN TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(C.Valvula)), '')) IS NOT NULL
            THEN CONVERT(NVARCHAR(50), TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(C.Valvula)), '')))
            ELSE NULLIF(LTRIM(RTRIM(C.Valvula)), '')
        END
      ) IN ('1','2','3')
ORDER BY C.Turno, C.Valvula;

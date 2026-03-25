/*
Fase 15 - Cierre de cargas Pesos/Vegetativa (Fase 1)
Objetivo:
1) Canonizar reglas MDM.Regla_Modulo_Raw para VI / 9.1 / 9.2.
2) Validar resolver VI (Turno+Valvula, ignorando cama).
3) Reabrir solo registros de Bronce afectados por geografia especial.
4) Opcional: reabrir tambien registros por variedad no reconocida.

Notas:
- No fuerza token 9. (sin submodulo). Se mantiene en cuarentena.
- Idempotente: se puede ejecutar varias veces.
*/

SET NOCOUNT ON;

DECLARE @incluir_variedades BIT = 0; -- 0=solo geografia especial, 1=incluye variedad no reconocida

BEGIN TRY
    BEGIN TRAN;

    /* =============================================
       1) Canonizacion reglas Modulo_Raw
       ============================================= */

    -- VI
    UPDATE MDM.Regla_Modulo_Raw
    SET Es_Activa = 0,
        Fecha_Modificacion = SYSDATETIME(),
        Observacion = N'Desactivada por canonizacion Fase 15'
    WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = 'VI'
      AND Es_Activa = 1;

    IF EXISTS (
        SELECT 1
        FROM MDM.Regla_Modulo_Raw
        WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = 'VI'
    )
    BEGIN
        ;WITH x AS (
            SELECT TOP (1) *
            FROM MDM.Regla_Modulo_Raw
            WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = 'VI'
            ORDER BY ID_Regla_Modulo DESC
        )
        UPDATE x
        SET Modulo_Raw = N'VI',
            Modulo_Int = NULL,
            SubModulo_Int = NULL,
            Tipo_Conduccion = N'TEST_BLOCK',
            Es_Test_Block = 1,
            Es_Activa = 1,
            Fecha_Modificacion = SYSDATETIME(),
            Observacion = N'Canonica Fase 15: VI => TEST_BLOCK';
    END
    ELSE
    BEGIN
        INSERT INTO MDM.Regla_Modulo_Raw (
            Modulo_Raw,
            Modulo_Int,
            SubModulo_Int,
            Tipo_Conduccion,
            Es_Test_Block,
            Es_Activa,
            Fecha_Creacion,
            Fecha_Modificacion,
            Observacion
        )
        VALUES (
            N'VI',
            NULL,
            NULL,
            N'TEST_BLOCK',
            1,
            1,
            SYSDATETIME(),
            SYSDATETIME(),
            N'Canonica Fase 15: VI => TEST_BLOCK'
        );
    END;

    -- 9.1
    UPDATE MDM.Regla_Modulo_Raw
    SET Es_Activa = 0,
        Fecha_Modificacion = SYSDATETIME(),
        Observacion = N'Desactivada por canonizacion Fase 15'
    WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.1'
      AND Es_Activa = 1;

    IF EXISTS (
        SELECT 1
        FROM MDM.Regla_Modulo_Raw
        WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.1'
    )
    BEGIN
        ;WITH x AS (
            SELECT TOP (1) *
            FROM MDM.Regla_Modulo_Raw
            WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.1'
            ORDER BY ID_Regla_Modulo DESC
        )
        UPDATE x
        SET Modulo_Raw = N'9.1',
            Modulo_Int = 9,
            SubModulo_Int = 1,
            Tipo_Conduccion = N'SUELO',
            Es_Test_Block = 0,
            Es_Activa = 1,
            Fecha_Modificacion = SYSDATETIME(),
            Observacion = N'Canonica Fase 15: 9.1 => Modulo 9 SubModulo 1';
    END
    ELSE
    BEGIN
        INSERT INTO MDM.Regla_Modulo_Raw (
            Modulo_Raw,
            Modulo_Int,
            SubModulo_Int,
            Tipo_Conduccion,
            Es_Test_Block,
            Es_Activa,
            Fecha_Creacion,
            Fecha_Modificacion,
            Observacion
        )
        VALUES (
            N'9.1',
            9,
            1,
            N'SUELO',
            0,
            1,
            SYSDATETIME(),
            SYSDATETIME(),
            N'Canonica Fase 15: 9.1 => Modulo 9 SubModulo 1'
        );
    END;

    -- 9.2
    UPDATE MDM.Regla_Modulo_Raw
    SET Es_Activa = 0,
        Fecha_Modificacion = SYSDATETIME(),
        Observacion = N'Desactivada por canonizacion Fase 15'
    WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.2'
      AND Es_Activa = 1;

    IF EXISTS (
        SELECT 1
        FROM MDM.Regla_Modulo_Raw
        WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.2'
    )
    BEGIN
        ;WITH x AS (
            SELECT TOP (1) *
            FROM MDM.Regla_Modulo_Raw
            WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) = '9.2'
            ORDER BY ID_Regla_Modulo DESC
        )
        UPDATE x
        SET Modulo_Raw = N'9.2',
            Modulo_Int = 9,
            SubModulo_Int = 2,
            Tipo_Conduccion = N'MACETA',
            Es_Test_Block = 0,
            Es_Activa = 1,
            Fecha_Modificacion = SYSDATETIME(),
            Observacion = N'Canonica Fase 15: 9.2 => Modulo 9 SubModulo 2';
    END
    ELSE
    BEGIN
        INSERT INTO MDM.Regla_Modulo_Raw (
            Modulo_Raw,
            Modulo_Int,
            SubModulo_Int,
            Tipo_Conduccion,
            Es_Test_Block,
            Es_Activa,
            Fecha_Creacion,
            Fecha_Modificacion,
            Observacion
        )
        VALUES (
            N'9.2',
            9,
            2,
            N'MACETA',
            0,
            1,
            SYSDATETIME(),
            SYSDATETIME(),
            N'Canonica Fase 15: 9.2 => Modulo 9 SubModulo 2'
        );
    END;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    THROW;
END CATCH;
GO

/* =============================================
   2) Smoke tests resolver para VI (ignora cama)
   ============================================= */

EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='0';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='1';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='2';
GO

/* =============================================
   3) Reapertura dirigida en Bronce
   ============================================= */

-- GO reinicia el batch; se redeclara el flag para esta seccion.
DECLARE @incluir_variedades BIT = 0; -- 0=solo geografia especial, 1=incluye variedad no reconocida

UPDATE B
SET B.Estado_Carga = 'CARGADO'
FROM Bronce.Evaluacion_Pesos B
JOIN MDM.Cuarentena Q
  ON Q.Tabla_Origen = 'Bronce.Evaluacion_Pesos'
 AND Q.ID_Registro_Origen = B.ID_Evaluacion_Pesos
WHERE Q.Estado = 'PENDIENTE'
  AND (
      Q.Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
      OR (
          @incluir_variedades = 1
          AND Q.Motivo = N'Variedad no reconocida — requiere revisión en MDM'
      )
  );

UPDATE B
SET B.Estado_Carga = 'CARGADO'
FROM Bronce.Evaluacion_Vegetativa B
JOIN MDM.Cuarentena Q
  ON Q.Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
 AND Q.ID_Registro_Origen = B.ID_Evaluacion_Vegetativa
WHERE Q.Estado = 'PENDIENTE'
  AND (
      Q.Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
      OR (
          @incluir_variedades = 1
          AND Q.Motivo = N'Variedad no reconocida — requiere revisión en MDM'
      )
  );
GO

/* =============================================
   4) Control rapido de pendientes geografia
   ============================================= */

SELECT
    Tabla_Origen,
    COUNT(*) AS Pendientes_Geografia
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
  AND Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen
ORDER BY Tabla_Origen;
GO


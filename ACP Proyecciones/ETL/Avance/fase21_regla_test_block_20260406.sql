SET NOCOUNT ON;

DECLARE @reglas TABLE (
    Modulo_Raw NVARCHAR(50) NOT NULL,
    Modulo_Int INT NULL,
    SubModulo_Int INT NULL,
    Tipo_Conduccion NVARCHAR(50) NULL,
    Es_Test_Block BIT NOT NULL,
    Es_Activa BIT NOT NULL,
    Observacion NVARCHAR(300) NULL
);

INSERT INTO @reglas (
    Modulo_Raw,
    Modulo_Int,
    SubModulo_Int,
    Tipo_Conduccion,
    Es_Test_Block,
    Es_Activa,
    Observacion
)
VALUES
    (
        N'Test Block',
        NULL,
        NULL,
        N'TEST_BLOCK',
        1,
        1,
        N'Alias operativo Fase 21: Test Block => caso test block resoluble por Turno/Valvula'
    );

MERGE MDM.Regla_Modulo_Raw AS destino
USING @reglas AS origen
    ON UPPER(LTRIM(RTRIM(destino.Modulo_Raw))) = UPPER(LTRIM(RTRIM(origen.Modulo_Raw)))
WHEN MATCHED THEN
    UPDATE SET
        destino.Modulo_Int = origen.Modulo_Int,
        destino.SubModulo_Int = origen.SubModulo_Int,
        destino.Tipo_Conduccion = origen.Tipo_Conduccion,
        destino.Es_Test_Block = origen.Es_Test_Block,
        destino.Es_Activa = origen.Es_Activa,
        destino.Fecha_Modificacion = SYSDATETIME(),
        destino.Observacion = origen.Observacion
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
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
        origen.Modulo_Raw,
        origen.Modulo_Int,
        origen.SubModulo_Int,
        origen.Tipo_Conduccion,
        origen.Es_Test_Block,
        origen.Es_Activa,
        SYSDATETIME(),
        SYSDATETIME(),
        origen.Observacion
    );

SELECT
    Modulo_Raw,
    Modulo_Int,
    SubModulo_Int,
    Tipo_Conduccion,
    Es_Test_Block,
    Es_Activa,
    Observacion
FROM MDM.Regla_Modulo_Raw
WHERE UPPER(LTRIM(RTRIM(Modulo_Raw))) IN (N'VI', N'TEST BLOCK')
ORDER BY Modulo_Raw;

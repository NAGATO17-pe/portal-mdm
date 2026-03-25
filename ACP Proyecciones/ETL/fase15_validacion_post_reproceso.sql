/*
Fase 15 - Validacion post reproceso (Pesos/Vegetativa)
Ejecutar despues de:
1) fase15_reapertura_backlog_geografia.sql
2) py fase15_reproceso_pesos_vegetativa.py
*/

SET NOCOUNT ON;
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

/* 1) Smoke test resolvedor VI: debe devolver RESUELTA_TEST_BLOCK */
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='0';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='1';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='2';

/* 2) Cuarentena actual por motivo (solo dos tablas objetivo) */
SELECT
    Tabla_Origen,
    Motivo,
    COUNT(*) AS Filas_Pendientes
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen, Motivo
ORDER BY Tabla_Origen, Filas_Pendientes DESC;

/* 3) Control especifico geografia especial (debe caer fuerte) */
SELECT
    Tabla_Origen,
    COUNT(*) AS Geografia_Especial_Pendiente
FROM MDM.Cuarentena
WHERE Estado = 'PENDIENTE'
  AND Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen
ORDER BY Tabla_Origen;

/* 4) Control 9. en cuarentena (esperado) */
SELECT
    Q.Tabla_Origen,
    COUNT(*) AS Filas_9_Punto
FROM MDM.Cuarentena Q
WHERE Q.Estado = 'PENDIENTE'
  AND Q.Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
  AND Q.Valor_Recibido LIKE 'Modulo=9.%'
  AND Q.Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Q.Tabla_Origen
ORDER BY Q.Tabla_Origen;

/* 5) Carga de hoy en facts (hecho directo en tablas, no en auditoria) */
SELECT 'Silver.Fact_Evaluacion_Pesos' AS Tabla,
       COUNT(*) AS Filas_Hoy,
       MAX(Fecha_Sistema) AS Ultimo_Registro
FROM Silver.Fact_Evaluacion_Pesos
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Silver.Fact_Evaluacion_Vegetativa' AS Tabla,
       COUNT(*) AS Filas_Hoy,
       MAX(Fecha_Sistema) AS Ultimo_Registro
FROM Silver.Fact_Evaluacion_Vegetativa
WHERE CAST(Fecha_Sistema AS DATE) = @hoy;

/* 6) Estado de calidad cama (sin regresion esperada: OK_OPERATIVO) */
EXEC Silver.sp_Validar_Calidad_Camas @Cama_Max_Permitida = 100, @Max_Camas_Por_Geografia = 100;


/* 7) Control estructural: ID_Registro_Origen en nuevas cuarentenas */
SELECT
    Tabla_Origen,
    COUNT(*) AS Nuevas_Cuarentenas_Hoy,
    SUM(CASE WHEN ID_Registro_Origen IS NOT NULL THEN 1 ELSE 0 END) AS Con_ID_Registro_Origen
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE) = @hoy
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen
ORDER BY Tabla_Origen;


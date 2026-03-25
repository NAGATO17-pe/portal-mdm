/*
Fase 15B - Reapertura por firma operativa (sin ID_Registro_Origen)
Usar cuando MDM.Cuarentena no tiene ID_Registro_Origen poblado.
*/

SET NOCOUNT ON;

DECLARE @incluir_variedades BIT = 0; -- 0=solo geografia especial, 1=incluye variedad no reconocida

/* =========================
   Pesos
   ========================= */
;WITH firmas_geo_pesos AS (
    SELECT DISTINCT Q.Valor_Recibido
    FROM MDM.Cuarentena Q
    WHERE Q.Estado = 'PENDIENTE'
      AND Q.Tabla_Origen = 'Bronce.Evaluacion_Pesos'
      AND Q.Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
),
firmas_var_pesos AS (
    SELECT DISTINCT Q.Valor_Recibido
    FROM MDM.Cuarentena Q
    WHERE @incluir_variedades = 1
      AND Q.Estado = 'PENDIENTE'
      AND Q.Tabla_Origen = 'Bronce.Evaluacion_Pesos'
      AND Q.Motivo = N'Variedad no reconocida — requiere revisión en MDM'
),
objetivo_pesos AS (
    SELECT DISTINCT B.ID_Evaluacion_Pesos
    FROM Bronce.Evaluacion_Pesos B
    WHERE EXISTS (
        SELECT 1
        FROM firmas_geo_pesos F
        WHERE F.Valor_Recibido = CONCAT(
            N'Modulo=', COALESCE(B.Modulo_Raw, N'None'),
            N' | Turno=', COALESCE(B.Turno_Raw, N'None'),
            N' | Valvula=', COALESCE(B.Valvula_Raw, N'None'),
            N' | Cama=', COALESCE(B.Cama_Raw, N'None')
        )
    )
    OR EXISTS (
        SELECT 1
        FROM firmas_var_pesos F
        WHERE F.Valor_Recibido = COALESCE(B.Variedad_Raw, N'None')
    )
)
UPDATE B
SET B.Estado_Carga = 'CARGADO'
FROM Bronce.Evaluacion_Pesos B
JOIN objetivo_pesos O ON O.ID_Evaluacion_Pesos = B.ID_Evaluacion_Pesos;

SELECT @@ROWCOUNT AS Pesos_Reabiertos;

/* =========================
   Vegetativa
   ========================= */
;WITH firmas_geo_veg AS (
    SELECT DISTINCT Q.Valor_Recibido
    FROM MDM.Cuarentena Q
    WHERE Q.Estado = 'PENDIENTE'
      AND Q.Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
      AND Q.Motivo = N'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
),
firmas_var_veg AS (
    SELECT DISTINCT Q.Valor_Recibido
    FROM MDM.Cuarentena Q
    WHERE @incluir_variedades = 1
      AND Q.Estado = 'PENDIENTE'
      AND Q.Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
      AND Q.Motivo = N'Variedad no reconocida — requiere revisión en MDM'
),
objetivo_veg AS (
    SELECT DISTINCT B.ID_Evaluacion_Vegetativa
    FROM Bronce.Evaluacion_Vegetativa B
    WHERE EXISTS (
        SELECT 1
        FROM firmas_geo_veg F
        WHERE F.Valor_Recibido = CONCAT(
            N'Modulo=', COALESCE(B.Modulo_Raw, N'None'),
            N' | Turno=', COALESCE(B.Turno_Raw, N'None'),
            N' | Valvula=', COALESCE(B.Valvula_Raw, N'None'),
            N' | Cama=', COALESCE(B.Cama_Raw, N'None')
        )
    )
    OR EXISTS (
        SELECT 1
        FROM firmas_var_veg F
        WHERE F.Valor_Recibido = COALESCE(B.Descripcion_Raw, N'None')
    )
)
UPDATE B
SET B.Estado_Carga = 'CARGADO'
FROM Bronce.Evaluacion_Vegetativa B
JOIN objetivo_veg O ON O.ID_Evaluacion_Vegetativa = B.ID_Evaluacion_Vegetativa;

SELECT @@ROWCOUNT AS Vegetativa_Reabiertos;

/* Control rapido */
SELECT 'Pesos' AS Tabla, COUNT(*) AS Cargado_Actual
FROM Bronce.Evaluacion_Pesos
WHERE Estado_Carga = 'CARGADO'
UNION ALL
SELECT 'Vegetativa' AS Tabla, COUNT(*) AS Cargado_Actual
FROM Bronce.Evaluacion_Vegetativa
WHERE Estado_Carga = 'CARGADO';

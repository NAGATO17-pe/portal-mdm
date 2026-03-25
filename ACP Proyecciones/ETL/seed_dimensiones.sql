-- ============================================================
-- seed_dimensiones.sql  (v2 — corregido contra DDL v2)
-- Carga inicial de dimensiones maestras
-- ACP Geographic Phenology | Campana 2026
--
-- EJECUTAR: Una sola vez, despues del DDL, antes del ETL
-- ORDEN   : Respetar el orden — hay dependencias entre dims
-- Verificado contra: DDL_Geographic_Phenology_v2.sql
-- ============================================================

USE ACP_Geographic_Phenology;
GO

-- ── 1. Dim_Tiempo (2025-06-01 -> 2026-06-30) ─────────────────
PRINT 'Cargando Dim_Tiempo...';

WITH fechas AS (
    SELECT CAST('2025-03-01' AS DATE) AS fecha
    UNION ALL
    SELECT DATEADD(DAY, 1, fecha)
    FROM fechas
    WHERE fecha < '2026-06-30'
)
INSERT INTO Silver.Dim_Tiempo (
    ID_Tiempo,
    Fecha,
    Anio,
    Mes,
    Semana_ISO,
    Semana_Cosecha,
    Dia_Semana,
    Nombre_Mes,
    Es_Fin_Semana
)
SELECT
    CAST(FORMAT(fecha, 'yyyyMMdd') AS INT)       AS ID_Tiempo,
    fecha                                         AS Fecha,
    YEAR(fecha)                                   AS Anio,
    MONTH(fecha)                                  AS Mes,
    DATEPART(ISO_WEEK, fecha)                     AS Semana_ISO,
    DATEDIFF(WEEK, '2025-03-01', fecha) + 1       AS Semana_Cosecha,
    DATEPART(WEEKDAY, fecha)                      AS Dia_Semana,
    DATENAME(MONTH, fecha)                        AS Nombre_Mes,
    CASE WHEN DATEPART(WEEKDAY, fecha) IN (1,7)
         THEN 1 ELSE 0 END                        AS Es_Fin_Semana
FROM fechas
OPTION (MAXRECURSION 400);

PRINT '  OK — ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' dias cargados.';
GO

-- ── 2. Dim_Estado_Fenologico ─────────────────────────────────
PRINT 'Cargando Dim_Estado_Fenologico...';

INSERT INTO Silver.Dim_Estado_Fenologico (
    Nombre_Estado,
    Orden_Estado
) VALUES
    ('Boton Floral',  0),
    ('Flor',          1),
    ('Pequena',       2),
    ('Verde',         3),
    ('Inicio F1',     4),
    ('Inicio F2',     5),
    ('Crema',         6),
    ('Madura',        7),
    ('Cosechable',    8);

PRINT '  OK — 9 estados cargados.';
GO

-- ── 3. Dim_Cinta ─────────────────────────────────────────────
PRINT 'Cargando Dim_Cinta...';

INSERT INTO Silver.Dim_Cinta (
    Color_Cinta,
    Descripcion
) VALUES
    ('Roja',     'Cinta roja — referencia visual campana'),
    ('Azul',     'Cinta azul — referencia visual campana'),
    ('Verde',    'Cinta verde — referencia visual campana'),
    ('Amarilla', 'Cinta amarilla — referencia visual campana'),
    ('Blanca',   'Cinta blanca — referencia visual campana'),
    ('Naranja',  'Cinta naranja — referencia visual campana');

PRINT '  OK — 6 cintas cargadas.';
GO

-- ── 4. Dim_Condicion_Cultivo ──────────────────────────────────
PRINT 'Cargando Dim_Condicion_Cultivo...';

INSERT INTO Silver.Dim_Condicion_Cultivo (
    Sustrato,
    Certificacion
) VALUES
    ('Suelo',    'GlobalGAP'),
    ('Suelo',    'Organico'),
    ('Sustrato', 'GlobalGAP'),
    ('Sustrato', 'Organico'),
    ('Suelo',    'Sin certificacion');

PRINT '  OK — 5 condiciones cargadas.';
GO

-- ── 5. Dim_Actividad_Operativa ────────────────────────────────
PRINT 'Cargando Dim_Actividad_Operativa...';

INSERT INTO Silver.Dim_Actividad_Operativa (
    ID_SAP,
    Nombre_Actividad,
    ID_Labor,
    Nombre_Labor,
    Categoria
) VALUES
    (NULL, 'Cosecha',                  NULL, NULL, 'Cosecha'),
    (NULL, 'Poda',                     NULL, NULL, 'Poda'),
    (NULL, 'Defoliacion',              NULL, NULL, 'Poda'),
    (NULL, 'Evaluacion fenologica',    NULL, NULL, 'Evaluacion'),
    (NULL, 'Evaluacion de pesos',      NULL, NULL, 'Evaluacion'),
    (NULL, 'Evaluacion vegetativa',    NULL, NULL, 'Evaluacion'),
    (NULL, 'Aplicacion fitosanitaria', NULL, NULL, 'Sanidad'),
    (NULL, 'Riego',                    NULL, NULL, 'Riego'),
    (NULL, 'Fertilizacion',            NULL, NULL, 'Riego'),
    (NULL, 'Supervision',              NULL, NULL, 'Gestion'),
    (NULL, 'Fiscalizacion',            NULL, NULL, 'Gestion'),
    (NULL, 'Transporte interno',       NULL, NULL, 'Logistica');

PRINT '  OK — 12 actividades cargadas.';
GO

-- ── 6. Dim_Escenario_Proyeccion ───────────────────────────────
PRINT 'Cargando Dim_Escenario_Proyeccion...';

INSERT INTO Silver.Dim_Escenario_Proyeccion (
    Tipo_Escenario,
    Descripcion,
    Horizonte_Semanas,
    Frecuencia_Actualizacion
) VALUES
    ('Presupuesto', 'Proyeccion inicial de temporada aprobada por gerencia',   52, 'Anual'),
    ('Preliminar',  'Primera proyeccion operativa al inicio de campana',        20, 'Semanal'),
    ('Mensual',     'Proyeccion mensual con conteos actualizados',              12, 'Mensual'),
    ('Final',       'Proyeccion cierre de campana — consenso final',             6, 'Semanal'),
    ('Manual',      'Ajuste manual aprobado por el analista',                    4, 'Ad hoc'),
    ('Modelo',      'Proyeccion generada por modelo predictivo TFT/XGBoost',   12, 'Semanal'),
    ('Ajuste',      'Ajuste diario sobre proyeccion vigente',                    2, 'Diario');

PRINT '  OK — 7 escenarios cargados.';
GO

-- ── 7. Dim_Estado_Workflow ────────────────────────────────────
PRINT 'Cargando Dim_Estado_Workflow...';

INSERT INTO Silver.Dim_Estado_Workflow (
    Estado,
    Descripcion
) VALUES
    ('Borrador',    'Proyeccion en construccion — no publicada'),
    ('En revision', 'Enviada a revision por el analista'),
    ('Aprobada',    'Aprobada por jefatura — es la version vigente'),
    ('Archivada',   'Reemplazada por version mas reciente'),
    ('Rechazada',   'Rechazada en revision — requiere correccion');

PRINT '  OK — 5 estados de workflow cargados.';
GO

-- ── 8. Dim_Personal — Surrogate -1 ───────────────────────────
PRINT 'Cargando Dim_Personal (surrogate -1)...';

SET IDENTITY_INSERT Silver.Dim_Personal ON;

INSERT INTO Silver.Dim_Personal (
    ID_Personal,
    DNI,
    Nombre_Completo,
    Rol,
    Sexo,
    ID_Planilla,
    Pct_Asertividad,
    Dias_Ausentismo
) VALUES (
    -1,
    '00000000',
    'Sin Evaluador',
    'Sin asignar',
    NULL,
    NULL,
    NULL,
    0
);

SET IDENTITY_INSERT Silver.Dim_Personal OFF;

PRINT '  OK — Surrogate -1 cargado.';
GO

-- ── 9. Dim_Variedad ───────────────────────────────────────────
PRINT 'Cargando Dim_Variedad...';

INSERT INTO Silver.Dim_Variedad (
    Nombre_Variedad,
    Breeder
) VALUES
    ('Biloxi',      'Mississippi State'),
    ('Jewel',       'University of Florida'),
    ('Misty',       'University of Florida'),
    ('Star',        'University of Florida'),
    ('Springhigh',  'University of Florida'),
    ('Emerald',     'University of Florida'),
    ('Abundance',   'Fall Creek'),
    ('Ventura',     'Fall Creek'),
    ('Sekoya Pop',  'Sekoya'),
    ('Megacrisp',   'Fall Creek'),
    ('Calixto',     'Planasa'),
    ('Snowchaser',  'Fall Creek'),
    ('Kestrel',     'Fall Creek'),
    ('Last Call',   'Fall Creek'),
    ('Cargo',       'Fall Creek'),
    ('Top Shelf',   'Fall Creek'),
    ('Dt Ig 06',    'ACP I+D'),
    ('Kms1530',     'ACP I+D'),
    ('Dt 12-5',     'ACP I+D'),
    ('Dt Ig 02',    'ACP I+D'),
    ('O''Neal',     'University of Florida');

PRINT '  OK — 21 variedades cargadas.';
GO

-- ── 10. MDM.Catalogo_Variedades ───────────────────────────────
PRINT 'Cargando MDM.Catalogo_Variedades...';

INSERT INTO MDM.Catalogo_Variedades (
    Nombre_Canonico,
    Breeder,
    Es_Activa,
    Fecha_Creacion,
    Fecha_Modificacion
)
SELECT
    Nombre_Variedad,
    Breeder,
    1,
    SYSDATETIME(),
    SYSDATETIME()
FROM Silver.Dim_Variedad;

PRINT '  OK — catalogo MDM sincronizado con Dim_Variedad.';
GO

-- ── 11. Config.Parametros_Pipeline ───────────────────────────
PRINT 'Cargando Config.Parametros_Pipeline...';

INSERT INTO Config.Parametros_Pipeline (
    Nombre_Parametro,
    Valor,
    Descripcion,
    Fecha_Modificacion,
    Modificado_Por
) VALUES
    ('CAMPANA_ACTIVA',       '2026',           'Campana en curso',                              SYSDATETIME(), 'SISTEMA'),
    ('CULTIVO_ACTIVO',       'Arandano',        'Cultivo en scope actual',                       SYSDATETIME(), 'SISTEMA'),
    ('CHUNK_SIZE_INSERT',    '500',             'Filas por batch en INSERT masivo',              SYSDATETIME(), 'SISTEMA'),
    ('RUTA_ENTRADA',         'data/entrada',    'Carpeta raiz de archivos Excel de campo',       SYSDATETIME(), 'SISTEMA'),
    ('RUTA_PROCESADOS',      'data/procesados', 'Carpeta de archivos ya procesados',             SYSDATETIME(), 'SISTEMA'),
    ('DNI_LONGITUD',         '8',               'Longitud esperada de DNI en digitos',           SYSDATETIME(), 'SISTEMA'),
    ('PESO_BAYA_MIN',        '0.5',             'Peso minimo valido de baya en gramos',          SYSDATETIME(), 'SISTEMA'),
    ('PESO_BAYA_MAX',        '8.0',             'Peso maximo valido de baya en gramos',          SYSDATETIME(), 'SISTEMA'),
    ('MUESTRAS_MIN',         '1',               'Cantidad minima de muestras por evaluacion',    SYSDATETIME(), 'SISTEMA'),
    ('TOTAL_PLANTAS_MIN',    '1',               'Plantas minimas por registro de sanidad',       SYSDATETIME(), 'SISTEMA'),
    ('LEVENSHTEIN_UMBRAL',   '0.85',            'Score minimo para homologacion automatica',     SYSDATETIME(), 'SISTEMA'),
    ('DIA_PINTADO_FLORES',   '2',               'Dia semana esperado Pintado Flores (2=Lunes)',  SYSDATETIME(), 'SISTEMA'),
    ('DIA_TASA_BROTES',      '6',               'Dia semana esperado Tasa Brotes (6=Viernes)',   SYSDATETIME(), 'SISTEMA');

PRINT '  OK — 13 parametros cargados.';
GO

-- ── 12. Config.Reglas_Validacion ──────────────────────────────
PRINT 'Cargando Config.Reglas_Validacion...';

INSERT INTO Config.Reglas_Validacion (
    Tabla_Destino,
    Columna,
    Valor_Min,
    Valor_Max,
    Tipo_Validacion,
    Accion,
    Descripcion,
    Activo,
    Fecha_Creacion,
    Fecha_Modificacion
) VALUES
    ('Silver.Fact_Evaluacion_Pesos', 'Peso_Promedio_Baya_g',  0.5,  8.0,  'RANGO', 'RECHAZAR', 'Peso baya fuera de rango biologico 0.5-8.0g',   1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Fact_Peladas',          'Muestras',              1,    NULL, 'RANGO', 'RECHAZAR', 'Muestras debe ser mayor o igual a 1',            1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Fact_Sanidad_Activo',   'Total_Plantas',         1,    NULL, 'RANGO', 'RECHAZAR', 'Total_Plantas debe ser mayor o igual a 1',       1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Fact_Telemetria_Clima', 'Temperatura_Max_C',    -5,   50,   'RANGO', 'ALERTA',   'Temperatura maxima fuera de rango esperado',     1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Fact_Telemetria_Clima', 'Temperatura_Min_C',    -5,   50,   'RANGO', 'ALERTA',   'Temperatura minima fuera de rango esperado',     1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Fact_Telemetria_Clima', 'Humedad_Relativa_Pct',  0,  100,   'RANGO', 'ALERTA',   'Humedad fuera de rango 0-100',                   1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Dim_Personal',          'Pct_Asertividad',       0,  100,   'RANGO', 'ALERTA',   'Porcentaje de asertividad fuera de rango 0-100', 1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Fact_Cosecha_SAP',      'Kg_Neto_MP',            0,  NULL,  'RANGO', 'ALERTA',   'Kg neto no puede ser negativo',                  1, SYSDATETIME(), SYSDATETIME()),
    ('Silver.Fact_Tareo',            'Horas_Trabajadas',      0,   24,   'RANGO', 'ALERTA',   'Horas trabajadas fuera de rango 0-24',           1, SYSDATETIME(), SYSDATETIME());

PRINT '  OK — 9 reglas de validacion cargadas.';
GO

-- ── VERIFICACION FINAL ────────────────────────────────────────
PRINT '';
PRINT '=== VERIFICACION DE CARGA ===';

SELECT 'Silver.Dim_Tiempo'               AS Tabla, COUNT(*) AS Filas FROM Silver.Dim_Tiempo
UNION ALL
SELECT 'Silver.Dim_Estado_Fenologico',            COUNT(*) FROM Silver.Dim_Estado_Fenologico
UNION ALL
SELECT 'Silver.Dim_Cinta',                        COUNT(*) FROM Silver.Dim_Cinta
UNION ALL
SELECT 'Silver.Dim_Condicion_Cultivo',            COUNT(*) FROM Silver.Dim_Condicion_Cultivo
UNION ALL
SELECT 'Silver.Dim_Actividad_Operativa',          COUNT(*) FROM Silver.Dim_Actividad_Operativa
UNION ALL
SELECT 'Silver.Dim_Escenario_Proyeccion',         COUNT(*) FROM Silver.Dim_Escenario_Proyeccion
UNION ALL
SELECT 'Silver.Dim_Estado_Workflow',              COUNT(*) FROM Silver.Dim_Estado_Workflow
UNION ALL
SELECT 'Silver.Dim_Personal',                     COUNT(*) FROM Silver.Dim_Personal
UNION ALL
SELECT 'Silver.Dim_Variedad',                     COUNT(*) FROM Silver.Dim_Variedad
UNION ALL
SELECT 'MDM.Catalogo_Variedades',                 COUNT(*) FROM MDM.Catalogo_Variedades
UNION ALL
SELECT 'Config.Parametros_Pipeline',              COUNT(*) FROM Config.Parametros_Pipeline
UNION ALL
SELECT 'Config.Reglas_Validacion',                COUNT(*) FROM Config.Reglas_Validacion;

PRINT '=== SEED DATA COMPLETO ===';
GO

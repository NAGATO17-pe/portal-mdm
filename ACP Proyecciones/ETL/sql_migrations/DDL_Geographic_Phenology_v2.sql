-- ============================================================
-- DWH Geographic Phenology — Agrícola Cerro Prieto
-- DDL Completo — Creación de Base de Datos y Esquemas
-- Collation: Modern_Spanish_CI_AS
-- Campaña 2026 | Marzo 2026
-- ============================================================

USE master;
GO

CREATE DATABASE ACP_Geographic_Phenology
    COLLATE Modern_Spanish_CI_AS;
GO

USE ACP_Geographic_Phenology;
GO

-- ============================================================
-- ESQUEMAS LÓGICOS
-- ============================================================
CREATE SCHEMA Bronce;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO
CREATE SCHEMA MDM;
GO
CREATE SCHEMA Config;
GO
CREATE SCHEMA Auditoria;
GO


-- ============================================================
-- CAPA BRONCE (22 tablas)
-- Datos crudos. Sin transformación. Todo NVARCHAR.
-- Solo se agrega Nombre_Archivo + Fecha_Sistema.
-- Eliminadas: Reporte_Pesos, Seguimiento_Asistencia, Balance_Hidrico
-- ============================================================

CREATE TABLE Bronce.Dashboard (
    ID_Dashboard        BIGINT IDENTITY(1,1) PRIMARY KEY,
    Nombre_Archivo      NVARCHAR(255)   NOT NULL,
    Ruta_Origen         NVARCHAR(500)   NULL,
    Fecha_Sistema       DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Conteo_Fruta (
    ID_Conteo_Fruta         BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Tipo_Evaluacion_Raw     NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Induccion_Floral (
    ID_Induccion_Floral     BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Tipo_Evaluacion_Raw     NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Ciclos_Fenologicos (
    ID_Ciclo_Fenologico     BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Organo_Raw              NVARCHAR(50)    NULL,
    Color_Raw               NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    FechaSubida_Raw         NVARCHAR(50)    NULL,  -- Formato YYYY-MM-DD HH:MM sin segundos
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Maduracion (
    ID_Maduracion           BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_Carga            NVARCHAR(20)    NOT NULL DEFAULT 'CARGADO'
);
GO

CREATE TABLE Bronce.Pintado_Flores (
    ID_Pintado_Flores       BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Peladas (
    ID_Peladas              BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Evaluacion_Vegetativa (
    ID_Evaluacion_Veg       BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Semanas_Poda_Raw        NVARCHAR(20)    NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Tasa_Crecimiento_Brotes (
    ID_Tasa_Crecimiento     BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Evaluacion_Calidad_Poda (
    ID_Evaluacion_Poda      BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Tipo_Evaluacion_Raw     NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Fisiologia (
    ID_Fisiologia           BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Brote_Raw               NVARCHAR(10)    NULL,  -- Mixto: '1'-'5' y 'B1'-'B5'
    Tercio_Raw              NVARCHAR(20)    NULL,
    Hinchadas_Raw           NVARCHAR(20)    NULL,  -- Object mixto en pandas
    Productivas_Raw         NVARCHAR(20)    NULL,  -- Object mixto en pandas
    Total_Org_Raw           NVARCHAR(20)    NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Calibres (
    ID_Calibres             BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Consolidado_Tareos (
    ID_Tareo                BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    IDPersonalGeneral_Raw   NVARCHAR(20)    NULL,  -- Viene numerico: riesgo de perder ceros
    DNIResponsable_Raw      NVARCHAR(20)    NULL,  -- Mismo caso
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Actividad_Raw           NVARCHAR(150)   NULL,
    Labor_Raw               NVARCHAR(150)   NULL,
    HorasTrabajadas_Raw     NVARCHAR(20)    NULL,
    IDPlanilla_Raw          NVARCHAR(20)    NULL,  -- 0.2% nulos
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Fiscalizacion (
    ID_Fiscalizacion        BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    DNI_Raw                 NVARCHAR(20)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

-- Bronce.Seguimiento_Asistencia ELIMINADA
-- Decisión: dato cubierto por Fact_Tareo (Consolidado_Tareos).
-- Dias_Ausentismo se calcula en ETL desde Fact_Tareo → UPDATE Dim_Personal.
-- No aporta columnas únicas ni grain distinto.

CREATE TABLE Bronce.Seguimiento_Errores (
    ID_Seguimiento_Errores  BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Evaluador_Raw           NVARCHAR(150)   NULL,
    Tipo_Error_Raw          NVARCHAR(150)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Evaluacion_Pesos (
    ID_Evaluacion_Pesos     BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Evaluacion_Raw          NVARCHAR(50)    NULL,
    PesoBaya_Raw            NVARCHAR(20)    NULL,  -- CRITICO: 2.5 vs 25 — validar rango en ETL
    CantMuestra_Raw         NVARCHAR(20)    NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

-- Bronce.Reporte_Pesos ELIMINADA
-- Decisión: resumen intermedio cubierto por Bronce.Evaluacion_Pesos → Fact_Evaluacion_Pesos.
-- No aporta dato original. Redundante.

CREATE TABLE Bronce.Reporte_Cosecha (
    ID_Reporte_Cosecha      BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Turno_Raw               NVARCHAR(50)    NULL,
    Valvula_Raw             NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    KgNeto_Raw              NVARCHAR(30)    NULL,
    Jabas_Raw               NVARCHAR(20)    NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Cierre_Mapas_Cosecha (
    ID_Cierre_Cosecha       BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Reporte_Clima (
    ID_Reporte_Clima        BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Hora_Raw                NVARCHAR(20)    NULL,
    Sector_Raw              NVARCHAR(50)    NULL,
    TempMax_Raw             NVARCHAR(20)    NULL,
    TempMin_Raw             NVARCHAR(20)    NULL,
    Humedad_Raw             NVARCHAR(20)    NULL,
    Precipitacion_Raw       NVARCHAR(20)    NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Variables_Meteorologicas (
    ID_Variables_Met        BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    VPD_Raw                 NVARCHAR(20)    NULL,
    Radiacion_Raw           NVARCHAR(20)    NULL,
    Valores_Raw             NVARCHAR(MAX)   NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

-- Bronce.Balance_Hidrico ELIMINADA
-- Decisión: dominio de riego, fuera del scope de proyección de cosecha.
-- Si se necesita correlación estrés hídrico/rendimiento: se reincorpora en Fase futura.

CREATE TABLE Bronce.Data_SAP (
    ID_Data_SAP             BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Cosecha_Raw       NVARCHAR(50)    NULL,
    Consumidor_PEP_Raw      NVARCHAR(100)   NULL,
    Des_Consumidor_PEP_Raw  NVARCHAR(200)   NULL,  -- 'SECTOR X MODULO Y TURNO Z' — parsing regex
    Variedad_Codigo_Raw     NVARCHAR(50)    NULL,
    Des_Variedad_Raw        NVARCHAR(100)   NULL,
    Material_Codigo_Raw     NVARCHAR(50)    NULL,  -- Trailing spaces — RTRIM requerido
    Descripcion_Material_Raw NVARCHAR(200)  NULL,
    Codigo_Cliente_Raw      NVARCHAR(30)    NULL,  -- Float con nulos — 26.5% nulos
    Responsable_Raw         NVARCHAR(150)   NULL,  -- 27% nulos
    Lote_Raw                NVARCHAR(50)    NULL,  -- 27% nulos
    Almacen_Raw             NVARCHAR(50)    NULL,  -- 27% nulos
    Peso_Bruto_Raw          NVARCHAR(30)    NULL,
    Peso_Tara_Raw           NVARCHAR(30)    NULL,
    Peso_Neto_Raw           NVARCHAR(30)    NULL,
    Cantidad_Jabas_Raw      NVARCHAR(20)    NULL,
    Doc_Remision_Raw        NVARCHAR(50)    NULL,
    Fecha_Recepcion_Raw     NVARCHAR(50)    NULL,  -- 0.7% nulos
    FechaSubida_Raw         NVARCHAR(50)    NULL,  -- YYYY-MM-DD HH:MM sin segundos
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Bronce.Proyeccion_Pesos (
    ID_Proyeccion_Pesos     BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fecha_Raw               NVARCHAR(50)    NULL,
    Modulo_Raw              NVARCHAR(50)    NULL,
    Variedad_Raw            NVARCHAR(100)   NULL,
    Peso_Proyectado_Raw     NVARCHAR(20)    NULL,
    Nombre_Archivo          NVARCHAR(255)   NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO


-- ============================================================
-- CAPA SILVER — DIMENSIONES (10)
-- ============================================================

CREATE TABLE Silver.Dim_Tiempo (
    ID_Tiempo               INT             PRIMARY KEY,  -- Formato YYYYMMDD
    Fecha                   DATE            NOT NULL,
    Anio                    INT             NOT NULL,
    Mes                     INT             NOT NULL,
    Semana_ISO              INT             NOT NULL,
    Semana_Cosecha          INT             NULL,
    Dia_Semana              INT             NOT NULL,  -- 1=Lunes, 7=Domingo
    Nombre_Mes              NVARCHAR(20)    NOT NULL,
    Es_Fin_Semana           BIT             NOT NULL DEFAULT 0
);
GO

CREATE TABLE Silver.Dim_Condicion_Cultivo (
    ID_Condicion            INT             IDENTITY(1,1) PRIMARY KEY,
    Sustrato                NVARCHAR(100)   NOT NULL,
    Certificacion           NVARCHAR(50)    NOT NULL   -- Organico / Convencional
);
GO

CREATE TABLE Silver.Dim_Estado_Workflow (
    ID_Workflow             INT             IDENTITY(1,1) PRIMARY KEY,
    Estado                  NVARCHAR(50)    NOT NULL,  -- Pendiente / Aprobado / Rechazado
    Descripcion             NVARCHAR(200)   NULL
);
GO

CREATE TABLE Silver.Dim_Cinta (
    ID_Cinta                INT             IDENTITY(1,1) PRIMARY KEY,
    Color_Cinta             NVARCHAR(50)    NOT NULL,
    Descripcion             NVARCHAR(200)   NULL
);
GO

CREATE TABLE Silver.Dim_Estado_Fenologico (
    ID_Estado_Fenologico    INT             IDENTITY(1,1) PRIMARY KEY,
    Nombre_Estado           NVARCHAR(100)   NOT NULL,
    -- Boton Floral / Flor / Pequena / Verde / Inicio F1 / Inicio F2 / Crema / Madura / Cosechable
    Orden_Estado            INT             NOT NULL   -- 0 a 8
);
GO

CREATE TABLE Silver.Dim_Variedad (
    ID_Variedad             INT             IDENTITY(1,1) PRIMARY KEY,
    Nombre_Variedad         NVARCHAR(100)   NOT NULL,  -- Nombre canonico normalizado
    Breeder                 NVARCHAR(100)   NULL
);
GO

CREATE TABLE Silver.Dim_Actividad_Operativa (
    ID_Actividad            INT             IDENTITY(1,1) PRIMARY KEY,
    ID_SAP                  NVARCHAR(50)    NULL,
    Nombre_Actividad        NVARCHAR(150)   NOT NULL,
    ID_Labor                NVARCHAR(50)    NULL,
    Nombre_Labor            NVARCHAR(150)   NULL,
    Categoria               NVARCHAR(100)   NULL
);
GO

CREATE TABLE Silver.Dim_Escenario_Proyeccion (
    ID_Escenario            INT             IDENTITY(1,1) PRIMARY KEY,
    Tipo_Escenario          NVARCHAR(50)    NOT NULL,
    -- Valores: Presupuesto / Preliminar / Final / Mensual /
    --          Six_Week / Six_Week_Formalizado / Ajuste_Semanal
    Descripcion             NVARCHAR(200)   NULL,
    Horizonte_Semanas       INT             NULL,
    Frecuencia_Actualizacion NVARCHAR(50)   NULL
);
GO

CREATE TABLE Silver.Dim_Personal (
    ID_Personal             INT             IDENTITY(1,1) PRIMARY KEY,
    DNI                     NVARCHAR(20)    NOT NULL,  -- NVARCHAR: preserva ceros a la izquierda
    Nombre_Completo         NVARCHAR(200)   NOT NULL,
    Rol                     NVARCHAR(100)   NULL,
    Sexo                    NVARCHAR(10)    NULL,
    ID_Planilla             NVARCHAR(20)    NULL,
    Pct_Asertividad         DECIMAL(5,2)    NULL,
    Dias_Ausentismo         INT             NULL
);
GO

-- Surrogate para evaluador desconocido
INSERT INTO Silver.Dim_Personal (DNI, Nombre_Completo, Rol)
VALUES ('00000000', 'Sin Evaluador', 'N/A');
GO

CREATE TABLE Silver.Dim_Geografia (
    ID_Geografia            INT             IDENTITY(1,1) PRIMARY KEY,
    Fundo                   NVARCHAR(100)   NOT NULL,
    Sector                  NVARCHAR(100)   NULL,
    Modulo                  INT             NULL,
    Turno                   INT             NULL,
    Valvula                 NVARCHAR(50)    NULL,
    Cama                    NVARCHAR(50)    NULL,
    Es_Test_Block           BIT             NOT NULL DEFAULT 0,
    Codigo_SAP_Campo        NVARCHAR(50)    NULL,  -- Lookup hoja Protocolo SAP
    Fecha_Inicio_Vigencia   DATE            NOT NULL,  -- SCD Tipo 2
    Fecha_Fin_Vigencia      DATE            NULL,      -- NULL = vigente
    Es_Vigente              BIT             NOT NULL DEFAULT 1
);
GO


-- ============================================================
-- CAPA SILVER — FACTS (10)
-- ============================================================

CREATE TABLE Silver.Fact_Cosecha_SAP (
    ID_Cosecha_SAP          BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    ID_Condicion_Cultivo    INT             NOT NULL REFERENCES Silver.Dim_Condicion_Cultivo(ID_Condicion),
    Kg_Brutos               DECIMAL(10,3)   NULL,
    Kg_Neto_MP              DECIMAL(10,3)   NULL,
    CONSTRAINT chk_kg_neto CHECK (Kg_Neto_MP >= 0),
    Cantidad_Jabas          INT             NULL,
    Lote                    NVARCHAR(50)    NULL,
    Almacen                 NVARCHAR(50)    NULL,
    Doc_Remision            NVARCHAR(50)    NULL,
    Codigo_Cliente          NVARCHAR(30)    NULL,
    Responsable             NVARCHAR(150)   NULL,
    Descripcion_Material    NVARCHAR(200)   NULL,
    Codigo_SAP_Material     NVARCHAR(50)    NULL,
    Fecha_Recepcion         DATETIME2       NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO

CREATE TABLE Silver.Fact_Conteo_Fenologico (
    ID_Conteo_Fenologico    BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    ID_Personal             INT             NULL    REFERENCES Silver.Dim_Personal(ID_Personal),
    -- Nullable: 63% sin evaluador — Surrogate ID=-1 cuando aplica
    ID_Cinta                INT             NOT NULL REFERENCES Silver.Dim_Cinta(ID_Cinta),
    ID_Estado_Fenologico    INT             NOT NULL REFERENCES Silver.Dim_Estado_Fenologico(ID_Estado_Fenologico),
    Cantidad_Organos        INT             NOT NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO

CREATE TABLE Silver.Fact_Telemetria_Clima (
    ID_Telemetria           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    Temperatura_Max_C       DECIMAL(6,2)    NULL,
    Temperatura_Min_C       DECIMAL(6,2)    NULL,
    Humedad_Relativa_Pct    DECIMAL(6,2)    NULL,
    Precipitacion_mm        DECIMAL(8,2)    NULL,
    VPD                     DECIMAL(8,4)    NULL,
    Radiacion_Solar         DECIMAL(10,4)   NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Silver.Fact_Proyecciones (
    ID_Proyeccion           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    ID_Escenario            INT             NOT NULL REFERENCES Silver.Dim_Escenario_Proyeccion(ID_Escenario),
    ID_Estado_Workflow      INT             NOT NULL REFERENCES Silver.Dim_Estado_Workflow(ID_Workflow),
    Kg_Proyectados          DECIMAL(12,3)   NOT NULL,
    MAPE                    DECIMAL(8,4)    NULL,
    Version_Modelo          NVARCHAR(50)    NULL,
    Fecha_Cutoff            DATETIME2       NOT NULL,
    ID_Version_Datos        NVARCHAR(100)   NULL,  -- Hash del snapshot de datos
    Flag_Override           BIT             NOT NULL DEFAULT 0,
    Motivo_Override         NVARCHAR(500)   NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO

CREATE TABLE Silver.Fact_Evaluacion_Vegetativa (
    ID_Evaluacion_Veg       BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    Semanas_Despues_Poda    INT             NULL,
    Promedio_Altura         DECIMAL(8,2)    NULL,
    Promedio_Tallos_Basales DECIMAL(8,2)    NULL,
    Promedio_Tallos_Basales_Nuevos DECIMAL(8,2) NULL,
    Promedio_Brotes_Generales   DECIMAL(8,2) NULL,  -- BG 1-4 / n
    Promedio_Brotes_Productivos DECIMAL(8,2) NULL,  -- BP Totales / n
    Promedio_Diametro_Brote DECIMAL(8,2)    NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO

CREATE TABLE Silver.Fact_Sanidad_Activo (
    ID_Sanidad              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    Plantas_Vivas           INT             NULL,
    Plantas_Muertas         INT             NULL,
    Total_Plantas           INT             NULL,
    Pct_Mortalidad          AS (CAST(Plantas_Muertas AS DECIMAL(8,2)) /
                                NULLIF(Total_Plantas, 0) * 100) PERSISTED,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Silver.Fact_Evaluacion_Pesos (
    ID_Evaluacion_Pesos     BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    ID_Personal             INT             NOT NULL REFERENCES Silver.Dim_Personal(ID_Personal),
    Peso_Promedio_Baya_g    DECIMAL(6,2)    NOT NULL,
    CONSTRAINT chk_peso_baya CHECK (Peso_Promedio_Baya_g BETWEEN 0.5 AND 8.0),
    Cantidad_Bayas_Muestra  INT             NULL,
    Peso_Proyectado_Baya_g  DECIMAL(6,2)    NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO

CREATE TABLE Silver.Fact_Ciclo_Poda (
    ID_Poda                 BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    Tipo_Evaluacion         NVARCHAR(100)   NULL,  -- PODA GENERAL / etc
    Promedio_Tallos_Planta  DECIMAL(8,2)    NULL,
    Promedio_Longitud_Tallo DECIMAL(8,2)    NULL,
    Promedio_Diametro_Tallo DECIMAL(8,2)    NULL,
    Promedio_Ramilla_Planta DECIMAL(8,2)    NULL,
    Promedio_Tocones_Planta DECIMAL(8,2)    NULL,
    Promedio_Cortes_Defectuosos DECIMAL(8,2) NULL,
    Promedio_Altura_Poda    DECIMAL(8,2)    NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO

CREATE TABLE Silver.Fact_Tareo (
    ID_Tareo                BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Personal             INT             NOT NULL REFERENCES Silver.Dim_Personal(ID_Personal),
    ID_Actividad_Operativa  INT             NOT NULL REFERENCES Silver.Dim_Actividad_Operativa(ID_Actividad),
    ID_Personal_Supervisor  INT             NULL    REFERENCES Silver.Dim_Personal(ID_Personal),
    Horas_Trabajadas        DECIMAL(6,2)    NOT NULL,
    ID_Planilla             NVARCHAR(20)    NULL,
    Es_Observado_SAP        BIT             NOT NULL DEFAULT 0,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Silver.Fact_Fisiologia (
    ID_Fisiologia           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    Tercio                  NVARCHAR(20)    NULL,
    Brotes_Productivos      INT             NULL,
    Brotes_Vegetativos      INT             NULL,
    Hinchadas               INT             NULL,
    Productivas             INT             NULL,
    Total_Organos           INT             NULL,
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO

-- ------------------------------------------------------------
-- Fact_Peladas [NUEVA — v2]
-- Muestreo físico de bayas por punto en campo.
-- Grain: Fecha + Geografía + Variedad + Punto
-- Metodología: se desprenden frutos de puntos específicos
-- y se cuentan todos los estados fenológicos simultáneamente.
-- Input directo del modelo Six Weeks y Proyección Mensual.
-- Fuente: Bronce.Peladas + Bronce.Conteo_Fruta (validado linaje)
-- Errores documentados: confusión entre estados adyacentes
-- (F1/F2, Crema/Madura). Protocolo HITL activo en pesos.
-- ------------------------------------------------------------
CREATE TABLE Silver.Fact_Peladas (
    ID_Peladas              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Geografia            INT             NOT NULL REFERENCES Silver.Dim_Geografia(ID_Geografia),
    ID_Tiempo               INT             NOT NULL REFERENCES Silver.Dim_Tiempo(ID_Tiempo),
    ID_Variedad             INT             NOT NULL REFERENCES Silver.Dim_Variedad(ID_Variedad),
    ID_Personal             INT             NULL     REFERENCES Silver.Dim_Personal(ID_Personal),
    Punto                   INT             NOT NULL,
    Botones_Florales        INT             NOT NULL DEFAULT 0 CHECK (Botones_Florales >= 0),
    Flores                  INT             NOT NULL DEFAULT 0 CHECK (Flores >= 0),
    Bayas_Pequenas          INT             NOT NULL DEFAULT 0 CHECK (Bayas_Pequenas >= 0),
    Bayas_Grandes           INT             NOT NULL DEFAULT 0 CHECK (Bayas_Grandes >= 0),
    Fase_1                  INT             NOT NULL DEFAULT 0 CHECK (Fase_1 >= 0),
    Fase_2                  INT             NOT NULL DEFAULT 0 CHECK (Fase_2 >= 0),
    Bayas_Cremas            INT             NOT NULL DEFAULT 0 CHECK (Bayas_Cremas >= 0),
    Bayas_Maduras           INT             NOT NULL DEFAULT 0 CHECK (Bayas_Maduras >= 0),
    Bayas_Cosechables       INT             NOT NULL DEFAULT 0 CHECK (Bayas_Cosechables >= 0),
    Plantas_Productivas     INT             NOT NULL DEFAULT 0 CHECK (Plantas_Productivas >= 0),
    Plantas_No_Productivas  INT             NOT NULL DEFAULT 0 CHECK (Plantas_No_Productivas >= 0),
    Muestras                INT             NOT NULL            CHECK (Muestras >= 1),
    Fecha_Evento            DATETIME2       NOT NULL,
    Fecha_Sistema           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Estado_DQ               NVARCHAR(20)    NOT NULL DEFAULT 'Aprobado'
);
GO


-- ============================================================
-- CAPA ORO — MARTS (6)
-- Tablas anchas desnormalizadas. Solo Power BI accede aquí.
-- ============================================================

CREATE TABLE Gold.Mart_Cosecha (
    ID_Mart_Cosecha         BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Semana_ISO              INT             NOT NULL,
    Fecha_Cosecha           DATE            NOT NULL,
    Modulo                  INT             NULL,
    Turno                   INT             NULL,
    Variedad                NVARCHAR(100)   NOT NULL,
    Condicion               NVARCHAR(50)    NULL,
    Kg_Neto_Real            DECIMAL(12,3)   NULL,
    Kg_Proyectados          DECIMAL(12,3)   NULL,
    Pct_Cumplimiento        AS (CASE WHEN Kg_Proyectados > 0
                                THEN CAST(Kg_Neto_Real AS DECIMAL(10,2)) /
                                     Kg_Proyectados * 100
                                ELSE NULL END) PERSISTED,
    Cantidad_Jabas          INT             NULL,
    Peso_Promedio_Jaba_kg   AS (CASE WHEN Cantidad_Jabas > 0
                                THEN CAST(Kg_Neto_Real AS DECIMAL(10,3)) /
                                     Cantidad_Jabas
                                ELSE NULL END) PERSISTED,
    Fecha_Actualizacion     DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Gold.Mart_Proyecciones (
    ID_Mart_Proyeccion      BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Semana_Objetivo         INT             NOT NULL,
    Modulo                  INT             NULL,
    Turno                   INT             NULL,
    Variedad                NVARCHAR(100)   NOT NULL,
    Version_Escenario       NVARCHAR(50)    NOT NULL,
    Fecha_Generacion        DATE            NOT NULL,
    Fecha_Cutoff            DATETIME2       NOT NULL,
    Kg_Proyectados          DECIMAL(12,3)   NOT NULL,
    Kg_Real                 DECIMAL(12,3)   NULL,
    Error_MAPE              DECIMAL(8,4)    NULL,
    Desviacion_kg           AS (Kg_Real - Kg_Proyectados) PERSISTED,
    Flag_Override           BIT             NOT NULL DEFAULT 0,
    Motivo_Override         NVARCHAR(500)   NULL,
    Fecha_Actualizacion     DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Gold.Mart_Fenologia (
    ID_Mart_Fenologia       BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Semana_ISO              INT             NOT NULL,
    Modulo                  INT             NULL,
    Variedad                NVARCHAR(100)   NOT NULL,
    Color_Cinta             NVARCHAR(50)    NULL,
    Estado_Fenologico       NVARCHAR(100)   NOT NULL,
    Orden_Estado            INT             NOT NULL,
    Cantidad_Bayas          INT             NULL,
    Pct_Cosechable          DECIMAL(8,2)    NULL,
    Pct_Avance_Ciclo        AS (CAST(Orden_Estado AS DECIMAL(5,2)) / 8 * 100) PERSISTED,
    Brotes_Productivos      INT             NULL,
    Brotes_Vegetativos      INT             NULL,
    Ratio_Productivo_Veg    AS (CASE WHEN Brotes_Vegetativos > 0
                                THEN CAST(Brotes_Productivos AS DECIMAL(8,2)) /
                                     Brotes_Vegetativos
                                ELSE NULL END) PERSISTED,
    Fecha_Actualizacion     DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Gold.Mart_Clima (
    ID_Mart_Clima           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Semana_ISO              INT             NOT NULL,
    Modulo                  INT             NULL,
    Temp_Max_Promedio       DECIMAL(6,2)    NULL,
    Temp_Min_Promedio       DECIMAL(6,2)    NULL,
    Temp_Media              AS ((Temp_Max_Promedio + Temp_Min_Promedio) / 2) PERSISTED,
    Humedad_Relativa_Prom   DECIMAL(6,2)    NULL,
    Precipitacion_Total_mm  DECIMAL(8,2)    NULL,
    Radiacion_Promedio      DECIMAL(10,4)   NULL,
    VPD_Promedio            DECIMAL(8,4)    NULL,
    Dias_Con_Lluvia         INT             NULL,
    Fecha_Actualizacion     DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Gold.Mart_Pesos_Calibres (
    ID_Mart_Pesos           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Semana_ISO              INT             NOT NULL,
    Modulo                  INT             NULL,
    Variedad                NVARCHAR(100)   NOT NULL,
    Evaluador               NVARCHAR(200)   NULL,
    Cant_Bayas_Muestra      INT             NULL,
    Peso_Promedio_Baya_g    DECIMAL(6,2)    NULL,
    Peso_Proyectado_Baya_g  DECIMAL(6,2)    NULL,
    Tendencia_Peso          DECIMAL(6,2)    NULL,
    Estado_DQ               NVARCHAR(20)    NULL,
    Fecha_Actualizacion     DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Gold.Mart_Administrativo (
    ID_Mart_Admin           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Semana_ISO              INT             NOT NULL,
    DNI_Personal            NVARCHAR(20)    NOT NULL,
    Nombre_Personal         NVARCHAR(200)   NOT NULL,
    Sexo                    NVARCHAR(10)    NULL,
    Rol                     NVARCHAR(100)   NULL,
    Actividad               NVARCHAR(150)   NULL,
    Labor                   NVARCHAR(150)   NULL,
    Horas_Trabajadas        DECIMAL(8,2)    NULL,
    Dias_Trabajados         INT             NULL,
    Pct_Asertividad         DECIMAL(5,2)    NULL,
    Registros_Observados_SAP INT            NULL,
    Supervisor              NVARCHAR(200)   NULL,
    Fecha_Actualizacion     DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO


-- ============================================================
-- MDM — GOBIERNO DE DATOS (5)
-- Gestionado desde Streamlit. Ni Python ETL ni Power BI
-- modifican estas tablas directamente.
-- ============================================================

CREATE TABLE MDM.Catalogo_Variedades (
    ID_Catalogo_Variedad    INT             IDENTITY(1,1) PRIMARY KEY,
    Nombre_Canonico         NVARCHAR(100)   NOT NULL UNIQUE,
    Breeder                 NVARCHAR(100)   NULL,
    Es_Activa               BIT             NOT NULL DEFAULT 1,
    Fecha_Creacion          DATETIME2       NOT NULL DEFAULT GETDATE(),
    Fecha_Modificacion      DATETIME2       NULL
);
GO

CREATE TABLE MDM.Catalogo_Geografia (
    ID_Catalogo_Geografia   INT             IDENTITY(1,1) PRIMARY KEY,
    Fundo                   NVARCHAR(100)   NOT NULL,
    Sector                  NVARCHAR(100)   NULL,
    Modulo                  INT             NULL,
    Turno                   INT             NULL,
    Valvula                 NVARCHAR(50)    NULL,
    Cama                    NVARCHAR(50)    NULL,
    Codigo_SAP_Campo        NVARCHAR(50)    NULL,
    Es_Test_Block           BIT             NOT NULL DEFAULT 0,
    Es_Activa               BIT             NOT NULL DEFAULT 1,
    Fecha_Creacion          DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE MDM.Catalogo_Personal (
    ID_Catalogo_Personal    INT             IDENTITY(1,1) PRIMARY KEY,
    DNI                     NVARCHAR(20)    NOT NULL UNIQUE,
    Nombre_Completo         NVARCHAR(200)   NOT NULL,
    Rol                     NVARCHAR(100)   NULL,
    Sexo                    NVARCHAR(10)    NULL,
    Es_Activo               BIT             NOT NULL DEFAULT 1,
    Fecha_Creacion          DATETIME2       NOT NULL DEFAULT GETDATE(),
    Fecha_Modificacion      DATETIME2       NULL
);
GO

CREATE TABLE MDM.Diccionario_Homologacion (
    ID_Homologacion         INT             IDENTITY(1,1) PRIMARY KEY,
    Texto_Crudo             NVARCHAR(200)   NOT NULL,
    Valor_Canonico          NVARCHAR(200)   NOT NULL,
    Tabla_Origen            NVARCHAR(100)   NOT NULL,
    Campo_Origen            NVARCHAR(100)   NOT NULL,
    Score_Levenshtein       DECIMAL(5,4)    NULL,
    Aprobado_Por            NVARCHAR(20)    NULL,
    Fecha_Aprobacion        DATETIME2       NULL,
    Veces_Aplicado          INT             NOT NULL DEFAULT 0
);
GO

CREATE TABLE MDM.Cuarentena (
    ID_Cuarentena           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Tabla_Origen            NVARCHAR(100)   NOT NULL,
    Campo_Origen            NVARCHAR(100)   NOT NULL,
    Valor_Recibido          NVARCHAR(500)   NULL,
    Motivo                  NVARCHAR(200)   NOT NULL,
    Tipo_Regla              NVARCHAR(20)    NOT NULL,
    -- RANGO / CATALOGO / FORMATO / ESTADISTICA
    Score_Levenshtein       DECIMAL(5,4)    NULL,
    Estado                  NVARCHAR(20)    NOT NULL DEFAULT 'Pendiente',
    Valor_Corregido         NVARCHAR(500)   NULL,
    Aprobado_Por            NVARCHAR(20)    NULL,
    Fecha_Ingreso           DATETIME2       NOT NULL DEFAULT GETDATE(),
    Fecha_Resolucion        DATETIME2       NULL,
    ID_Registro_Origen      BIGINT          NULL
    -- FK lógica al registro Bronce que generó la alerta
);
GO


-- ============================================================
-- CONFIG — PARÁMETROS OPERATIVOS (4)
-- ============================================================

CREATE TABLE Config.Parametros_Pipeline (
    ID_Parametro            INT             IDENTITY(1,1) PRIMARY KEY,
    Nombre_Parametro        NVARCHAR(100)   NOT NULL UNIQUE,
    Valor                   NVARCHAR(500)   NOT NULL,
    Descripcion             NVARCHAR(300)   NULL,
    Fecha_Modificacion      DATETIME2       NULL,
    Modificado_Por          NVARCHAR(20)    NULL
);
GO

CREATE TABLE Config.Reglas_Validacion (
    ID_Regla                INT             IDENTITY(1,1) PRIMARY KEY,
    Tabla_Destino           NVARCHAR(100)   NOT NULL,
    Columna                 NVARCHAR(100)   NOT NULL,
    Valor_Min               DECIMAL(18,4)   NULL,
    Valor_Max               DECIMAL(18,4)   NULL,
    Tipo_Validacion         NVARCHAR(20)    NOT NULL,
    -- RANGO / CATALOGO / FORMATO / ESTADISTICA
    Accion                  NVARCHAR(20)    NOT NULL DEFAULT 'CUARENTENA',
    -- CUARENTENA / RECHAZAR / ALERTA
    Descripcion             NVARCHAR(300)   NULL,
    Activo                  BIT             NOT NULL DEFAULT 1,
    Fecha_Creacion          DATETIME2       NOT NULL DEFAULT GETDATE(),
    Fecha_Modificacion      DATETIME2       NULL
);
GO

CREATE TABLE Config.Analogos_Variedades (
    ID_Analogo              INT             IDENTITY(1,1) PRIMARY KEY,
    Variedad_Nueva          NVARCHAR(100)   NOT NULL,
    Variedad_Analoga        NVARCHAR(100)   NOT NULL,
    Factor_Vigor            DECIMAL(6,4)    NULL,
    Desfase_Fenologico_Dias INT             NULL,
    Activo                  BIT             NOT NULL DEFAULT 1,
    Fecha_Creacion          DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Config.Calendario_Entregables (
    ID_Entregable           INT             IDENTITY(1,1) PRIMARY KEY,
    Tipo_Escenario          NVARCHAR(50)    NOT NULL,
    Frecuencia              NVARCHAR(20)    NOT NULL,
    Dia_Generacion          NVARCHAR(20)    NULL,
    Hora_Limite             TIME            NULL,
    Activo                  BIT             NOT NULL DEFAULT 1
);
GO


-- ============================================================
-- AUDITORIA — TRAZABILIDAD COMPLETA (3)
-- ============================================================

CREATE TABLE Auditoria.Log_Carga (
    ID_Log_Carga            BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Nombre_Proceso          NVARCHAR(150)   NOT NULL,
    Tabla_Destino           NVARCHAR(100)   NOT NULL,
    Nombre_Archivo_Fuente   NVARCHAR(255)   NULL,
    Filas_Leidas            INT             NULL,
    Filas_Insertadas        INT             NULL,
    Filas_Cuarentena        INT             NULL,
    Filas_Rechazadas        INT             NULL,
    Duracion_Segundos       DECIMAL(10,2)   NULL,
    Estado_Proceso          NVARCHAR(20)    NOT NULL,
    -- Exitoso / Fallido / Parcial
    Mensaje_Error           NVARCHAR(MAX)   NULL,
    Fecha_Inicio            DATETIME2       NOT NULL,
    Fecha_Fin               DATETIME2       NULL
);
GO

CREATE TABLE Auditoria.Log_Decisiones_MDM (
    ID_Log_MDM              BIGINT          IDENTITY(1,1) PRIMARY KEY,
    ID_Cuarentena           BIGINT          NOT NULL REFERENCES MDM.Cuarentena(ID_Cuarentena),
    Accion                  NVARCHAR(20)    NOT NULL,  -- Aprobado / Rechazado
    Valor_Original          NVARCHAR(500)   NULL,
    Valor_Final             NVARCHAR(500)   NULL,
    Analista_DNI            NVARCHAR(20)    NOT NULL,
    Comentario              NVARCHAR(500)   NULL,
    Fecha_Decision          DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE Auditoria.Log_Versiones_Modelo (
    ID_Log_Modelo           BIGINT          IDENTITY(1,1) PRIMARY KEY,
    Nombre_Modelo           NVARCHAR(100)   NOT NULL,
    Version_Modelo          NVARCHAR(50)    NOT NULL,
    Tipo_Escenario          NVARCHAR(50)    NULL,
    Fecha_Cutoff            DATETIME2       NOT NULL,
    ID_Version_Datos        NVARCHAR(100)   NULL,
    WMAPE_Obtenido          DECIMAL(8,4)    NULL,
    MAE_Obtenido            DECIMAL(12,4)   NULL,
    Semanas_Evaluadas       INT             NULL,
    Estado                  NVARCHAR(20)    NOT NULL,  -- Exitoso / Fallido
    Mensaje_Error           NVARCHAR(MAX)   NULL,
    Fecha_Ejecucion         DATETIME2       NOT NULL DEFAULT GETDATE()
);
GO


-- ============================================================
-- DATOS SEMILLA — Valores fijos que no cambian
-- ============================================================

-- Dim_Escenario_Proyeccion — 7 escenarios oficiales
INSERT INTO Silver.Dim_Escenario_Proyeccion (Tipo_Escenario, Descripcion, Horizonte_Semanas, Frecuencia_Actualizacion) VALUES
('Presupuesto',           'Proyeccion anual inicial de campana',              52, 'Anual'),
('Preliminar',            'Primera revision de la proyeccion anual',          52, 'Anual'),
('Final',                 'Proyeccion anual cerrada — version oficial',       52, 'Anual'),
('Mensual',               'Ajuste mensual de la proyeccion',                  4,  'Mensual'),
('Six_Week',              'Proyeccion de 6 semanas adelante',                 6,  'Semanal'),
('Six_Week_Formalizado',  'Six Week validado y distribuido — version formal', 6,  'Semanal'),
('Ajuste_Semanal',        'Ajuste diario sobre el Six Week vigente',          1,  'Diaria');
GO

-- Dim_Estado_Workflow
INSERT INTO Silver.Dim_Estado_Workflow (Estado, Descripcion) VALUES
('Pendiente',   'Proyeccion generada, pendiente de revision'),
('Aprobado',    'Proyeccion revisada y aprobada para distribucion'),
('Rechazado',   'Proyeccion rechazada — requiere re-generacion');
GO

-- Dim_Estado_Fenologico — 9 estados del ciclo arándano
INSERT INTO Silver.Dim_Estado_Fenologico (Nombre_Estado, Orden_Estado) VALUES
('Boton Floral', 0),
('Flor',         1),
('Pequena',      2),
('Verde',        3),
('Inicio F1',    4),
('Inicio F2',    5),
('Crema',        6),
('Madura',       7),
('Cosechable',   8);
GO

-- Config.Reglas_Validacion — reglas de rango criticas
INSERT INTO Config.Reglas_Validacion (Tabla_Destino, Columna, Valor_Min, Valor_Max, Tipo_Validacion, Accion, Descripcion) VALUES
('Fact_Evaluacion_Pesos', 'Peso_Promedio_Baya_g', 0.5,   8.0,   'RANGO', 'CUARENTENA', 'Rango biologico baya arandano en gramos'),
('Fact_Cosecha_SAP',      'Kg_Neto_MP',           0,     9999,  'RANGO', 'CUARENTENA', 'Kg neto materia prima por registro SAP'),
('Fact_Conteo_Fenologico','Cantidad_Organos',      0,     9999,  'RANGO', 'CUARENTENA', 'Conteo de organos por planta'),
('Fact_Telemetria_Clima', 'Temperatura_Max_C',    -5,    50,    'RANGO', 'ALERTA',     'Temperatura maxima esperada en campo'),
('Fact_Telemetria_Clima', 'Humedad_Relativa_Pct',  0,    100,   'RANGO', 'CUARENTENA', 'Humedad relativa en porcentaje'),
('Fact_Fisiologia',       'Total_Organos',          0,   10000, 'RANGO', 'CUARENTENA', 'Total de organos tras agregacion por Tercio'),
('Fact_Peladas',          'Muestras',               1,   200,   'RANGO', 'CUARENTENA', 'Plantas muestreadas por punto en pelada'),
('Fact_Peladas',          'Bayas_Cosechables',      0,   9999,  'RANGO', 'CUARENTENA', 'Conteo de bayas cosechables en pelada');
GO

-- Surrogate -1 para Sin Evaluador en Dim_Personal
-- Cubre el 63% de registros sin evaluador en Fact_Conteo_Fenologico y Fact_Peladas
SET IDENTITY_INSERT Silver.Dim_Personal ON;
INSERT INTO Silver.Dim_Personal (ID_Personal, DNI, Nombre_Completo, Rol, Pct_Asertividad, Dias_Ausentismo)
VALUES (-1, '00000000', 'Sin Evaluador', 'Sin Asignar', 0, 0);
SET IDENTITY_INSERT Silver.Dim_Personal OFF;
GO

PRINT '====================================================';
PRINT 'ACP_Geographic_Phenology v2 creada correctamente.';
PRINT '====================================================';
PRINT 'Bronce   : 22 tablas (eliminadas: Reporte_Pesos, Seguimiento_Asistencia, Balance_Hidrico)';
PRINT 'Silver   : 21 tablas (10 Dims + 11 Facts — nueva: Fact_Peladas)';
PRINT 'Gold     :  6 tablas (Marts desnormalizados)';
PRINT 'MDM      :  5 tablas (Catalogo + Diccionario + Cuarentena)';
PRINT 'Config   :  4 tablas (Parametros + Reglas + Analogos + Calendario)';
PRINT 'Auditoria:  3 tablas (Log_Carga + Log_MDM + Log_Versiones)';
PRINT '----------------------------------------------------';
PRINT 'TOTAL    : 61 tablas';
PRINT '====================================================';
GO

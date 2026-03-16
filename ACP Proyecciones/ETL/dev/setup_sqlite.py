"""
dev/setup_sqlite.py
===================
Crea la base de datos SQLite de desarrollo: acp_dev.db

Replica el schema real del DWH (Bronce, MDM, Config, Auditoria)
para pruebas sin SQL Server.

Uso:
    python dev/setup_sqlite.py

Resultado:
    data/acp_dev.db  (si no existe, lo crea; si existe, no hace nada)
"""
import sqlite3
import os
from pathlib import Path

# ── Ruta del archivo de BD ────────────────────────────────────────────────
DB_PATH = Path(__file__).parent.parent / "data" / "acp_dev.db"


DDL = """
-- ══════════════════════════════════════════════════════
--  BRONCE — tablas de aterrizaje crudas
-- ══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS "Bronce.Evaluacion_Pesos" (
    ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    Archivo_Origen  TEXT,
    Fundo_Raw       TEXT,
    Sector_Raw      TEXT,
    Modulo_Raw      TEXT,
    Variedad_Raw    TEXT,
    Evaluador_Raw   TEXT,
    Fecha_Raw       TEXT,
    PesoBaya_Raw    TEXT,
    Firmeza_Raw     TEXT,
    Color_Brix_Raw  TEXT,
    Cargado_En      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS "Bronce.Conteo_Fruta" (
    ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    Archivo_Origen  TEXT,
    Fundo_Raw       TEXT,
    Sector_Raw      TEXT,
    Modulo_Raw      TEXT,
    Variedad_Raw    TEXT,
    Evaluador_Raw   TEXT,
    Fecha_Raw       TEXT,
    Estado_Raw      TEXT,
    Conteo_Raw      TEXT,
    Cinta_Raw       TEXT,
    Cargado_En      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS "Bronce.Peladas" (
    ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    Archivo_Origen  TEXT,
    Fundo_Raw       TEXT,
    Sector_Raw      TEXT,
    Modulo_Raw      TEXT,
    Variedad_Raw    TEXT,
    Evaluador_Raw   TEXT,
    Fecha_Raw       TEXT,
    Muestras_Raw    TEXT,
    Peso_Peladas_Raw TEXT,
    Cargado_En      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS "Bronce.Data_SAP" (
    ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    Archivo_Origen  TEXT,
    Fundo_Raw       TEXT,
    Sector_Raw      TEXT,
    Modulo_Raw      TEXT,
    Des_Variedad_Raw TEXT,
    Fecha_Raw       TEXT,
    Kg_Bruto_Raw    TEXT,
    Kg_Tara_Raw     TEXT,
    Kg_Neto_Raw     TEXT,
    Cargado_En      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS "Bronce.Ciclos_Fenologicos" (
    ID                  INTEGER PRIMARY KEY AUTOINCREMENT,
    Archivo_Origen      TEXT,
    Fundo_Raw           TEXT,
    Sector_Raw          TEXT,
    Modulo_Raw          TEXT,
    Variedad_Raw        TEXT,
    Evaluador_Raw       TEXT,
    Fecha_Raw           TEXT,
    IDESTADOCICLO_Raw   TEXT,
    Cargado_En          TEXT DEFAULT (datetime('now'))
);

-- ══════════════════════════════════════════════════════
--  MDM — tablas maestras y cuarentena
-- ══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS "MDM.Catalogo_Variedades" (
    ID_Variedad         INTEGER PRIMARY KEY AUTOINCREMENT,
    Nombre_Canonico     TEXT NOT NULL,
    Breeder             TEXT,
    Es_Activa           INTEGER DEFAULT 1,
    Fecha_Creacion      TEXT DEFAULT (datetime('now')),
    Fecha_Modificacion  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS "MDM.Cuarentena" (
    ID_Cuarentena   INTEGER PRIMARY KEY AUTOINCREMENT,
    Tabla_Origen    TEXT NOT NULL,
    Columna_Origen  TEXT NOT NULL,
    Valor_Raw       TEXT,
    Motivo          TEXT,
    Severidad       TEXT DEFAULT 'ALTO',
    Estado          TEXT DEFAULT 'Pendiente',
    Fecha_Ingreso   TEXT DEFAULT (datetime('now')),
    Resuelto_En     TEXT,
    Resuelto_Por    TEXT,
    Decision        TEXT
);

CREATE TABLE IF NOT EXISTS "MDM.Homologacion" (
    ID_Homologacion         INTEGER PRIMARY KEY AUTOINCREMENT,
    Texto_Crudo             TEXT NOT NULL,
    Valor_Canonico_Sugerido TEXT,
    Score                   REAL,
    Tabla_Origen            TEXT,
    Veces_Visto             INTEGER DEFAULT 1,
    Aprobado                INTEGER DEFAULT 0,
    Aprobado_Por            TEXT,
    Fecha_Sugerencia        TEXT DEFAULT (datetime('now')),
    Fecha_Aprobacion        TEXT
);

-- ══════════════════════════════════════════════════════
--  CONFIG — reglas y parámetros
-- ══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS "Config.Parametros_Pipeline" (
    ID_Parametro        INTEGER PRIMARY KEY AUTOINCREMENT,
    Nombre_Parametro    TEXT UNIQUE NOT NULL,
    Valor               TEXT,
    Descripcion         TEXT,
    Fecha_Modificacion  TEXT DEFAULT (datetime('now')),
    Modificado_Por      TEXT DEFAULT 'SISTEMA'
);

CREATE TABLE IF NOT EXISTS "Config.Reglas_Validacion" (
    ID_Regla            INTEGER PRIMARY KEY AUTOINCREMENT,
    Tabla_Destino       TEXT,
    Columna             TEXT,
    Valor_Min           REAL,
    Valor_Max           REAL,
    Tipo_Validacion     TEXT DEFAULT 'RANGO',
    Accion              TEXT DEFAULT 'RECHAZAR',
    Descripcion         TEXT,
    Activo              INTEGER DEFAULT 1,
    Fecha_Creacion      TEXT DEFAULT (datetime('now')),
    Fecha_Modificacion  TEXT DEFAULT (datetime('now'))
);

-- ══════════════════════════════════════════════════════
--  AUDITORIA — log de cargas
-- ══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS "Auditoria.Log_Carga" (
    ID_Log          INTEGER PRIMARY KEY AUTOINCREMENT,
    Tabla_Destino   TEXT,
    Archivo_Origen  TEXT,
    Filas_Insertadas INTEGER DEFAULT 0,
    Filas_Rechazadas INTEGER DEFAULT 0,
    Inicio_Carga    TEXT DEFAULT (datetime('now')),
    Fin_Carga       TEXT,
    Estado          TEXT DEFAULT 'OK',
    Mensaje         TEXT
);
"""

SEED_DATA = """
-- Parámetros del pipeline
INSERT OR IGNORE INTO "Config.Parametros_Pipeline"
    (Nombre_Parametro, Valor, Descripcion) VALUES
    ('CAMPANA_ACTIVA',    '2026-A', 'Identificador de la campaña agrícola activa'),
    ('PESO_BAYA_MIN',     '0.5',   'Peso mínimo de baya aceptado (gramos)'),
    ('PESO_BAYA_MAX',     '8.0',   'Peso máximo de baya aceptado (gramos)'),
    ('LEVENSHTEIN_UMBRAL','0.65',  'Score mínimo para sugerir homologación automática'),
    ('CHUNK_SIZE_INSERT', '500',   'Tamaño de bloque para inserciones en BD'),
    ('SCORE_AUTO_APROBACION','0.90','Score mínimo para aprobar homologación sin revisión'),
    ('DIAS_RETENCION_CUARENTENA','90','Días que se conservan registros en cuarentena'),
    ('LOG_NIVEL',         'INFO',  'Nivel de log del pipeline');

-- Reglas de validación
INSERT OR IGNORE INTO "Config.Reglas_Validacion"
    (Tabla_Destino, Columna, Valor_Min, Valor_Max, Tipo_Validacion, Accion, Descripcion) VALUES
    ('Bronce.Evaluacion_Pesos',    'PesoBaya_Raw',       0.5,  8.0,  'RANGO', 'RECHAZAR', 'Peso baya fuera de rango biológico 0.5-8.0g'),
    ('Bronce.Peladas',             'Muestras_Raw',       1,    NULL, 'RANGO', 'RECHAZAR', 'Muestras debe ser mayor o igual a 1'),
    ('Bronce.Conteo_Fruta',        'Conteo_Raw',         0,    9999, 'RANGO', 'RECHAZAR', 'Conteo de fruta fuera de rango'),
    ('Bronce.Ciclos_Fenologicos',  'IDESTADOCICLO_Raw',  0,    8,    'RANGO', 'RECHAZAR', 'Estado de ciclo debe estar entre 0 y 8');

-- Catálogo de variedades inicial
INSERT OR IGNORE INTO "MDM.Catalogo_Variedades"
    (Nombre_Canonico, Breeder, Es_Activa) VALUES
    ('Biloxi',      'Mississippi State',  1),
    ('Emerald',     'University of Florida', 1),
    ('Misty',       'University of Florida', 1),
    ('Sekoya Pop',  'Sekoya',             1),
    ('Megacrisp',   'Fall Creek',         1),
    ('Draper',      'Fall Creek',         1),
    ('O''Neal',     'University of Florida', 1),
    ('Blueray',     'Corneille',          1),
    ('Snowchaser',  'Fall Creek',         1),
    ('StarBurst',   'ACP Propio',         1);
"""


def crear_bd():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    existe = DB_PATH.exists()
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    cursor.executescript(DDL)
    if not existe:
        cursor.executescript(SEED_DATA)
        print(f"✅ Base de datos creada con seed data: {DB_PATH}")
    else:
        print(f"ℹ️  Base de datos ya existe — no se sobreescriben datos: {DB_PATH}")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    crear_bd()
    print("\nTablas creadas:")
    conn = sqlite3.connect(str(DB_PATH))
    tablas = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    for (t,) in tablas:
        n = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        print(f"  {t:45} {n} filas")
    conn.close()

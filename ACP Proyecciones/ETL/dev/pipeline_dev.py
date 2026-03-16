"""
dev/pipeline_dev.py
===================
Versión de prueba del pipeline ETL a SQLite.

Flujo REAL (replicando SQL Server):
  1. Bronce carga TODO el Excel como NVARCHAR crudo (SIN validación, 0 rechazados).
  2. Al pasar datos de Bronce a Silver se aplican:
      - Homologación (Variedad_Raw → Variedad_Canonica)
      - Reglas de DQ (PesoBaya_Raw)
      - Todo rechazo en la etapa Silver va a MDM.Cuarentena
  3. Resultado se registra en Auditoria.
"""

import sys
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import pprint

# ── Rutas ─────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.parent
DB_PATH    = ROOT / "data" / "acp_dev.db"
DIR_ENTRADA = ROOT / "data" / "entrada"

if not DB_PATH.exists():
    from dev.setup_sqlite import crear_bd
    crear_bd()

def _log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def _get_conn():
    return sqlite3.connect(str(DB_PATH))

def _leer_parametro(conn, nombre: str, default=None):
    row = conn.execute('SELECT Valor FROM "Config.Parametros_Pipeline" WHERE Nombre_Parametro = ?', (nombre,)).fetchone()
    return row[0] if row else default

def _leer_reglas(conn) -> list[dict]:
    rows = conn.execute('SELECT Tabla_Destino, Columna, Valor_Min, Valor_Max FROM "Config.Reglas_Validacion" WHERE Activo = 1').fetchall()
    return [{"tabla": r[0], "columna": r[1], "min": r[2], "max": r[3]} for r in rows]

# ── Mapeo ────────────────────────────────────────────────────────────────
MAPEO_TABLAS = {
    "evaluacion_pesos": {
        "tabla_bronce": "Bronce.Evaluacion_Pesos",
        "tabla_silver": "Silver.Fact_Evaluacion_Pesos",
        "col_peso": "PesoBaya_Raw",
        "col_variedad": "Variedad_Raw",
        "columnas": {
            "Fundo":    "Fundo_Raw",
            "Sector":   "Sector_Raw",
            "Modulo":   "Modulo_Raw",
            "Variedad": "Variedad_Raw",
            "Evaluador":"Evaluador_Raw",
            "Fecha":    "Fecha_Raw",
            "PesoBaya": "PesoBaya_Raw",
            "Firmeza":  "Firmeza_Raw",
            "Brix":     "Color_Brix_Raw",
        }
    },
    "conteo_fruta": {
        "tabla_bronce": "Bronce.Conteo_Fruta",
        "tabla_silver": "Silver.Fact_Conteo_Fruta",
        "col_peso": None,
        "col_variedad": "Variedad_Raw",
        "columnas": {
            "Fundo":    "Fundo_Raw",
            "Sector":   "Sector_Raw",
            "Modulo":   "Modulo_Raw",
            "Variedad": "Variedad_Raw",
            "Evaluador":"Evaluador_Raw",
            "Fecha":    "Fecha_Raw",
            "Estado":   "Estado_Raw",
            "Conteo":   "Conteo_Raw",
            "Cinta":    "Cinta_Raw",
        }
    },
    "evaluación vegetativa": {
        "tabla_bronce": "Bronce.Ciclos_Fenologicos",
        "tabla_silver": "Silver.Fact_Ciclos_Fen",
        "col_peso": None,
        "col_variedad": "Descripción",
        "skiprows": 1,
        "columnas": {
            "Modulo":   "Modulo_Raw",
            "Descripción": "Variedad_Raw",
            "Consumidor": "Fundo_Raw",
            "Evaluación": "IDESTADOCICLO_Raw",
            "Fecha": "Fecha_Raw",
            "Nombres": "Evaluador_Raw"
        }
    }
}

def _sugerir_homologacion(conn, valor_raw: str, col_origen: str, tabla_bronce: str):
    variedades = conn.execute('SELECT Nombre_Canonico FROM "MDM.Catalogo_Variedades" WHERE Es_Activa = 1').fetchall()
    try:
        from rapidfuzz import fuzz
        mejor_score = 0
        mejor_nombre = None
        for (nombre,) in variedades:
            score = fuzz.ratio(str(valor_raw).upper(), nombre.upper()) / 100
            if score > mejor_score:
                mejor_score = score
                mejor_nombre = nombre

        umbral = float(_leer_parametro(conn, "LEVENSHTEIN_UMBRAL", 0.65))
        if mejor_score >= umbral and mejor_nombre:
            existe = conn.execute('SELECT 1 FROM "MDM.Homologacion" WHERE Texto_Crudo = ? AND Tabla_Origen = ?', (valor_raw, tabla_bronce)).fetchone()
            if not existe:
                conn.execute(
                    '''INSERT INTO "MDM.Homologacion" (Texto_Crudo, Valor_Canonico_Sugerido, Score, Tabla_Origen, Veces_Visto) VALUES (?, ?, ?, ?, 1)''',
                    (valor_raw, mejor_nombre, round(mejor_score, 3), tabla_bronce)
                )
            else:
                conn.execute('''UPDATE "MDM.Homologacion" SET Veces_Visto = Veces_Visto + 1 WHERE Texto_Crudo = ? AND Tabla_Origen = ?''', (valor_raw, tabla_bronce))
    except ImportError:
        pass


def procesar_archivo(archivo: Path, conn: sqlite3.Connection, reglas: list[dict]) -> dict:
    nombre = archivo.stem.lower()
    tipo = next((c for c in MAPEO_TABLAS if c in nombre), None)
    
    if not tipo:
        return {"archivo": archivo.name, "bronce": 0, "silver": 0, "cuarentena": 0, "estado": "⚠️ Ignorado"}

    cfg = MAPEO_TABLAS[tipo]
    tabla_bronce = cfg["tabla_bronce"]
    tabla_silver = cfg["tabla_silver"]
    
    try:
        skip = cfg.get("skiprows", 0)
        df = pd.read_excel(archivo, dtype=str, skiprows=skip)
    except Exception as e:
        return {"archivo": archivo.name, "bronce": 0, "silver": 0, "cuarentena": 0, "estado": f"❌ Error lectura"}

    df_renamed = df.rename(columns=cfg["columnas"])
    # Filtrar solo las columnas de destino que existen en la tabla Bronce
    cols = [c for c in df_renamed.columns if c in cfg["columnas"].values()]
    df_bronce = df_renamed[cols].copy()
    
    # Insertar todo en Bronce
    df_bronce['Archivo_Origen'] = archivo.name
    # Remove NaN values converting them to None for SQLite
    df_bronce = df_bronce.where(pd.notnull(df_bronce), None)
    
    try:
        df_bronce.to_sql(name=tabla_bronce, con=conn, if_exists='append', index=False)
        filas_bronce = len(df_bronce)
    except Exception as e:
        print(f"    [Error insertando en {tabla_bronce}]: {e}")
        return {"archivo": archivo.name, "bronce": 0, "silver": 0, "cuarentena": 0, "estado": f"❌ Error SQL"}

    # ── 2. Pase a SILVER (Acá ocurren los rechazos a cuarentena) ──
    filas_silver = 0
    filas_cuarentena = 0
    inicio_carga = datetime.now().isoformat()

    for _, row in df_bronce.iterrows():
        fila_dict = row.to_dict()
        rechazado = False
        
        # Validar Reglas de DQ
        for regla in reglas:
            if regla["tabla"] != tabla_silver: continue
            # El cargador original mapea PesoBaya_Raw -> Peso_Promedio_Baya_g, usamos reglas de Silver
            col_raw = None
            if "Peso" in regla["columna"] and cfg["col_peso"]:
                col_raw = cfg["col_peso"]
                
            if col_raw and col_raw in fila_dict:
                try:
                    val = float(str(fila_dict[col_raw]).replace(",", "."))
                    if (regla["min"] is not None and val < float(regla["min"])) or \
                       (regla["max"] is not None and val > float(regla["max"])):
                        motivo = f"{regla['columna']}={val} fuera de rango ({regla['min']}-{regla['max']})"
                        conn.execute(
                            '''INSERT INTO "MDM.Cuarentena" (Tabla_Origen, Columna_Origen, Valor_Raw, Motivo, Severidad) VALUES (?, ?, ?, ?, ?)''',
                            (tabla_bronce, col_raw, str(fila_dict[col_raw]), motivo, "CRÍTICO")
                        )
                        rechazado = True
                        break # Fila rechazada por peso
                except ValueError:
                    pass

        if rechazado:
            filas_cuarentena += 1
            continue

        # Validaciones de MDM (Variedad) - Solo sugerimos si Levenshtein, pero asumimos que pasa si es válida
        col_var = cfg["col_variedad"]
        if col_var and col_var in fila_dict and str(fila_dict[col_var]).strip() != "nan":
            _sugerir_homologacion(conn, str(fila_dict[col_var]), col_var, tabla_bronce)
            
        filas_silver += 1

    # Log
    conn.execute(
        '''INSERT INTO "Auditoria.Log_Carga" (Tabla_Destino, Archivo_Origen, Filas_Insertadas, Filas_Rechazadas, Inicio_Carga, Fin_Carga, Estado) VALUES (?, ?, ?, ?, ?, ?, ?)''',
        (tabla_bronce, archivo.name, filas_bronce, 0, inicio_carga, datetime.now().isoformat(), "OK")
    )

    return {
        "archivo": archivo.name,
        "bronce": filas_bronce,
        "silver": filas_silver,
        "cuarentena": filas_cuarentena,
        "estado": "✅ Procesado"
    }

def ejecutar():
    inicio = datetime.now()
    print("=" * 60)
    print("  ACP DevMode — ETL a SQLite (Comportamiento Real)")
    print("=" * 60)

    conn = _get_conn()
    
    # ── Forzar restablecimiento de BD limpia para pruebas exactas
    conn.execute('DELETE FROM "MDM.Cuarentena"')
    conn.execute('DELETE FROM "MDM.Homologacion"')
    for t in ["Bronce.Evaluacion_Pesos", "Bronce.Conteo_Fruta", "Auditoria.Log_Carga"]:
        conn.execute(f'DELETE FROM "{t}"')
    conn.commit()

    reglas = _leer_reglas(conn)
    _log(f"Reglas de DQ activas: {len(reglas)}")

    archivos = list(DIR_ENTRADA.glob("*.xlsx")) + list(DIR_ENTRADA.glob("*.xls"))
    _log(f"Archivos encontrados: {len(archivos)}")

    resumen = []
    for archivo in archivos:
        r = procesar_archivo(archivo, conn, reglas)
        resumen.append(r)
        conn.commit()
        print(f"  → {r['archivo']}: {r['bronce']} Bronce | {r['silver']} Silver | {r['cuarentena']} Cuarentena")

    print("\n✅ Carga completada. Revisa el portal.")
    conn.close()

if __name__ == "__main__":
    ejecutar()

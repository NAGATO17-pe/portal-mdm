"""
dev/conexion_sqlite.py
======================
Reemplazo de config/conexion.py pero para SQLite de desarrollo.
Expone el mismo API: obtener_engine(), verificar_conexion()

NO modifica config/conexion.py — es un archivo adicional independiente.

Uso desde pipeline_dev.py:
    from dev.conexion_sqlite import obtener_engine, verificar_conexion
"""
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ── Ruta del archivo SQLite ───────────────────────────────────────────────
DB_PATH = Path(__file__).parent.parent / "data" / "acp_dev.db"


def obtener_engine() -> Engine:
    """
    Retorna engine SQLAlchemy apuntando a SQLite local.
    Mismo nombre de función que config/conexion.py — intercambiable.
    """
    if not DB_PATH.exists():
        print(f"⚠️  acp_dev.db no encontrada. Ejecuta primero: python dev/setup_sqlite.py")
    url = f"sqlite:///{DB_PATH}"
    return create_engine(url, echo=False)


def verificar_conexion() -> bool:
    """Verifica que el archivo SQLite existe y es accesible."""
    try:
        engine = obtener_engine()
        with engine.connect() as conn:
            tablas = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
        print(f"✅ SQLite OK — {len(tablas)} tablas en {DB_PATH.name}")
        return True
    except Exception as e:
        print(f"❌ Error SQLite: {e}")
        return False

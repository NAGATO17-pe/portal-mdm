from sqlalchemy import text
from config.conexion import obtener_engine

e = obtener_engine()
with e.connect() as conn:
    cols = conn.execute(text(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA='MDM' AND TABLE_NAME='Log_Decisiones_MDM' ORDER BY ORDINAL_POSITION"
    )).fetchall()
    print([c[0] for c in cols])

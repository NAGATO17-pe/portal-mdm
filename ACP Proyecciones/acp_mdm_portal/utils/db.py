"""
utils/db.py — Conexion a Base de Datos SQL Server para el Portal MDM
Importa directamente desde el ETL para no duplicar la logica de conexion.
"""
import sys
import os
import pandas as pd
import streamlit as st
from sqlalchemy import text

# Añadir el directorio ETL al path para poder importar config.conexion
_ETL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'ETL'))
if _ETL_DIR not in sys.path:
    sys.path.append(_ETL_DIR)

from config.conexion import obtener_engine as _obtener_engine_etl  # noqa: E402


@st.cache_resource
def obtener_engine():
    """Engine compartido (se crea una sola vez por sesion de Streamlit)."""
    return _obtener_engine_etl()


def ejecutar_query(query: str, params: dict | None = None) -> pd.DataFrame:
    """Ejecuta una consulta SELECT y retorna un DataFrame."""
    with obtener_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


def verificar_conexion() -> bool:
    """Retorna True si la conexion a SQL Server esta activa."""
    try:
        with obtener_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        st.error(f"Error de conexion a la base de datos: {e}")
        return False

"""
utils/db.py — Capa de datos del Portal MDM ACP (Enterprise)
=============================================================
Funciones centralizadas de acceso a datos:
  - Engine SQLAlchemy con pool tuning para uso concurrente
  - Queries simples y paginadas (Server-side con OFFSET/FETCH)
  - Verificación de conexión + health check descriptivo
  - Registro de auditoría de cambios manuales
"""

import os
import sys
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text

# Añadir el directorio ETL al path para importar config.conexion
_ETL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'ETL'))
if _ETL_DIR not in sys.path:
    sys.path.append(_ETL_DIR)

from config.conexion import obtener_engine as _obtener_engine_etl  # noqa: E402


# ── Engine con pool tuning ────────────────────────────────────────────────────

@st.cache_resource
def obtener_engine():
    """
    Engine compartido con pool sizing para soportar múltiples
    usuarios concurrentes sin agotar conexiones.
    """
    engine = _obtener_engine_etl()
    # Tuning del pool si el engine lo soporta
    engine.pool._pool.maxsize = 10        # max conexiones activas
    return engine


# ── Queries estándar ──────────────────────────────────────────────────────────

def ejecutar_query(query: str, params: dict | None = None) -> pd.DataFrame:
    """Ejecuta una consulta SELECT y retorna un DataFrame completo."""
    with obtener_engine().connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


# ── Paginación Server-Side (SQL OFFSET/FETCH) ────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def ejecutar_query_paginado(
    query_base: str,
    order_by: str,
    page: int = 1,
    page_size: int = 15,
    params: dict | None = None,
) -> tuple[pd.DataFrame, int]:
    """
    Ejecuta una consulta con paginación delegada a SQL Server.

    En lugar de traer TODOS los registros a Python (anti-patrón para
    tablas grandes), envía OFFSET/FETCH al motor SQL para que solo
    viaje por la red la página solicitada.

    Args:
        query_base:  SELECT sin ORDER BY final (ej: "SELECT * FROM Bronce.X WHERE ...")
        order_by:    Cláusula ORDER BY (ej: "Fecha_Sistema DESC")
        page:        Número de página (1-indexed)
        page_size:   Registros por página
        params:      Parámetros opcionales para la query

    Returns:
        (DataFrame con la página, total_count)
    """
    offset = (max(1, page) - 1) * page_size

    # Query para contar registros totales
    count_query = f"SELECT COUNT(*) AS total FROM ({query_base}) AS _conteo"

    # Query paginada con OFFSET/FETCH (SQL Server 2012+)
    paged_query = f"""
        {query_base}
        ORDER BY {order_by}
        OFFSET :_offset ROWS
        FETCH NEXT :_fetch ROWS ONLY
    """

    merged_params = dict(params or {})
    merged_params["_offset"] = offset
    merged_params["_fetch"]  = page_size

    with obtener_engine().connect() as conn:
        total = conn.execute(text(count_query), params or {}).scalar() or 0
        df    = pd.read_sql(text(paged_query), conn, params=merged_params)

    return df, int(total)


# ── Verificación de conexión ──────────────────────────────────────────────────

def verificar_conexion() -> bool:
    """Retorna True si la conexión a SQL Server está activa."""
    try:
        with obtener_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        st.error(f"Error de conexión a la base de datos: {e}")
        return False


def health_check() -> dict:
    """
    Diagnóstico detallado de la conexión a SQL Server.
    Retorna un dict con: conectado, base_datos, version, latencia_ms, uptime.
    """
    import time

    info = {
        "conectado":   False,
        "base_datos":  "—",
        "version":     "—",
        "latencia_ms": "—",
        "uptime":      "—",
    }
    try:
        t0 = time.perf_counter()
        with obtener_engine().connect() as conn:
            row = conn.execute(text("""
                SELECT
                    DB_NAME()                                       AS base,
                    SERVERPROPERTY('ProductVersion')                AS ver,
                    DATEDIFF(HOUR, sqlserver_start_time, GETDATE()) AS uptime_h
                FROM sys.dm_os_sys_info
            """)).fetchone()
            t1 = time.perf_counter()

            info["conectado"]   = True
            info["base_datos"]  = str(row.base)
            info["version"]     = f"SQL {row.ver}"
            info["latencia_ms"] = f"{(t1 - t0) * 1000:.0f} ms"
            info["uptime"]      = f"{row.uptime_h}h"
    except Exception:
        pass

    return info

@st.cache_data(ttl=3600)
def obtener_query_cacheada(query: str, params: dict | None = None) -> pd.DataFrame:
    """Para catálogos estáticos que casi nunca cambian (TTL 1 hora)."""
    return ejecutar_query(query, params)


# ── Ejecución de DML (INSERT/UPDATE) ─────────────────────────────────────────

def ejecutar_dml(query: str, params: dict | None = None) -> int:
    """
    Ejecuta un INSERT/UPDATE/DELETE y retorna el número de filas afectadas.
    Hace commit automáticamente.
    """
    with obtener_engine().begin() as conn:
        result = conn.execute(text(query), params or {})
        return result.rowcount


# ── Auditoría de Cambios ──────────────────────────────────────────────────────

def registrar_cambio(
    tabla: str,
    registro_id: str,
    campo: str,
    valor_anterior: str,
    valor_nuevo: str,
    usuario: str = "portal_user",
    accion: str = "UPDATE",
) -> None:
    """
    Registra un cambio manual realizado desde el portal en la tabla
    de auditoría. Se usa para trazabilidad completa de decisiones.

    Si la tabla Auditoria.Cambios_Portal no existe, falla silenciosamente
    (el portal no debe romperse por falta de tabla de auditoría).
    """
    try:
        ejecutar_dml("""
            INSERT INTO Auditoria.Cambios_Portal (
                Tabla_Afectada, Registro_ID, Campo,
                Valor_Anterior, Valor_Nuevo,
                Usuario, Accion, Fecha_Cambio
            ) VALUES (
                :tabla, :registro_id, :campo,
                :valor_anterior, :valor_nuevo,
                :usuario, :accion, GETDATE()
            )
        """, {
            "tabla":           tabla,
            "registro_id":     str(registro_id),
            "campo":           campo,
            "valor_anterior":  str(valor_anterior),
            "valor_nuevo":     str(valor_nuevo),
            "usuario":         usuario,
            "accion":          accion,
        })
    except Exception:
        pass  # Tabla no existe aún — no romper el portal

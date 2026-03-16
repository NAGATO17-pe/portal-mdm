"""
parametros.py
=============
Lee Config.Parametros_Pipeline desde SQL Server.
El pipeline usa estos valores en tiempo de ejecución.
Cambiarlos en Streamlit afecta el comportamiento del ETL
sin tocar código.
"""

from sqlalchemy import text
from config.conexion import obtener_engine


_cache_parametros: dict[str, str] = {}


def cargar_parametros() -> dict[str, str]:
    """
    Carga todos los parámetros activos desde Config.Parametros_Pipeline.
    Guarda en cache para no repetir la consulta en la misma ejecución.
    """
    global _cache_parametros

    if _cache_parametros:
        return _cache_parametros

    engine = obtener_engine()

    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT Parametro, Valor
            FROM Config.Parametros_Pipeline
        """))
        _cache_parametros = {
            fila.Parametro: fila.Valor
            for fila in resultado.fetchall()
        }

    return _cache_parametros


def obtener(parametro: str, default: str = '') -> str:
    """
    Retorna el valor de un parámetro.
    Si no existe retorna el default indicado.
    """
    parametros = cargar_parametros()
    return parametros.get(parametro, default)


def obtener_int(parametro: str, default: int = 0) -> int:
    """
    Retorna el valor de un parámetro como entero.
    """
    try:
        return int(obtener(parametro, str(default)))
    except (ValueError, TypeError):
        return default


def obtener_float(parametro: str, default: float = 0.0) -> float:
    """
    Retorna el valor de un parámetro como float.
    """
    try:
        return float(obtener(parametro, str(default)))
    except (ValueError, TypeError):
        return default


def limpiar_cache() -> None:
    """
    Limpia el cache — llamar al inicio de cada ejecución del pipeline.
    """
    global _cache_parametros
    _cache_parametros = {}

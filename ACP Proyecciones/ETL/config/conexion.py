"""
conexion.py
===========
Conexión a SQL Server via pyodbc + SQLAlchemy.
Las credenciales vienen del archivo .env — nunca hardcodeadas.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()


def obtener_cadena_conexion() -> str:
    """
    Construye la cadena de conexión desde variables de entorno.
    """
    servidor = os.getenv('DB_SERVIDOR')
    base     = os.getenv('DB_NOMBRE', 'ACP_Geographic_Phenology')
    usuario  = os.getenv('DB_USUARIO')
    clave    = os.getenv('DB_CLAVE')
    driver   = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')

    # Autenticación Windows (sin usuario/clave)
    if not usuario:
        return (
            f'mssql+pyodbc://@{servidor}/{base}'
            f'?driver={driver.replace(" ", "+")}'
            f'&trusted_connection=yes'
        )

    # Autenticación SQL Server
    return (
        f'mssql+pyodbc://{usuario}:{clave}@{servidor}/{base}'
        f'?driver={driver.replace(" ", "+")}'
    )


def obtener_engine() -> Engine:
    """
    Retorna el engine de SQLAlchemy.
    Reutilizar este engine en todo el ETL — no crear uno por módulo.
    """
    cadena = obtener_cadena_conexion()
    return create_engine(cadena, fast_executemany=True)


def verificar_conexion() -> bool:
    """
    Verifica que la conexión a SQL Server funciona.
    Retorna True si conecta, False si falla.
    """
    try:
        engine = obtener_engine()
        with engine.connect() as conexion:
            resultado = conexion.execute(
                text('SELECT DB_NAME() AS base_activa')
            )
            fila = resultado.fetchone()
            print(f'✅ Conectado a: {fila.base_activa}')
            return True
    except Exception as error:
        print(f'❌ Error de conexión: {error}')
        return False


if __name__ == '__main__':
    verificar_conexion()

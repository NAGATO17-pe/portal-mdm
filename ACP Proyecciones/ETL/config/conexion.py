"""
conexion.py
===========
Conexion a SQL Server via pyodbc + SQLAlchemy.
Usa odbc_connect directo para evitar problemas de parsing de URL.
"""

import os
import urllib
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()


def obtener_engine() -> Engine:
    servidor = os.getenv('DB_SERVIDOR', 'LCP-PAG-PRACTIC')
    base     = os.getenv('DB_NOMBRE', 'ACP_DataWarehose_Proyecciones')
    usuario  = os.getenv('DB_USUARIO')
    clave    = os.getenv('DB_CLAVE')
    driver   = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')

    if not usuario:
        cadena_pyodbc = (
            f'DRIVER={{{driver}}};'
            f'SERVER={servidor};'
            f'DATABASE={base};'
            f'Trusted_Connection=yes;'
            f'TrustServerCertificate=yes;'
        )
    else:
        cadena_pyodbc = (
            f'DRIVER={{{driver}}};'
            f'SERVER={servidor};'
            f'DATABASE={base};'
            f'UID={usuario};'
            f'PWD={clave};'
            f'TrustServerCertificate=yes;'
        )

    cadena_url = (
        'mssql+pyodbc:///?odbc_connect='
        + urllib.parse.quote_plus(cadena_pyodbc)
    )

    return create_engine(cadena_url, fast_executemany=True)


def verificar_conexion() -> bool:
    try:
        engine = obtener_engine()
        with engine.connect() as conexion:
            resultado = conexion.execute(
                text('SELECT DB_NAME() AS base_activa')
            )
            fila = resultado.fetchone()
            print(f'Conectado a: {fila.base_activa}')
            return True
    except Exception as error:
        print(f'Error de conexion: {error}')
        return False


if __name__ == '__main__':
    verificar_conexion()

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from sqlalchemy import text


RAIZ_PROYECTO = Path(__file__).resolve().parents[1]
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

from config.conexion import obtener_engine  # noqa: E402


def pytest_addoption(parser):
    parser.addoption(
        "--sql-integration",
        action="store_true",
        default=False,
        help="Ejecuta pruebas ETL que requieren SQL Server real.",
    )


@pytest.fixture(scope='session')
def engine(pytestconfig):
    if not pytestconfig.getoption("--sql-integration"):
        pytest.skip(
            "Pruebas ETL de integracion SQL omitidas. "
            "Usa --sql-integration para ejecutarlas contra SQL Server real."
        )
    return obtener_engine()


def scalar(engine, sql: str, **params):
    with engine.connect() as conexion:
        return conexion.execute(text(sql), params).scalar()


def fetch_one(engine, sql: str, **params):
    with engine.connect() as conexion:
        return conexion.execute(text(sql), params).fetchone()


def assert_tabla_existe(engine, esquema: str, tabla: str) -> None:
    cantidad = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :esquema
          AND TABLE_NAME = :tabla
        """,
        esquema=esquema,
        tabla=tabla,
    )
    assert int(cantidad or 0) == 1, f'No existe la tabla {esquema}.{tabla}'


def assert_columnas_existen(engine, esquema: str, tabla: str, columnas: list[str]) -> None:
    with engine.connect() as conexion:
        filas = conexion.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :esquema
                  AND TABLE_NAME = :tabla
                """
            ),
            {'esquema': esquema, 'tabla': tabla},
        ).fetchall()

    existentes = {str(fila[0]) for fila in filas}
    faltantes = [col for col in columnas if col not in existentes]
    assert not faltantes, f'Faltan columnas en {esquema}.{tabla}: {faltantes}'


def skip_si_fact_vacio(engine, tabla_fact: str) -> int:
    cantidad = int(scalar(engine, f"SELECT COUNT(*) FROM {tabla_fact}") or 0)
    if cantidad == 0:
        pytest.skip(f'{tabla_fact} no tiene filas en este momento')
    return cantidad

"""
tests/conftest.py
==================
Fixtures compartidas para las pruebas del backend ACP Platform.

Estrategia de aislamiento:
- Sobrescribe ACP_ENTORNO=test antes de importar la aplicación.
- Parchea verificar_conexion() para que no necesite SQL Server real.
- Parchea obtener_engine() para que no intente conectarse.
- Provee un TestClient de FastAPI listo para usar.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ── Garantiza que el directorio backend/ esté en sys.path ─────────────────────
_DIR_BACKEND = Path(__file__).resolve().parents[1]
if str(_DIR_BACKEND) not in sys.path:
    sys.path.insert(0, str(_DIR_BACKEND))

# ── Fuerza entorno = test ANTES de importar settings ──────────────────────────
os.environ.setdefault("ACP_ENTORNO", "test")
os.environ.setdefault("DB_SERVIDOR", "TEST_SERVER")
os.environ.setdefault("DB_NOMBRE", "TEST_DB")


# ── Mock de conexión SQL Server ────────────────────────────────────────────────
_INFO_BD_OK = {
    "conectado":   True,
    "base_datos":  "TEST_DB",
    "version":     "15.0.0.0",
    "latencia_ms": 1.0,
}

_INFO_BD_FALLO = {
    "conectado": False,
    "error":     "Sin conexión (mock de test)",
}


@pytest.fixture(scope="session")
def mock_conexion_ok():
    """Patch de verificar_conexion() que devuelve BD conectada."""
    with patch("nucleo.conexion.verificar_conexion", return_value=_INFO_BD_OK) as m:
        yield m


@pytest.fixture(scope="session")
def mock_conexion_fallo():
    """Patch de verificar_conexion() que devuelve BD no conectada."""
    with patch("nucleo.conexion.verificar_conexion", return_value=_INFO_BD_FALLO) as m:
        yield m


@pytest.fixture(scope="session")
def cliente(mock_conexion_ok):
    """
    TestClient de FastAPI con BD mockeada como conectada.
    Session-scoped para no recrear la app en cada test.
    """
    # Parchamos también obtener_engine para evitar intentos de conexión real
    mock_engine = MagicMock()

    with patch("nucleo.conexion.obtener_engine", return_value=mock_engine):
        from main import aplicacion
        with TestClient(aplicacion, raise_server_exceptions=True) as c:
            yield c


@pytest.fixture
def cliente_sin_bd(mock_conexion_fallo):
    """
    TestClient con BD simulada como NO conectada.
    Útil para probar degraded states.
    """
    mock_engine = MagicMock()
    with patch("nucleo.conexion.obtener_engine", return_value=mock_engine):
        from main import aplicacion
        with TestClient(aplicacion, raise_server_exceptions=False) as c:
            yield c

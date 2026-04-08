"""
tests/test_auditoria.py
========================
Pruebas de contrato para los endpoints de auditoría (v1):
    GET /api/v1/auditoria/log-carga
    GET /api/v1/auditoria/log-carga/{tabla_destino}

Todos requieren autenticación (viewer+).
"""

from datetime import datetime
from unittest.mock import patch

import pytest
from tests.conftest import auth_headers

_HISTORIAL_MOCK = [
    {
        "id_log":            1,
        "nombre_proceso":    "API_ETL_PIPELINE",
        "tabla_destino":     "PIPELINE_COMPLETO",
        "nombre_archivo":    "corrida_test",
        "fecha_inicio":      datetime(2026, 1, 15, 10, 0, 0),
        "fecha_fin":         datetime(2026, 1, 15, 10, 5, 0),
        "estado":            "OK",
        "filas_insertadas":  1000,
        "filas_rechazadas":  5,
        "duracion_segundos": 300,
        "mensaje_error":     None,
    }
]

_ULTIMO_ESTADO_MOCK = {
    "tabla_destino":     "Silver.Dim_Personal",
    "estado":            "OK",
    "fecha_inicio":      datetime(2026, 1, 15, 10, 0, 0),
    "fecha_fin":         datetime(2026, 1, 15, 10, 5, 0),
    "filas_insertadas":  500,
    "duracion_segundos": 120,
    "mensaje_error":     None,
}

_PATCH_HISTORIAL     = "api.rutas_auditoria.obtener_historial"
_PATCH_ULTIMO_ESTADO = "api.rutas_auditoria.obtener_ultimo_estado_tabla"

_URL = "/api/v1/auditoria/log-carga"

class TestLogCarga:
    def test_retorna_200_con_lista(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=_HISTORIAL_MOCK):
            resp = cliente.get(_URL, headers=auth_headers())
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(_URL)
        assert resp.status_code == 401

    def test_limite_default_es_50(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=[]) as mock_hist:
            cliente.get(_URL, headers=auth_headers())
            mock_hist.assert_called_once_with(limite=50)

    def test_limite_personalizado(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=[]) as mock_hist:
            cliente.get(f"{_URL}?limite=10", headers=auth_headers())
            mock_hist.assert_called_once_with(limite=10)

    def test_limite_minimo_1(self, cliente):
        resp = cliente.get(f"{_URL}?limite=0", headers=auth_headers())
        assert resp.status_code == 422

    def test_limite_maximo_500(self, cliente):
        resp = cliente.get(f"{_URL}?limite=501", headers=auth_headers())
        assert resp.status_code == 422

    def test_campos_del_historial(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=_HISTORIAL_MOCK):
            data = cliente.get(_URL, headers=auth_headers()).json()
        assert len(data) > 0
        entrada = data[0]
        for campo in ["id_log", "nombre_proceso", "tabla_destino", "estado", "filas_insertadas"]:
            assert campo in entrada


class TestUltimoEstadoTabla:
    def test_retorna_200_cuando_existe(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=_ULTIMO_ESTADO_MOCK):
            resp = cliente.get(f"{_URL}/Silver.Dim_Personal", headers=auth_headers())
        assert resp.status_code == 200

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(f"{_URL}/Silver.Dim_Personal")
        assert resp.status_code == 401

    def test_retorna_404_cuando_no_existe(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=None):
            resp = cliente.get(f"{_URL}/Tabla.Inexistente", headers=auth_headers())
        assert resp.status_code == 404

    def test_error_404_tiene_request_id(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=None):
            data = cliente.get(f"{_URL}/Tabla.Inexistente", headers=auth_headers()).json()
        assert "request_id" in data
        assert "timestamp" in data

    def test_campos_ultimo_estado(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=_ULTIMO_ESTADO_MOCK):
            data = cliente.get(f"{_URL}/Silver.Dim_Personal", headers=auth_headers()).json()
        for campo in ["tabla_destino", "estado", "fecha_inicio", "filas_insertadas"]:
            assert campo in data

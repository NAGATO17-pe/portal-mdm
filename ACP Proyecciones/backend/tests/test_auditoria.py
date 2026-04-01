"""
tests/test_auditoria.py
========================
Pruebas de contrato para los endpoints de auditoría:
    GET /api/auditoria/log-carga
    GET /api/auditoria/log-carga/{tabla_destino}

Nota sobre mocks: se parchea el símbolo en el módulo que lo importa
(api.rutas_auditoria), no en el módulo donde se define.
"""

from datetime import datetime
from unittest.mock import patch

import pytest


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


# Ruta exacta usada por el router para importar el servicio
_PATCH_HISTORIAL      = "api.rutas_auditoria.obtener_historial"
_PATCH_ULTIMO_ESTADO  = "api.rutas_auditoria.obtener_ultimo_estado_tabla"


class TestLogCarga:
    def test_retorna_200_con_lista(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=_HISTORIAL_MOCK):
            resp = cliente.get("/api/auditoria/log-carga")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_limite_default_es_50(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=[]) as mock_hist:
            cliente.get("/api/auditoria/log-carga")
            mock_hist.assert_called_once_with(limite=50)

    def test_limite_personalizado(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=[]) as mock_hist:
            cliente.get("/api/auditoria/log-carga?limite=10")
            mock_hist.assert_called_once_with(limite=10)

    def test_limite_minimo_1(self, cliente):
        resp = cliente.get("/api/auditoria/log-carga?limite=0")
        assert resp.status_code == 422

    def test_limite_maximo_500(self, cliente):
        resp = cliente.get("/api/auditoria/log-carga?limite=501")
        assert resp.status_code == 422

    def test_campos_del_historial(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=_HISTORIAL_MOCK):
            data = cliente.get("/api/auditoria/log-carga").json()
        assert len(data) > 0, "Se esperaba al menos un elemento en el historial"
        entrada = data[0]
        campos_requeridos = [
            "id_log", "nombre_proceso", "tabla_destino", "nombre_archivo",
            "fecha_inicio", "estado", "filas_insertadas",
        ]
        for campo in campos_requeridos:
            assert campo in entrada, f"Falta campo '{campo}' en respuesta de log-carga"


class TestUltimoEstadoTabla:
    def test_retorna_200_cuando_existe(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=_ULTIMO_ESTADO_MOCK):
            resp = cliente.get("/api/auditoria/log-carga/Silver.Dim_Personal")
        assert resp.status_code == 200

    def test_retorna_404_cuando_no_existe(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=None):
            resp = cliente.get("/api/auditoria/log-carga/Tabla.Inexistente")
        assert resp.status_code == 404

    def test_error_404_tiene_request_id(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=None):
            data = cliente.get("/api/auditoria/log-carga/Tabla.Inexistente").json()
        assert "request_id" in data
        assert "timestamp" in data

    def test_campos_ultimo_estado(self, cliente):
        with patch(_PATCH_ULTIMO_ESTADO, return_value=_ULTIMO_ESTADO_MOCK):
            data = cliente.get("/api/auditoria/log-carga/Silver.Dim_Personal").json()
        for campo in ["tabla_destino", "estado", "fecha_inicio", "filas_insertadas"]:
            assert campo in data, f"Falta campo '{campo}' en respuesta de último estado"

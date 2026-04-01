"""
tests/test_health.py
=====================
Pruebas de contrato para los endpoints de salud:
    GET /health/live
    GET /health/ready
    GET /health
"""

from unittest.mock import patch

import pytest


# ── /health/live ──────────────────────────────────────────────────────────────

class TestLiveness:
    def test_retorna_200(self, cliente):
        resp = cliente.get("/health/live")
        assert resp.status_code == 200

    def test_cuerpo_tiene_campos_requeridos(self, cliente):
        data = cliente.get("/health/live").json()
        assert data["estado"] == "vivo"
        assert "servicio" in data
        assert "version" in data
        assert "entorno" in data
        assert "timestamp" in data

    def test_no_contacta_bd(self, cliente):
        """Liveness no debe hablar con la BD — verificar_conexion NO debe llamarse."""
        with patch("api.rutas_health.verificar_conexion") as mock_vc:
            cliente.get("/health/live")
            mock_vc.assert_not_called()

    def test_tiene_header_request_id(self, cliente):
        resp = cliente.get("/health/live")
        assert "x-request-id" in resp.headers


# ── /health/ready ─────────────────────────────────────────────────────────────

class TestReadiness:
    _INFO_OK = {
        "conectado":  True,
        "base_datos": "TEST_DB",
        "latencia_ms": 2.5,
    }
    _INFO_FALLO = {
        "conectado": False,
        "error":     "timeout",
    }

    def test_200_cuando_bd_conectada(self, cliente):
        with patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK):
            resp = cliente.get("/health/ready")
        assert resp.status_code == 200
        assert resp.json()["estado"] == "listo"

    def test_503_cuando_bd_falla(self, cliente):
        with patch("api.rutas_health.verificar_conexion", return_value=self._INFO_FALLO):
            resp = cliente.get("/health/ready")
        assert resp.status_code == 503
        assert resp.json()["estado"] == "no_listo"

    def test_incluye_info_bd(self, cliente):
        with patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK):
            data = cliente.get("/health/ready").json()
        assert "base_datos" in data
        assert data["base_datos"]["conectado"] is True


# ── /health ───────────────────────────────────────────────────────────────────

class TestHealthCompleto:
    _INFO_OK = {
        "conectado":  True,
        "base_datos": "TEST_DB",
        "latencia_ms": 3.0,
        "version":    "15.0.0",
    }
    _INFO_FALLO = {"conectado": False, "error": "sin acceso"}

    def test_estado_activo_con_bd(self, cliente):
        with patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK):
            data = cliente.get("/health").json()
        assert data["estado"] == "activo"

    def test_estado_degradado_sin_bd(self, cliente):
        with patch("api.rutas_health.verificar_conexion", return_value=self._INFO_FALLO):
            data = cliente.get("/health").json()
        assert data["estado"] == "degradado"

    def test_campos_completos(self, cliente):
        with patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK):
            data = cliente.get("/health").json()
        for campo in ["servicio", "version", "entorno", "estado", "base_datos", "timestamp"]:
            assert campo in data, f"Falta el campo '{campo}' en /health"

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


class TestReadyControl:
    _INFO_OK = {
        "conectado": True,
        "base_datos": "TEST_DB",
        "latencia_ms": 2.0,
    }
    _CONTROL_OK = {
        "estado": "operativo",
        "resumen": {
            "corridas_activas": 1,
            "comandos_pendientes": 2,
            "comandos_procesando": 0,
        },
        "lock": {
            "estado_lock": "LIBRE",
            "segundos_desde_heartbeat": None,
        },
    }

    def test_200_cuando_control_plane_responde(self, cliente):
        with (
            patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK),
            patch("api.rutas_health._diagnostico_control_plane", return_value=(True, self._CONTROL_OK)),
        ):
            resp = cliente.get("/health/ready/control")
        assert resp.status_code == 200
        assert resp.json()["estado"] == "listo"

    def test_503_cuando_control_plane_falla(self, cliente):
        with (
            patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK),
            patch("api.rutas_health._diagnostico_control_plane", return_value=(False, {"estado": "error"})),
        ):
            resp = cliente.get("/health/ready/control")
        assert resp.status_code == 503
        assert resp.json()["estado"] == "no_listo"


class TestReadyRunner:
    _INFO_OK = {
        "conectado": True,
        "base_datos": "TEST_DB",
        "latencia_ms": 2.0,
    }

    def test_200_si_runner_esta_libre(self, cliente):
        control_plane = {
            "estado": "operativo",
            "resumen": {"corridas_activas": 0, "comandos_pendientes": 0, "comandos_procesando": 0},
            "lock": {"estado_lock": "LIBRE", "segundos_desde_heartbeat": None},
        }
        with (
            patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK),
            patch("api.rutas_health._diagnostico_control_plane", return_value=(True, control_plane)),
        ):
            resp = cliente.get("/health/ready/runner")
        assert resp.status_code == 200
        assert resp.json()["estado"] == "libre"

    def test_503_si_lock_vence(self, cliente):
        control_plane = {
            "estado": "operativo",
            "resumen": {"corridas_activas": 1, "comandos_pendientes": 0, "comandos_procesando": 1},
            "lock": {"estado_lock": "VENCIDO", "segundos_desde_heartbeat": 999},
        }
        with (
            patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK),
            patch("api.rutas_health._diagnostico_control_plane", return_value=(True, control_plane)),
        ):
            resp = cliente.get("/health/ready/runner")
        assert resp.status_code == 503
        assert resp.json()["estado"] == "lock_vencido"


class TestLockStatus:
    _INFO_OK = {
        "conectado": True,
        "base_datos": "TEST_DB",
        "latencia_ms": 2.0,
    }

    def test_retorna_lock_actual(self, cliente):
        lock = {
            "id_lock": 1,
            "estado_lock": "ACTIVO",
            "id_corrida_activa": "abc",
            "segundos_desde_heartbeat": 12,
        }
        with (
            patch("api.rutas_health.verificar_conexion", return_value=self._INFO_OK),
            patch("api.rutas_health.obtener_estado_lock", return_value=lock),
        ):
            resp = cliente.get("/health/lock")
        assert resp.status_code == 200
        assert resp.json()["estado"] == "activo"
        assert resp.json()["lock"]["id_corrida_activa"] == "abc"

    def test_503_si_bd_no_esta_lista(self, cliente):
        with patch("api.rutas_health.verificar_conexion", return_value={"conectado": False, "error": "timeout"}):
            resp = cliente.get("/health/lock")
        assert resp.status_code == 503


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

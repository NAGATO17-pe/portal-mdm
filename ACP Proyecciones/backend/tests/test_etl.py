"""
tests/test_etl.py
==================
Pruebas de contrato para los endpoints ETL v3 (modelo controlado persistente):
    POST   /api/v1/etl/corridas
    GET    /api/v1/etl/corridas
    GET    /api/v1/etl/corridas/activas
    GET    /api/v1/etl/corridas/{id}
    GET    /api/v1/etl/corridas/{id}/eventos
    DELETE /api/v1/etl/corridas/{id}

Todos los endpoints requieren autenticación.
El runner externo se mockea — no se verifica ejecución real.
"""

from unittest.mock import AsyncMock, patch
from datetime import datetime

import pytest
from tests.conftest import auth_headers

_CORRIDA_MOCK = {
    "id_corrida":   "test-uuid-1234",
    "id_log":       None,   # El runner lo rellena
    "iniciado_por": "operador01",
    "fecha_inicio": datetime(2026, 1, 15, 10, 0, 0),
    "estado":       "PENDIENTE",
}

_CORRIDA_BD_MOCK = {
    "id_corrida":       "test-uuid-1234",
    "iniciado_por":     "operador01",
    "comentario":       None,
    "estado":           "EJECUTANDO",
    "intento_numero":   1,
    "max_reintentos":   0,
    "fecha_solicitud":  datetime(2026, 1, 15, 10, 0, 0),
    "fecha_inicio":     datetime(2026, 1, 15, 10, 0, 5),
    "fecha_fin":        None,
    "pid_runner":       1234,
    "heartbeat_ultimo": datetime(2026, 1, 15, 10, 1, 0),
    "timeout_segundos": 3600,
    "mensaje_final":    None,
    "id_log_auditoria": 42,
    "modo_ejecucion": "facts",
    "facts": ["Fact_Telemetria_Clima"],
    "incluir_dependencias": True,
    "refrescar_gold": True,
    "forzar_relectura_bronce": True,
    "pasos": [
        {
            "id_paso": 1,
            "id_corrida": "test-uuid-1234",
            "nombre_paso": "Reprocesando Fact_Telemetria_Clima",
            "orden": 3,
            "estado": "OK",
            "fecha_inicio": datetime(2026, 1, 15, 10, 0, 10),
            "fecha_fin": datetime(2026, 1, 15, 10, 0, 40),
            "mensaje_error": None,
        }
    ],
}

_PASOS_MOCK = _CORRIDA_BD_MOCK["pasos"]

_FACTS_MOCK = [
    {
        "nombre_fact": "Fact_Telemetria_Clima",
        "orden": 12,
        "tabla_destino": "Silver.Fact_Telemetria_Clima",
        "fuentes_bronce": ["Bronce.Reporte_Clima", "Bronce.Variables_Meteorologicas"],
        "dependencias": [],
        "marts": ["Gold.Mart_Clima"],
        "releer_bronce_por_estado": True,
        "estrategia_rerun": "rebuild_fact",
    }
]

_HISTORIAL_MOCK = [
    {
        "id_log":            1,
        "nombre_proceso":    "ETL_RUNNER",
        "tabla_destino":     "PIPELINE_COMPLETO",
        "nombre_archivo":    "corrida_test-uuid",
        "fecha_inicio":      datetime(2026, 1, 15, 10, 0, 0),
        "fecha_fin":         datetime(2026, 1, 15, 10, 5, 0),
        "estado":            "OK",
        "filas_insertadas":  1000,
        "filas_rechazadas":  5,
        "duracion_segundos": 300,
        "mensaje_error":     None,
    }
]

_PATCH_INICIAR   = "api.rutas_etl.iniciar_corrida"
_PATCH_OBTENER   = "api.rutas_etl.obtener_corrida"
_PATCH_EXISTE    = "api.rutas_etl.corrida_existe"
_PATCH_HISTORIAL = "api.rutas_etl.obtener_historial"
_PATCH_ACTIVAS   = "api.rutas_etl.listar_corridas_activas"
_PATCH_PASOS     = "api.rutas_etl.obtener_pasos_corrida"
_PATCH_FACTS     = "api.rutas_etl.listar_catalogo_facts"
_PATCH_CANCELAR  = "api.rutas_etl.cancelar_corrida"
_PATCH_AUDITAR   = "api.rutas_etl.registrar_accion"

_URL = "/api/v1/etl/corridas"


# ─────────────────────────────────────────────────────────────────────────────
# POST /corridas — Encolar corrida
# ─────────────────────────────────────────────────────────────────────────────

class TestIniciarCorrida:
    def test_encola_y_retorna_200(self, cliente):
        with (
            patch(_PATCH_INICIAR, new_callable=AsyncMock, return_value=_CORRIDA_MOCK),
            patch(_PATCH_AUDITAR, return_value=None),
        ):
            resp = cliente.post(_URL, json={}, headers=auth_headers("operador_etl"))
        assert resp.status_code == 200

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.post(_URL, json={})
        assert resp.status_code == 401

    def test_viewer_no_puede_lanzar_retorna_403(self, cliente):
        resp = cliente.post(_URL, json={}, headers=auth_headers("viewer"))
        assert resp.status_code == 403

    def test_respuesta_tiene_estado_pendiente(self, cliente):
        with (
            patch(_PATCH_INICIAR, new_callable=AsyncMock, return_value=_CORRIDA_MOCK),
            patch(_PATCH_AUDITAR, return_value=None),
        ):
            data = cliente.post(_URL, json={}, headers=auth_headers("operador_etl")).json()
        assert data["estado"] == "PENDIENTE"
        assert data["id_log"] is None   # Runner no ha arrancado aún

    def test_respuesta_contiene_url_stream(self, cliente):
        with (
            patch(_PATCH_INICIAR, new_callable=AsyncMock, return_value=_CORRIDA_MOCK),
            patch(_PATCH_AUDITAR, return_value=None),
        ):
            data = cliente.post(_URL, json={}, headers=auth_headers("operador_etl")).json()
        assert "url_stream" in data
        assert data["id_corrida"] in data["url_stream"]

    def test_iniciado_por_viene_del_jwt(self, cliente):
        mock = {**_CORRIDA_MOCK, "iniciado_por": "testuser"}
        with (
            patch(_PATCH_INICIAR, new_callable=AsyncMock, return_value=mock),
            patch(_PATCH_AUDITAR, return_value=None),
        ):
            data = cliente.post(
                _URL,
                json={"iniciado_por": "INYECCION_MALICIOSA"},
                headers=auth_headers("operador_etl"),
            ).json()
        assert data["iniciado_por"] == "testuser"

    def test_modo_facts_sin_facts_retorna_422(self, cliente):
        resp = cliente.post(
            _URL,
            json={"modo_ejecucion": "facts"},
            headers=auth_headers("operador_etl"),
        )
        assert resp.status_code == 422

    def test_payload_parcial_se_reenvia_al_servicio(self, cliente):
        cuerpo = {
            "comentario": "reproceso clima",
            "modo_ejecucion": "facts",
            "facts": ["Fact_Telemetria_Clima"],
            "incluir_dependencias": False,
            "refrescar_gold": False,
            "forzar_relectura_bronce": True,
        }
        with (
            patch(_PATCH_INICIAR, new_callable=AsyncMock, return_value=_CORRIDA_MOCK) as mock_iniciar,
            patch(_PATCH_AUDITAR, return_value=None),
        ):
            resp = cliente.post(_URL, json=cuerpo, headers=auth_headers("operador_etl"))
        assert resp.status_code == 200
        mock_iniciar.assert_awaited_once_with(
            iniciado_por="testuser",
            comentario="reproceso clima",
            modo_ejecucion="facts",
            facts=["Fact_Telemetria_Clima"],
            incluir_dependencias=False,
            refrescar_gold=False,
            forzar_relectura_bronce=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# GET /corridas — Historial
# ─────────────────────────────────────────────────────────────────────────────

class TestHistorialCorridas:
    def test_retorna_lista(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=_HISTORIAL_MOCK):
            resp = cliente.get(_URL, headers=auth_headers("viewer"))
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(_URL)
        assert resp.status_code == 401

    def test_limite_default_es_50(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=[]) as m:
            cliente.get(_URL, headers=auth_headers("viewer"))
            m.assert_called_once_with(limite=50)


# ─────────────────────────────────────────────────────────────────────────────
# GET /corridas/activas
# ─────────────────────────────────────────────────────────────────────────────

class TestCornidasActivas:
    def test_retorna_lista_activas(self, cliente):
        with patch(_PATCH_ACTIVAS, return_value=[_CORRIDA_BD_MOCK]):
            resp = cliente.get(f"{_URL}/activas", headers=auth_headers("viewer"))
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(f"{_URL}/activas")
        assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# GET /corridas/{id} — Estado individual
# ─────────────────────────────────────────────────────────────────────────────

class TestEstadoCorrida:
    def test_retorna_estado(self, cliente):
        with patch(_PATCH_OBTENER, return_value=_CORRIDA_BD_MOCK):
            resp = cliente.get(f"{_URL}/test-uuid-1234", headers=auth_headers("viewer"))
        assert resp.status_code == 200
        data = resp.json()
        assert data["estado"] == "EJECUTANDO"
        assert data["pid_runner"] == 1234
        assert data["modo_ejecucion"] == "facts"
        assert data["pasos"][0]["nombre_paso"] == "Reprocesando Fact_Telemetria_Clima"

    def test_no_existente_retorna_404(self, cliente):
        with patch(_PATCH_OBTENER, return_value=None):
            resp = cliente.get(f"{_URL}/no-existe", headers=auth_headers("viewer"))
        assert resp.status_code == 404

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(f"{_URL}/test-uuid-1234")
        assert resp.status_code == 401


class TestPasosCorrida:
    def test_retorna_traza_de_pasos(self, cliente):
        with (
            patch(_PATCH_EXISTE, return_value=True),
            patch(_PATCH_PASOS, return_value=_PASOS_MOCK),
        ):
            resp = cliente.get(f"{_URL}/test-uuid-1234/pasos", headers=auth_headers("viewer"))
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["estado"] == "OK"
        assert data[0]["orden"] == 3

    def test_retorna_404_si_corrida_no_existe(self, cliente):
        with patch(_PATCH_EXISTE, return_value=False):
            resp = cliente.get(f"{_URL}/no-existe/pasos", headers=auth_headers("viewer"))
        assert resp.status_code == 404


class TestCatalogoFacts:
    def test_retorna_catalogo_facts(self, cliente):
        with patch(_PATCH_FACTS, return_value=_FACTS_MOCK):
            resp = cliente.get("/api/v1/etl/facts", headers=auth_headers("viewer"))
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["nombre_fact"] == "Fact_Telemetria_Clima"
        assert data[0]["estrategia_rerun"] == "rebuild_fact"


# ─────────────────────────────────────────────────────────────────────────────
# GET /corridas/{id}/eventos — SSE
# ─────────────────────────────────────────────────────────────────────────────

class TestStreamEventos:
    def test_corrida_inexistente_retorna_404(self, cliente):
        with patch(_PATCH_EXISTE, return_value=False):
            resp = cliente.get(
                f"{_URL}/uuid-inexistente/eventos",
                headers=auth_headers("viewer"),
            )
        assert resp.status_code == 404

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(f"{_URL}/uuid-test/eventos")
        assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /corridas/{id} — Cancelar
# ─────────────────────────────────────────────────────────────────────────────

class TestCancelarCorrida:
    def test_cancelar_corrida_activa(self, cliente):
        with (
            patch(_PATCH_OBTENER,  return_value=_CORRIDA_BD_MOCK),
            patch(_PATCH_CANCELAR, new_callable=AsyncMock, return_value=True),
            patch(_PATCH_AUDITAR,  return_value=None),
        ):
            resp = cliente.delete(
                f"{_URL}/test-uuid-1234",
                headers=auth_headers("operador_etl"),
            )
        assert resp.status_code == 200
        assert resp.json()["cancelado"] is True

    def test_cancelar_corrida_terminada_retorna_false(self, cliente):
        corrida_ok = {**_CORRIDA_BD_MOCK, "estado": "OK"}
        with (
            patch(_PATCH_OBTENER,  return_value=corrida_ok),
            patch(_PATCH_CANCELAR, new_callable=AsyncMock, return_value=False),
            patch(_PATCH_AUDITAR,  return_value=None),
        ):
            resp = cliente.delete(
                f"{_URL}/test-uuid-1234",
                headers=auth_headers("operador_etl"),
            )
        assert resp.status_code == 200
        assert resp.json()["cancelado"] is False

    def test_cancelar_inexistente_retorna_404(self, cliente):
        with patch(_PATCH_OBTENER, return_value=None):
            resp = cliente.delete(
                f"{_URL}/no-existe",
                headers=auth_headers("operador_etl"),
            )
        assert resp.status_code == 404

    def test_viewer_no_puede_cancelar(self, cliente):
        resp = cliente.delete(
            f"{_URL}/test-uuid-1234",
                headers=auth_headers("viewer"),
        )
        assert resp.status_code == 403

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.delete(f"{_URL}/test-uuid-1234")
        assert resp.status_code == 401

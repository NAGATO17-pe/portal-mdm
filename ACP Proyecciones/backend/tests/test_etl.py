"""
tests/test_etl.py
==================
Pruebas de contrato para los endpoints ETL:
    POST /api/etl/corridas
    GET  /api/etl/corridas/{id_corrida}/eventos
    GET  /api/etl/corridas

Nota sobre mocks: se parchea el símbolo en el módulo router que lo importa.
"""

from unittest.mock import AsyncMock, patch
from datetime import datetime

import pytest


_CORRIDA_MOCK = {
    "id_corrida":   "test-corrida-uuid",
    "id_log":       42,
    "iniciado_por": "analista_test",
    "fecha_inicio": datetime(2026, 1, 15, 10, 0, 0),
}

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

# Rutas de mock: se parchea en el módulo router, no donde se define
_PATCH_INICIAR   = "api.rutas_etl.iniciar_corrida"
_PATCH_EXISTE    = "api.rutas_etl.corrida_existe"
_PATCH_HISTORIAL = "api.rutas_etl.obtener_historial"


class TestIniciarCorrida:
    def test_post_corrida_retorna_200(self, cliente):
        with patch(_PATCH_INICIAR, new_callable=AsyncMock) as mock_ic:
            mock_ic.return_value = _CORRIDA_MOCK
            resp = cliente.post(
                "/api/etl/corridas",
                json={"iniciado_por": "analista_test", "comentario": "Test de carga"},
            )
        assert resp.status_code == 200

    def test_respuesta_contiene_id_corrida(self, cliente):
        with patch(_PATCH_INICIAR, new_callable=AsyncMock) as mock_ic:
            mock_ic.return_value = _CORRIDA_MOCK
            data = cliente.post(
                "/api/etl/corridas",
                json={"iniciado_por": "analista_test"},
            ).json()
        assert "id_corrida" in data
        assert "url_stream" in data
        assert data["iniciado_por"] == "analista_test"

    def test_falta_iniciado_por_usa_default(self, cliente):
        """iniciado_por tiene default='backend_api' — JSON vacío es válido."""
        with patch(_PATCH_INICIAR, new_callable=AsyncMock) as mock_ic:
            mock_ic.return_value = _CORRIDA_MOCK
            resp = cliente.post("/api/etl/corridas", json={})
        # No debe retornar 422 porque iniciado_por tiene default
        assert resp.status_code == 200


class TestStreamEventos:
    def test_corrida_inexistente_retorna_404(self, cliente):
        with patch(_PATCH_EXISTE, return_value=False):
            resp = cliente.get("/api/etl/corridas/uuid-inexistente/eventos")
        assert resp.status_code == 404

    def test_error_incluye_mensaje_descriptivo(self, cliente):
        with patch(_PATCH_EXISTE, return_value=False):
            data = cliente.get("/api/etl/corridas/uuid-inexistente/eventos").json()
        assert "no encontrada" in data["mensaje"].lower() or "uuid-inexistente" in data["mensaje"]


class TestHistorialCorridas:
    def test_get_historial_retorna_lista(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=_HISTORIAL_MOCK):
            resp = cliente.get("/api/etl/corridas")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_limite_por_defecto_es_50(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=[]) as mock_hist:
            cliente.get("/api/etl/corridas")
            mock_hist.assert_called_once_with(limite=50)

    def test_limite_personalizado(self, cliente):
        with patch(_PATCH_HISTORIAL, return_value=[]) as mock_hist:
            cliente.get("/api/etl/corridas?limite=10")
            mock_hist.assert_called_once_with(limite=10)

"""
tests/test_cuarentena.py
=========================
Pruebas de contrato para los endpoints de cuarentena:
    GET   /api/cuarentena
    PATCH /api/cuarentena/{tabla}/{id}/resolver
    PATCH /api/cuarentena/{tabla}/{id}/rechazar
"""

from unittest.mock import patch

import pytest


_PAGINA_MOCK = {
    "total":  100,
    "pagina": 1,
    "tamano": 20,
    "datos": [
        {
            "id_registro":       "42",
            "tabla_origen":      "Bronce.Cosecha",
            "columna_origen":    "Variedad",
            "valor_raw":         "Camarosa X",
            "nombre_archivo":    None,
            "fecha_ingreso":     "2026-01-15 10:00:00",
            "estado":            "PENDIENTE",
            "motivo":            "Valor no en catálogo",
            "id_registro_origen": "7",
        }
    ],
}

_ACCION_OK = {
    "id_registro":  "42",
    "estado_nuevo": "RESUELTO",
    "mensaje":      "Registro resuelto con valor corregido 'Camarosa'.",
}

_ACCION_RECHAZADO = {
    "id_registro":  "42",
    "estado_nuevo": "DESCARTADO",
    "mensaje":      "Registro descartado. Motivo: Valor inválido.",
}


class TestListarCuarentena:
    def test_retorna_200(self, cliente):
        with patch("servicios.servicio_cuarentena.listar_cuarentena", return_value=_PAGINA_MOCK):
            resp = cliente.get("/api/cuarentena")
        assert resp.status_code == 200

    def test_estructura_paginada(self, cliente):
        with patch("servicios.servicio_cuarentena.listar_cuarentena", return_value=_PAGINA_MOCK):
            data = cliente.get("/api/cuarentena").json()
        assert "total" in data
        assert "pagina" in data
        assert "tamano" in data
        assert "datos" in data
        assert isinstance(data["datos"], list)

    def test_paginacion_parametros_default(self, cliente):
        with patch("servicios.servicio_cuarentena.listar_cuarentena") as mock_lc:
            mock_lc.return_value = _PAGINA_MOCK
            cliente.get("/api/cuarentena")
            mock_lc.assert_called_once_with(pagina=1, tamano=20, tabla_filtro=None)

    def test_filtro_por_tabla(self, cliente):
        with patch("servicios.servicio_cuarentena.listar_cuarentena") as mock_lc:
            mock_lc.return_value = _PAGINA_MOCK
            cliente.get("/api/cuarentena?tabla_filtro=Bronce.Cosecha")
            args = mock_lc.call_args
            assert args.kwargs["tabla_filtro"] == "Bronce.Cosecha"

    def test_tamano_fuera_de_rango_retorna_422(self, cliente):
        resp = cliente.get("/api/cuarentena?tamano=0")
        assert resp.status_code == 422

        resp = cliente.get("/api/cuarentena?tamano=101")
        assert resp.status_code == 422


class TestResolverCuarentena:
    def test_resolver_retorna_200(self, cliente):
        with patch("servicios.servicio_cuarentena.resolver_registro", return_value=_ACCION_OK):
            resp = cliente.patch(
                "/api/cuarentena/Bronce.Cosecha/42/resolver",
                json={
                    "valor_canonico": "Camarosa",
                    "analista": "12345678",
                    "comentario": "Variedad corregida",
                },
            )
        assert resp.status_code == 200

    def test_resolver_respuesta_tiene_estado_nuevo(self, cliente):
        with patch("servicios.servicio_cuarentena.resolver_registro", return_value=_ACCION_OK):
            data = cliente.patch(
                "/api/cuarentena/Bronce.Cosecha/42/resolver",
                json={"valor_canonico": "Camarosa", "analista": "12345678"},
            ).json()
        assert data["estado_nuevo"] == "RESUELTO"
        assert data["id_registro"] == "42"

    def test_resolver_sin_analista_retorna_422(self, cliente):
        resp = cliente.patch(
            "/api/cuarentena/Bronce.Cosecha/42/resolver",
            json={"valor_canonico": "Camarosa"},
        )
        assert resp.status_code == 422


class TestRechazarCuarentena:
    def test_rechazar_retorna_200(self, cliente):
        with patch("servicios.servicio_cuarentena.rechazar_registro", return_value=_ACCION_RECHAZADO):
            resp = cliente.patch(
                "/api/cuarentena/Bronce.Cosecha/42/rechazar",
                json={"motivo": "Valor inválido.", "analista": "12345678"},
            )
        assert resp.status_code == 200

    def test_rechazar_sin_motivo_retorna_422(self, cliente):
        resp = cliente.patch(
            "/api/cuarentena/Bronce.Cosecha/42/rechazar",
            json={"analista": "12345678"},
        )
        assert resp.status_code == 422

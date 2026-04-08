"""
tests/test_cuarentena.py
=========================
Pruebas de contrato para los endpoints de cuarentena (v1):
    GET   /api/v1/cuarentena
    PATCH /api/v1/cuarentena/{tabla}/{id}/resolver
    PATCH /api/v1/cuarentena/{tabla}/{id}/rechazar

Todos los endpoints requieren autenticación.
"""

from unittest.mock import patch

import pytest
from tests.conftest import auth_headers

_PAGINA_MOCK = {
    "total":  100,
    "pagina": 1,
    "tamano": 20,
    "datos": [
        {
            "id_registro":        "42",
            "tabla_origen":       "Bronce.Cosecha",
            "columna_origen":     "Variedad",
            "valor_raw":          "Camarosa X",
            "nombre_archivo":     None,
            "fecha_ingreso":      "2026-01-15 10:00:00",
            "estado":             "PENDIENTE",
            "motivo":             "Valor no en catálogo",
            "id_registro_origen": 7,
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
    "mensaje":      "Registro descartado.",
}

_PATCH_LISTAR   = "api.rutas_cuarentena.listar_cuarentena"
_PATCH_RESOLVER = "api.rutas_cuarentena.resolver_registro"
_PATCH_RECHAZAR = "api.rutas_cuarentena.rechazar_registro"
_PATCH_AUDITAR  = "api.rutas_cuarentena.registrar_accion"

_URL = "/api/v1/cuarentena"

class TestListarCuarentena:
    def test_retorna_200(self, cliente):
        with patch(_PATCH_LISTAR, return_value=_PAGINA_MOCK):
            resp = cliente.get(_URL, headers=auth_headers("viewer"))
        assert resp.status_code == 200

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(_URL)
        assert resp.status_code == 401

    def test_estructura_paginada(self, cliente):
        with patch(_PATCH_LISTAR, return_value=_PAGINA_MOCK):
            data = cliente.get(_URL, headers=auth_headers("viewer")).json()
        for campo in ["total", "pagina", "tamano", "datos"]:
            assert campo in data
        assert isinstance(data["datos"], list)

    def test_paginacion_parametros_default(self, cliente):
        with patch(_PATCH_LISTAR, return_value=_PAGINA_MOCK) as mock_lc:
            cliente.get(_URL, headers=auth_headers("viewer"))
            mock_lc.assert_called_once_with(pagina=1, tamano=20, tabla_filtro=None)

    def test_filtro_por_tabla(self, cliente):
        with patch(_PATCH_LISTAR, return_value=_PAGINA_MOCK) as mock_lc:
            cliente.get(f"{_URL}?tabla_filtro=Bronce.Cosecha", headers=auth_headers("viewer"))
            assert mock_lc.call_args.kwargs["tabla_filtro"] == "Bronce.Cosecha"

    def test_tamano_fuera_de_rango_retorna_422(self, cliente):
        assert cliente.get(f"{_URL}?tamano=0",   headers=auth_headers("viewer")).status_code == 422
        assert cliente.get(f"{_URL}?tamano=101", headers=auth_headers("viewer")).status_code == 422


class TestResolverCuarentena:
    def test_retorna_200(self, cliente):
        with (
            patch(_PATCH_RESOLVER, return_value=_ACCION_OK),
            patch(_PATCH_AUDITAR,  return_value=None),
        ):
            resp = cliente.patch(
                f"{_URL}/Bronce.Cosecha/42/resolver",
                json={"valor_canonico": "Camarosa"},
                headers=auth_headers("analista_mdm"),
            )
        assert resp.status_code == 200

    def test_viewer_no_puede_resolver_retorna_403(self, cliente):
        resp = cliente.patch(
            f"{_URL}/Bronce.Cosecha/42/resolver",
            json={"valor_canonico": "Camarosa"},
            headers=auth_headers("viewer"),
        )
        assert resp.status_code == 403

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.patch(
            f"{_URL}/Bronce.Cosecha/42/resolver",
            json={"valor_canonico": "Camarosa"},
        )
        assert resp.status_code == 401

    def test_respuesta_tiene_estado_nuevo(self, cliente):
        with (
            patch(_PATCH_RESOLVER, return_value=_ACCION_OK),
            patch(_PATCH_AUDITAR,  return_value=None),
        ):
            data = cliente.patch(
                f"{_URL}/Bronce.Cosecha/42/resolver",
                json={"valor_canonico": "Camarosa"},
                headers=auth_headers("analista_mdm"),
            ).json()
        assert data["estado_nuevo"] == "RESUELTO"
        assert data["id_registro"] == "42"

    def test_sin_valor_canonico_retorna_422(self, cliente):
        resp = cliente.patch(
            f"{_URL}/Bronce.Cosecha/42/resolver",
            json={},
            headers=auth_headers("analista_mdm"),
        )
        assert resp.status_code == 422


class TestRechazarCuarentena:
    def test_retorna_200(self, cliente):
        with (
            patch(_PATCH_RECHAZAR, return_value=_ACCION_RECHAZADO),
            patch(_PATCH_AUDITAR,  return_value=None),
        ):
            resp = cliente.patch(
                f"{_URL}/Bronce.Cosecha/42/rechazar",
                json={"motivo": "Valor inválido."},
                headers=auth_headers("analista_mdm"),
            )
        assert resp.status_code == 200

    def test_viewer_no_puede_rechazar_retorna_403(self, cliente):
        resp = cliente.patch(
            f"{_URL}/Bronce.Cosecha/42/rechazar",
            json={"motivo": "x"},
            headers=auth_headers("viewer"),
        )
        assert resp.status_code == 403

    def test_sin_motivo_retorna_422(self, cliente):
        resp = cliente.patch(
            f"{_URL}/Bronce.Cosecha/42/rechazar",
            json={},
            headers=auth_headers("analista_mdm"),
        )
        assert resp.status_code == 422

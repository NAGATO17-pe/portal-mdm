from __future__ import annotations

from unittest.mock import patch

from tests.conftest import auth_headers

_URL = "/api/v1/catalogos"

_RESPUESTA_VARIEDADES = {
    "total": 1,
    "pagina": 1,
    "tamano": 20,
    "datos": [{"nombre_canonico": "Atlas", "breeder": "ACP", "es_activa": True}],
}

_RESPUESTA_GEOGRAFIA = {
    "total": 1,
    "pagina": 1,
    "tamano": 20,
    "datos": [
        {
            "fundo": "F1",
            "sector": "S1",
            "modulo": 9,
            "turno": 2,
            "valvula": "A1",
            "cama": "10",
            "es_test_block": False,
            "codigo_sap_campo": "SAP1",
            "es_vigente": True,
        }
    ],
}

_RESPUESTA_PERSONAL = {
    "total": 1,
    "pagina": 1,
    "tamano": 20,
    "datos": [
        {
            "dni": "12345678",
            "nombre_completo": "Test User",
            "rol": "Evaluador",
            "sexo": "M",
            "id_planilla": "P1",
            "pct_asertividad": 95.0,
            "dias_ausentismo": 1,
        }
    ],
}


def test_catalogo_variedades_conserva_envelope(cliente):
    with patch("api.rutas_catalogos.listar_variedades", return_value=_RESPUESTA_VARIEDADES):
        resp = cliente.get(f"{_URL}/variedades", headers=auth_headers("viewer"))
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["pagina"] == 1
    assert data["tamano"] == 20
    assert data["datos"][0]["nombre_canonico"] == "Atlas"


def test_catalogo_geografia_conserva_campos(cliente):
    with patch("api.rutas_catalogos.listar_geografia", return_value=_RESPUESTA_GEOGRAFIA):
        resp = cliente.get(f"{_URL}/geografia", headers=auth_headers("viewer"))
    assert resp.status_code == 200
    data = resp.json()
    assert data["datos"][0]["fundo"] == "F1"
    assert data["datos"][0]["es_vigente"] is True


def test_catalogo_personal_conserva_campos(cliente):
    with patch("api.rutas_catalogos.listar_personal", return_value=_RESPUESTA_PERSONAL):
        resp = cliente.get(f"{_URL}/personal", headers=auth_headers("viewer"))
    assert resp.status_code == 200
    data = resp.json()
    assert data["datos"][0]["dni"] == "12345678"
    assert data["datos"][0]["pct_asertividad"] == 95.0

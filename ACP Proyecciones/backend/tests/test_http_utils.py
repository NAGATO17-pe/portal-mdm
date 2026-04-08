from __future__ import annotations

from types import SimpleNamespace

from nucleo.http_utils import obtener_ip_cliente, obtener_request_id


def test_obtener_ip_cliente_usa_x_forwarded_for():
    request = SimpleNamespace(
        headers={"X-Forwarded-For": "10.0.0.1, 10.0.0.2"},
        client=SimpleNamespace(host="127.0.0.1"),
    )
    assert obtener_ip_cliente(request) == "10.0.0.1"


def test_obtener_ip_cliente_hace_fallback_a_client_host():
    request = SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.1"))
    assert obtener_ip_cliente(request) == "127.0.0.1"


def test_obtener_request_id_retorna_default_si_no_existe():
    request = SimpleNamespace(state=SimpleNamespace())
    assert obtener_request_id(request, "-") == "-"

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
import streamlit as st

URL_BACKEND = "http://127.0.0.1:8000"
URL_BASE = f"{URL_BACKEND}/api/v1"
TIMEOUT_API_SEG = (5, 30)

_SESSION = requests.Session()


@dataclass(slots=True)
class ResultadoApi:
    ok: bool
    status_code: int | None
    data: Any = None
    error: str | None = None
    request_id: str | None = None
    url: str | None = None


def _get_headers(content_type: str = "application/json") -> dict[str, str]:
    headers = {"Content-Type": content_type}
    token = st.session_state.get("jwt_token")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _extraer_request_id(response: requests.Response, data: Any) -> str | None:
    request_id = response.headers.get("X-Request-ID")
    if request_id:
        return request_id
    if isinstance(data, dict):
        return data.get("request_id")
    return None


def _intentar_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def _mensaje_error_http(status_code: int | None, data: Any) -> str:
    if isinstance(data, dict):
        for clave in ("mensaje", "detail", "error"):
            valor = data.get(clave)
            if valor:
                return str(valor)
    if status_code is None:
        return "Error desconocido de comunicación con el backend."
    return f"El backend respondió con estado HTTP {status_code}."


def _resultado_desde_respuesta(response: requests.Response) -> ResultadoApi:
    data = _intentar_json(response)
    ok = 200 <= response.status_code < 300
    return ResultadoApi(
        ok=ok,
        status_code=response.status_code,
        data=data,
        error=None if ok else _mensaje_error_http(response.status_code, data),
        request_id=_extraer_request_id(response, data),
        url=response.url,
    )


def _resultado_error(url: str, mensaje: str) -> ResultadoApi:
    return ResultadoApi(
        ok=False,
        status_code=None,
        data=None,
        error=mensaje,
        request_id=None,
        url=url,
    )


def _request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json: dict | None = None,
    data: dict | None = None,
    timeout: tuple[int, int] = TIMEOUT_API_SEG,
) -> ResultadoApi:
    try:
        response = _SESSION.request(
            method=method,
            url=url,
            headers=headers,
            json=json,
            data=data,
            timeout=timeout,
        )
        return _resultado_desde_respuesta(response)
    except requests.Timeout:
        return _resultado_error(url, "Timeout conectando al backend. Verifica el estado de la API.")
    except requests.RequestException as error:
        return _resultado_error(url, f"Error conectando al backend: {error}")


def mostrar_error_api(resultado: ResultadoApi, mensaje_base: str | None = None) -> None:
    partes = []
    if mensaje_base:
        partes.append(mensaje_base)
    if resultado.error:
        partes.append(resultado.error)
    if resultado.request_id:
        partes.append(f"request_id={resultado.request_id}")
    st.error(" | ".join(partes) if partes else "Error no controlado en la comunicación con el backend.")


def login_backend(username: str, password: str) -> ResultadoApi:
    return _request(
        "POST",
        f"{URL_BACKEND}/auth/login",
        data={"username": username, "password": password},
        headers=_get_headers("application/x-www-form-urlencoded"),
    )


def get_api(endpoint: str, base_url: str = URL_BASE) -> ResultadoApi:
    return _request("GET", f"{base_url}{endpoint}", headers=_get_headers())


def post_api(endpoint: str, payload: dict, base_url: str = URL_BASE) -> ResultadoApi:
    return _request("POST", f"{base_url}{endpoint}", json=payload, headers=_get_headers())


def patch_api(endpoint: str, payload: dict, base_url: str = URL_BASE) -> ResultadoApi:
    return _request("PATCH", f"{base_url}{endpoint}", json=payload, headers=_get_headers())


def delete_api(endpoint: str, base_url: str = URL_BASE) -> ResultadoApi:
    return _request("DELETE", f"{base_url}{endpoint}", headers=_get_headers())


def stream_api(id_corrida: str):
    """
    Lee el stream SSE de una corrida ETL línea a línea.

    Generador: yield str — cada línea de log emitida por el runner.
    Termina automáticamente cuando el servidor cierra la conexión
    o cuando el stream envía un evento con data '[DONE]'.

    Uso:
        for linea in stream_api(id_corrida):
            consola += linea + "\\n"
    """
    url     = f"{URL_BASE}/etl/corridas/{id_corrida}/eventos"
    headers = _get_headers()
    headers.pop("Content-Type", None)          # SSE no lleva Content-Type
    headers["Accept"] = "text/event-stream"

    try:
        with _SESSION.get(url, headers=headers, stream=True, timeout=(5, 1800)) as resp:
            if not resp.ok:
                yield f"[ERROR] El servidor respondió {resp.status_code}"
                return

            for raw_line in resp.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue                    # líneas vacías = separador SSE
                if raw_line.startswith("data:"):
                    payload = raw_line[5:].strip()
                    if payload == "[DONE]":
                        return
                    if payload:
                        yield payload
                elif raw_line.startswith("event:"):
                    pass                        # ignoramos el tipo de evento
    except requests.Timeout:
        yield "[TIMEOUT] El backend tardó demasiado en responder."
    except requests.RequestException as exc:
        yield f"[ERROR] Conexión perdida: {exc}"


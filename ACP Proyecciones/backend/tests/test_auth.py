"""
tests/test_auth.py
==================
Pruebas de contrato para el módulo de autenticación:
    POST /auth/login
    GET  /auth/me
    RBAC — acceso con y sin token, con rol insuficiente
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from nucleo.auth import crear_token, hash_clave, verificar_clave, UsuarioActual

# ─────────────────────────────────────────────────────────────────────────────
# Datos de prueba
# ─────────────────────────────────────────────────────────────────────────────

_CLAVE_OK   = "clave_segura_123"
_HASH_OK    = hash_clave(_CLAVE_OK)

_USUARIO_BD_OK = {
    "id_usuario":     1,
    "nombre_usuario": "analista01",
    "nombre_display": "Ana Analista",
    "email":          "ana@acp.pe",
    "hash_clave":     _HASH_OK,
    "rol":            "analista_mdm",
    "es_activo":      True,
    "fecha_creacion": None,
    "ultimo_acceso":  None,
}

_PATCH_BUSCAR  = "servicios.servicio_auth.repo.buscar_por_nombre"
_PATCH_ACCESO  = "servicios.servicio_auth.repo_log.registrar_acceso"
_PATCH_ULTIMO  = "servicios.servicio_auth.repo.registrar_ultimo_acceso"


def _token(nombre="analista01", rol="analista_mdm") -> str:
    return crear_token(
        nombre_usuario=nombre,
        rol=rol,
        nombre_display="Test Usuario",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pruebas unitarias del core de auth
# ─────────────────────────────────────────────────────────────────────────────

class TestCoreAuth:
    def test_hash_y_verificar(self):
        h = hash_clave("mi_clave")
        assert verificar_clave("mi_clave", h)
        assert not verificar_clave("mal_clave", h)

    def test_crear_token_y_decodificar(self):
        from nucleo.auth import decodificar_token
        token = crear_token("user01", "admin", "Usuario Test")
        payload = decodificar_token(token)
        assert payload["sub"] == "user01"
        assert payload["rol"] == "admin"

    def test_token_invalido_lanza_401(self, cliente):
        resp = cliente.get("/auth/me", headers={"Authorization": "Bearer token.invalido.xxx"})
        assert resp.status_code == 401

    def test_jerarquia_roles(self):
        admin        = UsuarioActual("u", "admin",        "U")
        operador     = UsuarioActual("u", "operador_etl", "U")
        analista     = UsuarioActual("u", "analista_mdm", "U")
        viewer       = UsuarioActual("u", "viewer",       "U")

        assert admin.tiene_rol("viewer")        # admin puede todo
        assert admin.tiene_rol("analista_mdm")
        assert admin.tiene_rol("operador_etl")
        assert admin.tiene_rol("admin")

        assert operador.tiene_rol("viewer")
        assert operador.tiene_rol("operador_etl")
        assert not operador.tiene_rol("admin")

        assert analista.tiene_rol("viewer")
        assert analista.tiene_rol("analista_mdm")
        assert not analista.tiene_rol("operador_etl")
        assert not analista.tiene_rol("admin")

        assert viewer.tiene_rol("viewer")
        assert not viewer.tiene_rol("analista_mdm")


# ─────────────────────────────────────────────────────────────────────────────
# Pruebas de endpoint /auth/login
# ─────────────────────────────────────────────────────────────────────────────

class TestLogin:
    def test_login_exitoso_retorna_token(self, cliente):
        with (
            patch(_PATCH_BUSCAR,  return_value=_USUARIO_BD_OK),
            patch(_PATCH_ACCESO,  return_value=None),
            patch(_PATCH_ULTIMO,  return_value=None),
        ):
            resp = cliente.post("/auth/login", data={
                "username": "analista01",
                "password": _CLAVE_OK,
            })
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert data["usuario"]["nombre_usuario"] == "analista01"
        assert data["usuario"]["rol"] == "analista_mdm"

    def test_login_clave_incorrecta_retorna_401(self, cliente):
        with (
            patch(_PATCH_BUSCAR,  return_value=_USUARIO_BD_OK),
            patch(_PATCH_ACCESO,  return_value=None),
        ):
            resp = cliente.post("/auth/login", data={
                "username": "analista01",
                "password": "clave_incorrecta",
            })
        assert resp.status_code == 401

    def test_login_usuario_inexistente_retorna_401(self, cliente):
        with (
            patch(_PATCH_BUSCAR,  return_value=None),
            patch(_PATCH_ACCESO,  return_value=None),
        ):
            resp = cliente.post("/auth/login", data={
                "username": "no_existe",
                "password": "cualquiera",
            })
        assert resp.status_code == 401

    def test_login_sin_username_retorna_422(self, cliente):
        resp = cliente.post("/auth/login", data={"password": "algo"})
        assert resp.status_code == 422


# ─────────────────────────────────────────────────────────────────────────────
# Pruebas de /auth/me
# ─────────────────────────────────────────────────────────────────────────────

class TestPerfil:
    def test_me_con_token_valido(self, cliente):
        token = _token("analista01", "analista_mdm")
        resp = cliente.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["nombre_usuario"] == "analista01"
        assert data["rol"] == "analista_mdm"

    def test_me_sin_token_retorna_401(self, cliente):
        resp = cliente.get("/auth/me")
        assert resp.status_code == 401

    def test_me_token_expirado_retorna_401(self, cliente):
        token = crear_token("u", "viewer", "U", ttl_minutos=-1)
        resp = cliente.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 401


# ─────────────────────────────────────────────────────────────────────────────
# Pruebas RBAC en endpoints críticos
# ─────────────────────────────────────────────────────────────────────────────

class TestRBACEndpointsETL:
    _URL = "/api/v1/etl/corridas"

    def test_sin_token_retorna_401(self, cliente):
        resp = cliente.get(self._URL)
        assert resp.status_code == 401

    def test_viewer_puede_ver_historial(self, cliente):
        token = _token("v01", "viewer")
        with patch("api.rutas_etl.obtener_historial", return_value=[]):
            resp = cliente.get(
                self._URL,
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200

    def test_viewer_no_puede_lanzar_etl(self, cliente):
        token = _token("v01", "viewer")
        resp = cliente.post(
            self._URL,
            json={},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

    def test_operador_puede_lanzar_etl(self, cliente):
        from unittest.mock import AsyncMock
        from datetime import datetime
        mock_corrida = {
            "id_corrida":   "uuid-op-test",
            "id_log":       99,
            "iniciado_por": "operador01",
            "fecha_inicio": datetime(2026, 1, 1),
        }
        token = _token("operador01", "operador_etl")
        with (
            patch("api.rutas_etl.iniciar_corrida", new_callable=AsyncMock, return_value=mock_corrida),
            patch("api.rutas_etl.registrar_accion", return_value=None),
        ):
            resp = cliente.post(
                self._URL,
                json={},
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200
        assert resp.json()["iniciado_por"] == "operador01"   # del JWT

    def test_iniciado_por_no_aceptado_del_body(self, cliente):
        """El campo iniciado_por ya no existe en el schema — FastAPI lo ignora."""
        from unittest.mock import AsyncMock
        from datetime import datetime
        mock_corrida = {
            "id_corrida":   "uuid-op-test2",
            "id_log":       100,
            "iniciado_por": "operador02",   # viene del servicio
            "fecha_inicio": datetime(2026, 1, 1),
        }
        token = _token("operador02", "operador_etl")
        with (
            patch("api.rutas_etl.iniciar_corrida", new_callable=AsyncMock, return_value=mock_corrida),
            patch("api.rutas_etl.registrar_accion", return_value=None),
        ):
            resp = cliente.post(
                self._URL,
                # Intenta inyectar iniciado_por desde el body
                json={"iniciado_por": "HACKER_INJECTION"},
                headers={"Authorization": f"Bearer {token}"},
            )
        # Debe ignorar el campo del body y usar el del JWT
        assert resp.status_code == 200
        assert resp.json()["iniciado_por"] == "operador02"


class TestRBACCuarentena:
    def test_viewer_puede_listar_cuarentena(self, cliente):
        token = _token("v01", "viewer")
        _pagina_vacia = {"total": 0, "pagina": 1, "tamano": 20, "datos": []}
        with patch("api.rutas_cuarentena.listar_cuarentena", return_value=_pagina_vacia):
            resp = cliente.get(
                "/api/v1/cuarentena",
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200

    def test_viewer_no_puede_resolver_cuarentena(self, cliente):
        token = _token("v01", "viewer")
        resp = cliente.patch(
            "/api/v1/cuarentena/Bronce.Cosecha/42/resolver",
            json={"valor_canonico": "Camarosa"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

    def test_analista_puede_resolver_cuarentena(self, cliente):
        token = _token("analista01", "analista_mdm")
        _accion_ok = {"id_registro": "42", "estado_nuevo": "RESUELTO", "mensaje": "OK"}
        with (
            patch("api.rutas_cuarentena.resolver_registro", return_value=_accion_ok),
            patch("api.rutas_cuarentena.registrar_accion",  return_value=None),
        ):
            resp = cliente.patch(
                "/api/v1/cuarentena/Bronce.Cosecha/42/resolver",
                json={"valor_canonico": "Camarosa"},
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200

    def test_analista_no_puede_lanzar_etl(self, cliente):
        token = _token("analista01", "analista_mdm")
        resp = cliente.post(
            "/api/v1/etl/corridas",
            json={},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 403

"""
api/rutas_health.py
====================
Endpoints de salud del backend ACP Platform.

Semántica:
    GET /health        — estado completo: proceso + BD
    GET /health/live   — liveness: el proceso respira (sin BD)
    GET /health/ready  — readiness: listo para tráfico (verifica BD)

Esta separación permite a orquestadores (k8s, compose, nginx) diferenciar
entre un proceso vivo pero no listo (ej: BD caída) y un proceso caído.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from nucleo.conexion import verificar_conexion
from nucleo.settings import settings
from repositorios.repo_control import obtener_estado_lock, obtener_resumen_control_plane

enrutador_health = APIRouter(tags=["Sistema"])

_SERVICIO = settings.api_titulo
_VERSION  = settings.api_version


def _timestamp() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _diagnostico_control_plane() -> tuple[bool, dict]:
    try:
        resumen = obtener_resumen_control_plane()
        lock = obtener_estado_lock()
        return True, {
            "estado": "operativo",
            "resumen": resumen,
            "lock": lock,
        }
    except Exception as exc:
        return False, {
            "estado": "error",
            "error": str(exc),
        }


@enrutador_health.get("/health/live", summary="Liveness check")
def liveness() -> JSONResponse:
    """
    Liveness probe — el proceso HTTP está activo.
    Nunca contacta la base de datos.
    Siempre retorna 200 si el proceso responde.
    """
    return JSONResponse(
        status_code=200,
        content={
            "servicio":  _SERVICIO,
            "version":   _VERSION,
            "entorno":   settings.entorno,
            "estado":    "vivo",
            "timestamp": _timestamp(),
        },
    )


@enrutador_health.get("/health/ready", summary="Readiness check")
def readiness() -> JSONResponse:
    """
    Readiness probe — el proceso está listo para servir tráfico.
    Verifica la conectividad con SQL Server.
    Retorna 200 si la BD responde, 503 si no.
    """
    info_bd = verificar_conexion()
    listo   = info_bd.get("conectado", False)

    return JSONResponse(
        status_code=200 if listo else 503,
        content={
            "servicio":    _SERVICIO,
            "version":     _VERSION,
            "entorno":     settings.entorno,
            "estado":      "listo" if listo else "no_listo",
            "base_datos":  info_bd,
            "timestamp":   _timestamp(),
        },
    )


@enrutador_health.get("/health/ready/control", summary="Readiness del control-plane ETL")
def readiness_control() -> JSONResponse:
    """
    Verifica que la BD responda y que el esquema Control.* sea accesible.
    """
    info_bd = verificar_conexion()
    bd_lista = info_bd.get("conectado", False)
    control_ok, control_plane = _diagnostico_control_plane() if bd_lista else (False, {
        "estado": "no_verificado",
        "error": "Base de datos no disponible",
    })
    listo = bd_lista and control_ok

    return JSONResponse(
        status_code=200 if listo else 503,
        content={
            "servicio": _SERVICIO,
            "version": _VERSION,
            "entorno": settings.entorno,
            "estado": "listo" if listo else "no_listo",
            "base_datos": info_bd,
            "control_plane": control_plane,
            "timestamp": _timestamp(),
        },
    )


@enrutador_health.get("/health/ready/runner", summary="Readiness del runner ETL")
def readiness_runner() -> JSONResponse:
    """
    Verifica que el control-plane esté sano y que no exista un lock vencido.
    """
    info_bd = verificar_conexion()
    bd_lista = info_bd.get("conectado", False)
    control_ok, control_plane = _diagnostico_control_plane() if bd_lista else (False, {
        "estado": "no_verificado",
        "error": "Base de datos no disponible",
    })
    estado_lock = (control_plane.get("lock") or {}).get("estado_lock") if control_ok else None
    runner_sano = bd_lista and control_ok and estado_lock != "VENCIDO"

    if not bd_lista:
        estado = "no_listo"
    elif not control_ok:
        estado = "control_plane_error"
    elif estado_lock == "VENCIDO":
        estado = "lock_vencido"
    elif estado_lock == "ACTIVO":
        estado = "ocupado"
    else:
        estado = "libre"

    return JSONResponse(
        status_code=200 if runner_sano else 503,
        content={
            "servicio": _SERVICIO,
            "version": _VERSION,
            "entorno": settings.entorno,
            "estado": estado,
            "base_datos": info_bd,
            "control_plane": control_plane,
            "timestamp": _timestamp(),
        },
    )


@enrutador_health.get("/health/lock", summary="Estado actual del lock del runner")
def estado_lock() -> JSONResponse:
    """
    Expone el estado del lock del runner para diagnóstico operativo.
    """
    info_bd = verificar_conexion()
    bd_lista = info_bd.get("conectado", False)
    if not bd_lista:
        return JSONResponse(
            status_code=503,
            content={
                "servicio": _SERVICIO,
                "version": _VERSION,
                "entorno": settings.entorno,
                "estado": "no_listo",
                "base_datos": info_bd,
                "lock": None,
                "timestamp": _timestamp(),
            },
        )

    try:
        lock = obtener_estado_lock()
        return JSONResponse(
            status_code=200,
            content={
                "servicio": _SERVICIO,
                "version": _VERSION,
                "entorno": settings.entorno,
                "estado": lock.get("estado_lock", "DESCONOCIDO").lower(),
                "base_datos": info_bd,
                "lock": lock,
                "timestamp": _timestamp(),
            },
        )
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={
                "servicio": _SERVICIO,
                "version": _VERSION,
                "entorno": settings.entorno,
                "estado": "error",
                "base_datos": info_bd,
                "lock": {"error": str(exc)},
                "timestamp": _timestamp(),
            },
        )


@enrutador_health.get("/health", summary="Estado completo del servicio")
def health() -> JSONResponse:
    """
    Health check completo: proceso + base de datos + metadatos.
    Útil para dashboards de monitoreo.
    """
    info_bd = verificar_conexion()
    listo   = info_bd.get("conectado", False)

    return JSONResponse(
        status_code=200,
        content={
            "servicio":    _SERVICIO,
            "version":     _VERSION,
            "entorno":     settings.entorno,
            "estado":      "activo" if listo else "degradado",
            "base_datos":  info_bd,
            "timestamp":   _timestamp(),
        },
    )

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

enrutador_health = APIRouter(tags=["Sistema"])

_SERVICIO = settings.api_titulo
_VERSION  = settings.api_version


def _timestamp() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


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

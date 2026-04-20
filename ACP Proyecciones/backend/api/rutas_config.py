"""
api/rutas_config.py
====================
Router /api/v1/config — Reglas de validación y parámetros del pipeline.

Seguridad: analista_mdm+ (nivel 20). admin (40) pasa automáticamente.
Nota: El rol 'editor' NO existe en la jerarquía — nunca usar.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from nucleo.auth import require_rol
from schemas.config.respuesta import (
    RespuestaPaginadaParametros,
    RespuestaPaginadaReglas,
)
import repositorios.repo_config as repo

enrutador_config = APIRouter(prefix="/v1/config", tags=["Configuración"])


@enrutador_config.get(
    "/reglas",
    response_model=RespuestaPaginadaReglas,
    summary="Lista reglas de validación",
    dependencies=[Depends(require_rol("analista_mdm"))],
)
def obtener_reglas(
    pagina: int = Query(default=1, ge=1),
    tamano: int = Query(default=15, ge=1, le=10000),
) -> RespuestaPaginadaReglas:
    return RespuestaPaginadaReglas(**repo.listar_reglas(pagina=pagina, tamano=tamano))


@enrutador_config.get(
    "/parametros",
    response_model=RespuestaPaginadaParametros,
    summary="Lista parámetros del pipeline",
    dependencies=[Depends(require_rol("analista_mdm"))],
)
def obtener_parametros(
    pagina: int = Query(default=1, ge=1),
    tamano: int = Query(default=10, ge=1, le=10000),
) -> RespuestaPaginadaParametros:
    return RespuestaPaginadaParametros(**repo.listar_parametros(pagina=pagina, tamano=tamano))

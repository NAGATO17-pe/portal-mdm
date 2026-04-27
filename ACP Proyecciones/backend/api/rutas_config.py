"""
api/rutas_config.py
====================
Router /api/v1/config — Reglas de validación y parámetros del pipeline.

Seguridad: analista_mdm+ (nivel 20). admin (40) pasa automáticamente.
Nota: El rol 'editor' NO existe en la jerarquía — nunca usar.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, HTTPException, status
from nucleo.auth import require_rol, obtener_usuario_actual
from schemas.config.respuesta import (
    RespuestaPaginadaParametros,
    RespuestaPaginadaReglas,
)
from schemas.config.peticion import SolicitudActualizarParametro, SolicitudBatchParametros
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



@enrutador_config.patch(
    "/parametros",
    summary="Actualiza múltiples parámetros del pipeline",
    dependencies=[Depends(require_rol("analista_mdm"))],
)
def actualizar_parametros_batch(
    peticion: SolicitudBatchParametros,
    usuario = Depends(obtener_usuario_actual),
):
    exitos = 0
    fallos = []
    for p in peticion.parametros:
        exito = repo.actualizar_parametro(
            nombre=p.nombre_parametro,
            valor=p.valor,
            modificado_por=usuario.nombre_usuario
        )
        if exito:
            exitos += 1
        else:
            fallos.append(p.nombre_parametro)
    
    return {
        "mensaje": f"Proceso finalizado. Exitos: {exitos}, Fallos: {len(fallos)}",
        "fallos": fallos
    }

@enrutador_config.patch(
    "/parametros/{nombre}",
    summary="Actualiza un solo parámetro del pipeline",
    dependencies=[Depends(require_rol("analista_mdm"))],
)
def actualizar_parametro(
    nombre: str,
    peticion: SolicitudActualizarParametro,
    usuario = Depends(obtener_usuario_actual),
):
    exito = repo.actualizar_parametro(
        nombre=nombre,
        valor=peticion.valor,
        modificado_por=usuario.nombre_usuario
    )
    if not exito:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Parámetro '{nombre}' no encontrado"
        )
    return {"mensaje": f"Parámetro '{nombre}' actualizado con éxito"}

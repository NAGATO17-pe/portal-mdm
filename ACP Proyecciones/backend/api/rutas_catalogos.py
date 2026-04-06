"""
api/rutas_catalogos.py
======================
Router /api/v1/catalogos — Catálogos MDM y Silver de solo lectura.

Seguridad: viewer+ en todos los endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from nucleo.auth import require_rol
from schemas.catalogos.respuesta import (
    RespuestaGeografia,
    RespuestaPaginadaCatalogo,
    RespuestaPersonal,
    RespuestaVariedad,
)
from servicios.servicio_catalogos import listar_geografia, listar_personal, listar_variedades

enrutador_catalogos = APIRouter(prefix="/v1/catalogos", tags=["Catálogos"])


@enrutador_catalogos.get(
    "/variedades",
    summary="Lista el catálogo de variedades activas",
    dependencies=[Depends(require_rol("viewer"))],
)
def obtener_variedades(
    pagina: int = Query(default=1, ge=1),
    tamano: int = Query(default=20, ge=1, le=100),
) -> RespuestaPaginadaCatalogo:
    resultado = listar_variedades(pagina=pagina, tamano=tamano)
    return RespuestaPaginadaCatalogo(
        total=resultado["total"],
        pagina=resultado["pagina"],
        tamano=resultado["tamano"],
        datos=[RespuestaVariedad(**fila) for fila in resultado["datos"]],
    )


@enrutador_catalogos.get(
    "/geografia",
    summary="Lista la geografía vigente",
    dependencies=[Depends(require_rol("viewer"))],
)
def obtener_geografia(
    pagina: int = Query(default=1, ge=1),
    tamano: int = Query(default=20, ge=1, le=100),
) -> RespuestaPaginadaCatalogo:
    resultado = listar_geografia(pagina=pagina, tamano=tamano)
    return RespuestaPaginadaCatalogo(
        total=resultado["total"],
        pagina=resultado["pagina"],
        tamano=resultado["tamano"],
        datos=[RespuestaGeografia(**fila) for fila in resultado["datos"]],
    )


@enrutador_catalogos.get(
    "/personal",
    summary="Lista el catálogo de personal",
    dependencies=[Depends(require_rol("viewer"))],
)
def obtener_personal(
    pagina: int = Query(default=1, ge=1),
    tamano: int = Query(default=20, ge=1, le=100),
) -> RespuestaPaginadaCatalogo:
    resultado = listar_personal(pagina=pagina, tamano=tamano)
    return RespuestaPaginadaCatalogo(
        total=resultado["total"],
        pagina=resultado["pagina"],
        tamano=resultado["tamano"],
        datos=[RespuestaPersonal(**fila) for fila in resultado["datos"]],
    )

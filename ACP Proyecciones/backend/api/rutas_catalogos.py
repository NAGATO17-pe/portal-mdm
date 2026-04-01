"""
api/rutas_catalogos.py
======================
Router de catalogos MDM y Silver de solo lectura.
"""

from fastapi import APIRouter, Query

from schemas.catalogos.respuesta import (
    RespuestaGeografia,
    RespuestaPaginadaCatalogo,
    RespuestaPersonal,
    RespuestaVariedad,
)
from servicios.servicio_catalogos import listar_geografia, listar_personal, listar_variedades

enrutador_catalogos = APIRouter(prefix="/catalogos", tags=["Catalogos"])


@enrutador_catalogos.get(
    "/variedades",
    summary="Lista el catalogo de variedades activas",
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
    summary="Lista la geografia vigente",
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
    summary="Lista el catalogo de personal",
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

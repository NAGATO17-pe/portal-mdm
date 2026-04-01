"""
api/rutas_cuarentena.py
========================
Router del módulo Cuarentena.
Solo valida entrada, llama al servicio y serializa la respuesta.
"""

from fastapi import APIRouter, Query

from schemas.cuarentena.peticion import PeticionResolverCuarentena, PeticionRechazarCuarentena
from schemas.cuarentena.respuesta import RespuestaPaginada, RespuestaAccionCuarentena, RespuestaCuarentena
from servicios.servicio_cuarentena import listar_cuarentena, resolver_registro, rechazar_registro

enrutador_cuarentena = APIRouter(prefix="/cuarentena", tags=["Cuarentena"])


@enrutador_cuarentena.get(
    "",
    response_model=RespuestaPaginada,
    summary="Lista registros en cuarentena",
    description=(
        "Lista registros pendientes desde MDM.Cuarentena. "
        "Soporta filtro por tabla origen y paginacion server-side."
    ),
)
def listar(
    pagina:        int = Query(default=1,  ge=1,   description="Número de página (inicia en 1)."),
    tamano:        int = Query(default=20, ge=1,   le=100, description="Registros por pagina."),
    tabla_filtro:  str | None = Query(default=None, description="Filtrar por nombre de tabla Bronce."),
) -> RespuestaPaginada:
    resultado = listar_cuarentena(pagina=pagina, tamano=tamano, tabla_filtro=tabla_filtro)
    datos = [RespuestaCuarentena(**r) for r in resultado["datos"]]
    return RespuestaPaginada(
        total=resultado["total"],
        pagina=resultado["pagina"],
        tamano=resultado["tamano"],
        datos=datos,
    )


@enrutador_cuarentena.patch(
    "/{tabla_origen}/{id_registro}/resolver",
    response_model=RespuestaAccionCuarentena,
    summary="Marca un registro de cuarentena como resuelto",
)
def resolver(
    tabla_origen: str,
    id_registro:  str,
    cuerpo:       PeticionResolverCuarentena,
) -> RespuestaAccionCuarentena:
    resultado = resolver_registro(
        tabla_origen=tabla_origen,
        id_registro=id_registro,
        valor_canonico=cuerpo.valor_canonico,
        analista=cuerpo.analista,
        comentario=cuerpo.comentario,
    )
    return RespuestaAccionCuarentena(**resultado)


@enrutador_cuarentena.patch(
    "/{tabla_origen}/{id_registro}/rechazar",
    response_model=RespuestaAccionCuarentena,
    summary="Marca un registro de cuarentena como descartado",
)
def rechazar(
    tabla_origen: str,
    id_registro:  str,
    cuerpo:       PeticionRechazarCuarentena,
) -> RespuestaAccionCuarentena:
    resultado = rechazar_registro(
        tabla_origen=tabla_origen,
        id_registro=id_registro,
        motivo=cuerpo.motivo,
        analista=cuerpo.analista,
    )
    return RespuestaAccionCuarentena(**resultado)

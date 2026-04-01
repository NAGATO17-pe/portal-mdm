"""
api/rutas_auditoria.py
=======================
Router del módulo Auditoría.
Solo consultas de lectura a Auditoria.Log_Carga.
"""

from fastapi import APIRouter, Query

from schemas.auditoria.respuesta import RespuestaLogCarga, RespuestaUltimoEstado
from servicios.servicio_auditoria import obtener_historial, obtener_ultimo_estado_tabla

enrutador_auditoria = APIRouter(prefix="/auditoria", tags=["Auditoría"])


@enrutador_auditoria.get(
    "/log-carga",
    response_model=list[RespuestaLogCarga],
    summary="Historial de cargas ETL",
    description="Retorna las últimas N entradas de Auditoria.Log_Carga ordenadas por fecha desc.",
)
def listar_log_carga(
    limite: int = Query(default=50, ge=1, le=500, description="Máximo de registros a retornar."),
) -> list[RespuestaLogCarga]:
    registros = obtener_historial(limite=limite)
    return [RespuestaLogCarga(**r) for r in registros]


@enrutador_auditoria.get(
    "/log-carga/{tabla_destino}",
    response_model=RespuestaUltimoEstado,
    summary="Último estado de una tabla",
    description="Retorna la última entrada de Auditoria.Log_Carga para una tabla específica.",
)
def ultimo_estado(tabla_destino: str) -> RespuestaUltimoEstado:
    datos = obtener_ultimo_estado_tabla(tabla_destino)
    if datos is None:
        from nucleo.excepciones import ErrorRecursoNoEncontrado
        raise ErrorRecursoNoEncontrado(f"Tabla '{tabla_destino}'")
    return RespuestaUltimoEstado(**datos)

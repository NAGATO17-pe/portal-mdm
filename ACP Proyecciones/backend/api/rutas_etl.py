"""
api/rutas_etl.py
================
Router del módulo ETL.
Responsabilidades: validar entrada, llamar al servicio, retornar schema de respuesta.
Nunca contiene lógica de negocio ni acceso directo a la BD.
"""

from datetime import datetime

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from broker.broker_sse import corrida_existe, generar_eventos
from nucleo.excepciones import ErrorCorridaNoEncontrada
from schemas.etl.peticion import PeticionIniciarCorrida
from schemas.etl.respuesta import RespuestaCorridaIniciada, RespuestaHistorialCorrida
from servicios.servicio_etl import iniciar_corrida
from servicios.servicio_auditoria import obtener_historial

enrutador_etl = APIRouter(prefix="/etl", tags=["ETL"])


@enrutador_etl.post(
    "/corridas",
    response_model=RespuestaCorridaIniciada,
    summary="Inicia una corrida del pipeline ETL",
    description=(
        "Registra la corrida en Auditoria.Log_Carga, crea la cola SSE "
        "y lanza el subproceso pipeline.py en segundo plano."
    ),
)
async def iniciar_corrida_etl(cuerpo: PeticionIniciarCorrida) -> RespuestaCorridaIniciada:
    datos = await iniciar_corrida(
        iniciado_por=cuerpo.iniciado_por,
        comentario=cuerpo.comentario,
    )
    return RespuestaCorridaIniciada(
        id_corrida=datos["id_corrida"],
        id_log=datos["id_log"],
        iniciado_por=datos["iniciado_por"],
        fecha_inicio=datos["fecha_inicio"],
        url_stream=f"/api/etl/corridas/{datos['id_corrida']}/eventos",
    )


@enrutador_etl.get(
    "/corridas/{id_corrida}/eventos",
    summary="Stream SSE de una corrida activa",
    description=(
        "Abre un canal Server-Sent Events que transmite las líneas del pipeline "
        "en tiempo real hasta recibir el sentinel [FIN_CORRIDA]."
    ),
)
async def stream_eventos_corrida(id_corrida: str, request: Request) -> EventSourceResponse:
    if not corrida_existe(id_corrida):
        raise ErrorCorridaNoEncontrada(id_corrida)

    return EventSourceResponse(generar_eventos(id_corrida))


@enrutador_etl.get(
    "/corridas",
    response_model=list[RespuestaHistorialCorrida],
    summary="Historial de corridas ETL",
    description="Retorna las últimas N ejecuciones registradas en Auditoria.Log_Carga.",
)
def listar_historial(limite: int = 50) -> list[RespuestaHistorialCorrida]:
    registros = obtener_historial(limite=limite)
    return [RespuestaHistorialCorrida(**r) for r in registros]

"""
schemas/etl/respuesta.py
=========================
Schemas de SALIDA para el módulo ETL.
Nunca se reutilizan como schemas de petición.
"""

from datetime import datetime
from pydantic import BaseModel, Field


class RespuestaCorridaIniciada(BaseModel):
    """Respuesta al cliente cuando se acepta una corrida ETL."""
    id_corrida:     str      = Field(description="UUID único de la corrida para suscribirse al SSE.")
    id_log:         int | None = Field(description="ID del registro en Auditoria.Log_Carga.")
    iniciado_por:   str      = Field(description="Quién inició la corrida.")
    fecha_inicio:   datetime = Field(description="Timestamp de inicio registrado.")
    url_stream:     str      = Field(description="URL para suscribirse al stream SSE de esta corrida.")

    model_config = {"from_attributes": True}


class RespuestaHistorialCorrida(BaseModel):
    """Una entrada del historial de corridas de Auditoria.Log_Carga."""
    id_log:             int
    nombre_proceso:     str
    tabla_destino:      str
    nombre_archivo:     str | None
    fecha_inicio:       datetime | None
    fecha_fin:          datetime | None
    estado:             str
    filas_insertadas:   int
    filas_rechazadas:   int
    duracion_segundos:  int | None
    mensaje_error:      str | None

    model_config = {"from_attributes": True}

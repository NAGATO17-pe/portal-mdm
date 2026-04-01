"""
schemas/etl/peticion.py
========================
Schemas de ENTRADA para el módulo ETL.
Nunca se reutilizan como schemas de respuesta.
"""

from pydantic import BaseModel, Field


class PeticionIniciarCorrida(BaseModel):
    """
    Cuerpo de la petición para iniciar una corrida del pipeline ETL.
    Por ahora no requiere parámetros obligatorios; se puede extender
    con filtros de fechas o configuración custom.
    """
    iniciado_por: str = Field(
        default="backend_api",
        description="Identificador del sistema o usuario que dispara la corrida.",
        max_length=100,
    )
    comentario: str | None = Field(
        default=None,
        description="Comentario opcional para registrar en la auditoría.",
        max_length=500,
    )

"""
schemas/etl/peticion.py
========================
Schemas de ENTRADA para el módulo ETL.
Versión 2: iniciado_por se elimina del body — se extrae del token JWT.
"""

from pydantic import BaseModel, Field


class PeticionIniciarCorrida(BaseModel):
    """
    Cuerpo de la petición para iniciar una corrida del pipeline ETL.

    IMPORTANTE: el campo 'iniciado_por' ya no es parte del body.
    El backend lo deriva del usuario autenticado (JWT).
    """
    comentario: str | None = Field(
        default=None,
        description="Comentario opcional para registrar en la auditoría.",
        max_length=500,
    )

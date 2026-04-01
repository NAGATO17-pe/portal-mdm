"""
schemas/cuarentena/peticion.py
===============================
Schemas de ENTRADA para el módulo Cuarentena.
"""

from pydantic import BaseModel, Field


class PeticionResolverCuarentena(BaseModel):
    """Cuerpo para aprobar un registro de cuarentena."""
    valor_canonico: str = Field(
        description="Valor canónico al que se homologa el registro.",
        min_length=1,
        max_length=200,
    )
    comentario: str | None = Field(
        default=None,
        description="Comentario de la decisión MDM.",
        max_length=500,
    )
    analista: str = Field(
        default="api_user",
        description="Identificador del analista que toma la decisión.",
        max_length=100,
    )


class PeticionRechazarCuarentena(BaseModel):
    """Cuerpo para descartar un registro de cuarentena."""
    motivo: str = Field(
        description="Razón del descarte.",
        min_length=1,
        max_length=500,
    )
    analista: str = Field(
        default="api_user",
        max_length=100,
    )

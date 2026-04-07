"""
schemas/etl/peticion.py
========================
Schemas de ENTRADA para el módulo ETL.
Versión 2: iniciado_por se elimina del body — se extrae del token JWT.
"""

from typing import Literal

from pydantic import BaseModel, Field, model_validator


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
    modo_ejecucion: Literal["completo", "facts"] = Field(
        default="completo",
        description="Modo de corrida. 'facts' habilita reproceso dirigido.",
    )
    facts: list[str] | None = Field(
        default=None,
        description="Facts a reprocesar cuando modo_ejecucion='facts'.",
    )
    incluir_dependencias: bool = Field(
        default=True,
        description="Ejecuta dimensiones y SPs dependientes antes del reproceso.",
    )
    refrescar_gold: bool = Field(
        default=True,
        description="Refresca los marts Gold impactados.",
    )
    forzar_relectura_bronce: bool = Field(
        default=True,
        description="Reabre filas PROCESADO/RECHAZADO en Bronce antes del reproceso.",
    )

    @model_validator(mode="after")
    def validar_modo_y_facts(self) -> "PeticionIniciarCorrida":
        if self.modo_ejecucion == "facts" and not self.facts:
            raise ValueError("facts es obligatorio cuando modo_ejecucion='facts'.")
        return self

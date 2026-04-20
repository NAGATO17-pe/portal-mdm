"""
schemas/reinyeccion/respuesta.py
==================================
Schemas de SALIDA para la Herramienta de Reinyección MDM.
"""
from pydantic import BaseModel, Field


class RespuestaConteoReinyeccion(BaseModel):
    """Cuántos registros están disponibles para reinyección."""
    candidatos: int = Field(description="Total de registros RESUELTOS listos para reinyectar.")


class RespuestaReinyeccion(BaseModel):
    """Resultado de una ejecución masiva de reinyección."""
    reinyectados: int = Field(description="Filas que fueron restauradas a Estado_Carga=CARGADO en Bronce.")
    omitidos:     int = Field(description="Filas que no pudieron reinyectarse (tabla no mapeada o sin ID origen).")
    detalle:      list[str] = Field(description="Detalle por tabla con el resultado de cada operación.")
    mensaje:      str = Field(description="Mensaje de resumen para el usuario.")

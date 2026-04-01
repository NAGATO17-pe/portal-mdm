"""
servicios/servicio_auditoria.py
================================
Lógica de consulta de auditoría.
Delega todo el acceso a datos a repositorios.repo_auditoria.
"""

from __future__ import annotations

import repositorios.repo_auditoria as repo


def obtener_historial(limite: int = 50) -> list[dict]:
    """Retorna las últimas N corridas registradas en Auditoria.Log_Carga."""
    return repo.listar_corridas(limite=limite)


def obtener_ultimo_estado_tabla(tabla_destino: str) -> dict | None:
    """
    Retorna el último estado de carga para una tabla específica.
    None si no hay registros. Propaga ErrorBaseDatos si falla la BD.
    """
    return repo.ultimo_estado_tabla(tabla_destino)

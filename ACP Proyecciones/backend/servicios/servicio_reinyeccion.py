"""
servicios/servicio_reinyeccion.py
==================================
Servicio de orquestación para la Herramienta de Reinyección MDM.

Responsabilidades:
  - Obtener los candidatos RESUELTOS desde repo_reinyeccion.
  - Delegar la actualización masiva en Bronce al repositorio.
  - Invalidar caché de cuarentena post-reinyección.
  - Registrar la acción en auditoría MDM.
"""

from __future__ import annotations

from nucleo.cache import cache
from nucleo.logging import obtener_logger
import repositorios.repo_reinyeccion as repo
import repositorios.repo_auditoria as repo_auditoria

log = obtener_logger(__name__)


def contar_candidatos(tabla_filtro: str | None = None) -> int:
    """Retorna cuántos registros RESUELTOS están disponibles para reinyectar."""
    return repo.contar_candidatos_reinyeccion(tabla_filtro=tabla_filtro)


def ejecutar_reinyeccion(
    analista: str,
    tabla_filtro: str | None = None,
) -> dict:
    """
    Flujo completo de reinyección:
    1. Obtiene todos los candidatos RESUELTOS (con ID_Registro_Origen válido).
    2. Actualiza masivamente Estado_Carga = 'CARGADO' en Bronce.
    3. Invalida la caché de cuarentena para que las vistas se actualicen.
    4. Registra la acción en auditoría MDM.

    Retorna: {reinyectados, omitidos, detalle}
    """
    log.info("Iniciando reinyección MDM", extra={"analista": analista, "filtro": tabla_filtro})

    candidatos = repo.obtener_resueltos_pendientes(tabla_filtro=tabla_filtro)

    if not candidatos:
        log.info("Sin candidatos para reinyección", extra={"analista": analista})
        return {
            "reinyectados": 0,
            "omitidos": 0,
            "detalle": ["ℹ️ No hay registros RESUELTOS con ID de origen válido para reinyectar."],
        }

    resultado = repo.reinyectar_en_bronce(candidatos)

    # Invalidar caché para que el dashboard de cuarentena refleje el estado actualizado
    cache.limpiar_todo()

    # Auditoría — un registro agregado por ejecución
    try:
        repo_auditoria.insertar_decision_mdm(
            tabla_origen="REINYECCION_MASIVA",
            id_registro="BATCH",
            valor_canonico="",
            decision="REINYECCION",
            analista=analista,
            comentario=(
                f"Reinyección masiva: {resultado['reinyectados']} registros reactivados, "
                f"{resultado['omitidos']} omitidos. Filtro: {tabla_filtro or 'todas las tablas'}."
            ),
        )
    except Exception:
        log.warning("No se pudo registrar auditoría de reinyección", exc_info=True)

    log.info(
        "Reinyección completada",
        extra={
            "reinyectados": resultado["reinyectados"],
            "omitidos": resultado["omitidos"],
            "analista": analista,
        },
    )
    return resultado

"""
repositorios/repo_auditoria.py
================================
Todas las consultas SQL relacionadas con Auditoria.Log_Carga
y Auditoria.Log_Decisiones_MDM.

Contrato:
- Recibe parámetros tipados.
- Retorna dicts o None; nunca retorna Row de SQLAlchemy al exterior.
- Propaga ErrorBaseDatos si falla la BD.
- Falla silenciosamente solo donde está documentado (registro de auditoría).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from nucleo.conexion import obtener_engine
from nucleo.excepciones import ErrorBaseDatos
from nucleo.logging import obtener_logger

log = obtener_logger(__name__)


# ── Escritura ──────────────────────────────────────────────────────────────────

def insertar_inicio_corrida(
    nombre_proceso: str,
    tabla_destino: str,
    nombre_archivo: str = "API_BACKEND",
) -> int | None:
    """
    Inserta una fila en Auditoria.Log_Carga con estado EN_PROCESO.
    Retorna el ID generado o None si falla (fallo silencioso).
    """
    try:
        with obtener_engine().begin() as con:
            resultado = con.execute(
                text("""
                    INSERT INTO Auditoria.Log_Carga (
                        Nombre_Proceso,
                        Tabla_Destino,
                        Nombre_Archivo_Fuente,
                        Fecha_Inicio,
                        Estado_Proceso,
                        Filas_Leidas,
                        Filas_Insertadas,
                        Filas_Rechazadas,
                        Duracion_Segundos,
                        Mensaje_Error
                    )
                    OUTPUT INSERTED.ID_Log_Carga
                    VALUES (
                        :nombre_proceso,
                        :tabla_destino,
                        :nombre_archivo,
                        :fecha_inicio,
                        'EN_PROCESO',
                        0, 0, 0, 0, NULL
                    )
                """),
                {
                    "nombre_proceso": nombre_proceso,
                    "tabla_destino":  tabla_destino,
                    "nombre_archivo": nombre_archivo,
                    "fecha_inicio":   datetime.now(),
                },
            )
            id_log = resultado.fetchone()[0]
            log.debug("Corrida registrada en auditoría", extra={"id_log": id_log})
            return id_log
    except SQLAlchemyError:
        log.exception("No se pudo registrar inicio de corrida en auditoría")
        return None


def actualizar_fin_corrida(
    id_log: int,
    estado: str,
    filas_insertadas: int = 0,
    filas_rechazadas: int = 0,
    mensaje_error: str | None = None,
) -> None:
    """
    Actualiza la fila de auditoría al finalizar (OK o ERROR).
    Falla silenciosamente para no bloquear la respuesta al cliente.
    """
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("""
                    UPDATE Auditoria.Log_Carga
                    SET
                        Fecha_Fin         = :fecha_fin,
                        Estado_Proceso    = :estado,
                        Filas_Insertadas  = :filas_insertadas,
                        Filas_Rechazadas  = :filas_rechazadas,
                        Duracion_Segundos = DATEDIFF(SECOND, Fecha_Inicio, :fecha_fin),
                        Mensaje_Error     = :mensaje_error
                    WHERE ID_Log_Carga = :id_log
                """),
                {
                    "fecha_fin":        datetime.now(),
                    "estado":           estado,
                    "filas_insertadas": filas_insertadas,
                    "filas_rechazadas": filas_rechazadas,
                    "mensaje_error":    mensaje_error,
                    "id_log":           id_log,
                },
            )
    except SQLAlchemyError:
        log.exception("No se pudo actualizar fin de corrida en auditoría", extra={"id_log": id_log})


def insertar_decision_mdm(
    tabla_origen: str,
    id_registro: str,
    valor_canonico: str,
    decision: str,
    analista: str,
    comentario: str,
) -> None:
    """
    Registra la decisión MDM en Auditoria.Log_Decisiones_MDM.
    Falla silenciosamente si la tabla no existe o hay error.
    """
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("""
                    INSERT INTO Auditoria.Log_Decisiones_MDM (
                        Tabla_Origen,
                        Texto_Crudo,
                        Valor_Canonico,
                        Decision,
                        Analista_DNI,
                        Comentario,
                        Fecha_Decision
                    ) VALUES (
                        :tabla_origen,
                        :id_registro,
                        :valor_canonico,
                        :decision,
                        :analista,
                        :comentario,
                        :fecha_decision
                    )
                """),
                {
                    "tabla_origen":    tabla_origen,
                    "id_registro":     id_registro,
                    "valor_canonico":  valor_canonico,
                    "decision":        decision,
                    "analista":        analista,
                    "comentario":      comentario,
                    "fecha_decision":  datetime.now(),
                },
            )
    except SQLAlchemyError:
        log.warning("No se pudo registrar decisión MDM en auditoría")


# ── Lectura ────────────────────────────────────────────────────────────────────

def listar_corridas(
    limite: int = 50,
    tabla_destino: str | None = None,
) -> list[dict]:
    """
    Consulta el historial de corridas en Auditoria.Log_Carga.
    Retorna lista de dicts listos para serializar a JSON.
    """
    filtro = "WHERE Tabla_Destino = :tabla" if tabla_destino else ""
    params: dict = {"limite": limite}
    if tabla_destino:
        params["tabla"] = tabla_destino

    try:
        with obtener_engine().connect() as con:
            filas = con.execute(
                text(f"""
                    SELECT TOP (:limite)
                        ID_Log_Carga            AS id_log,
                        Nombre_Proceso          AS nombre_proceso,
                        Tabla_Destino           AS tabla_destino,
                        Nombre_Archivo_Fuente   AS nombre_archivo,
                        Fecha_Inicio            AS fecha_inicio,
                        Fecha_Fin               AS fecha_fin,
                        Estado_Proceso          AS estado,
                        Filas_Insertadas        AS filas_insertadas,
                        Filas_Rechazadas        AS filas_rechazadas,
                        Duracion_Segundos       AS duracion_segundos,
                        Mensaje_Error           AS mensaje_error
                    FROM Auditoria.Log_Carga
                    {filtro}
                    ORDER BY Fecha_Inicio DESC
                """),
                params,
            ).fetchall()
            return [dict(fila._mapping) for fila in filas]
    except SQLAlchemyError as error:
        log.exception("Error al listar corridas de auditoría")
        return []


def ultimo_estado_tabla(tabla_destino: str) -> dict | None:
    """
    Retorna el último estado de carga para una tabla específica.
    None si no hay registros.
    """
    try:
        with obtener_engine().connect() as con:
            fila = con.execute(
                text("""
                    SELECT TOP 1
                        Tabla_Destino       AS tabla_destino,
                        Estado_Proceso      AS estado,
                        Fecha_Inicio        AS fecha_inicio,
                        Fecha_Fin           AS fecha_fin,
                        Filas_Insertadas    AS filas_insertadas,
                        Duracion_Segundos   AS duracion_segundos,
                        Mensaje_Error       AS mensaje_error
                    FROM Auditoria.Log_Carga
                    WHERE Tabla_Destino = :tabla
                    ORDER BY Fecha_Inicio DESC
                """),
                {"tabla": tabla_destino},
            ).fetchone()
            return dict(fila._mapping) if fila else None
    except SQLAlchemyError:
        log.exception("Error al consultar último estado de tabla", extra={"tabla": tabla_destino})
        raise ErrorBaseDatos()

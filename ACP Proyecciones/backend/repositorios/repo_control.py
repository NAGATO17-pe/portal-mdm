"""
repositorios/repo_control.py
=============================
Todas las operaciones SQL sobre el esquema Control.*.

Tablas cubiertas:
  Control.Corrida
  Control.Corrida_Evento
  Control.Corrida_Paso
  Control.Bloqueo_Ejecucion
  Control.Comando_Ejecucion

Contrato:
  - Recibe parámetros tipados, retorna dicts o None.
  - Nunca expone Row de SQLAlchemy al exterior.
  - Propaga ErrorBaseDatos en lecturas críticas.
  - Falla silenciosamente solo en escrituras decorativas (heartbeat, eventos).
"""

from __future__ import annotations

import socket
from datetime import datetime
from typing import Literal

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from nucleo.conexion import obtener_engine
from nucleo.excepciones import ErrorBaseDatos, ErrorRecursoNoEncontrado
from nucleo.logging import obtener_logger

log = obtener_logger(__name__)

EstadoCorrida  = Literal["PENDIENTE", "EJECUTANDO", "OK", "ERROR", "CANCELADO", "TIMEOUT"]
TipoEvento     = Literal["LOG", "PROGRESO", "ERROR", "FIN"]
TipoComando    = Literal["INICIAR", "CANCELAR", "REINTENTAR"]
EstadoComando  = Literal["PENDIENTE", "PROCESANDO", "PROCESADO", "ERROR_COLA"]


# ═══════════════════════════════════════════════════════════════════════════════
# Control.Corrida
# ═══════════════════════════════════════════════════════════════════════════════

def insertar_corrida(
    id_corrida: str,
    iniciado_por: str,
    comentario: str | None = None,
    max_reintentos: int = 0,
    timeout_segundos: int = 3600,
) -> None:
    """Crea el registro maestro de la corrida en estado PENDIENTE."""
    with obtener_engine().begin() as con:
        con.execute(
            text("""
                INSERT INTO Control.Corrida
                    (ID_Corrida, Iniciado_Por, Comentario, Estado,
                     Max_Reintentos, Timeout_Segundos, Fecha_Solicitud)
                VALUES
                    (:id, :usuario, :comentario, 'PENDIENTE',
                     :max_ret, :timeout, :ahora)
            """),
            {
                "id":         id_corrida,
                "usuario":    iniciado_por,
                "comentario": comentario,
                "max_ret":    max_reintentos,
                "timeout":    timeout_segundos,
                "ahora":      datetime.now(),
            },
        )


def obtener_corrida(id_corrida: str) -> dict | None:
    """Retorna el registro maestro de la corrida o None si no existe."""
    try:
        with obtener_engine().connect() as con:
            fila = con.execute(
                text("""
                    SELECT
                        ID_Corrida          AS id_corrida,
                        Iniciado_Por        AS iniciado_por,
                        Comentario          AS comentario,
                        Estado              AS estado,
                        Intento_Numero      AS intento_numero,
                        Max_Reintentos      AS max_reintentos,
                        Fecha_Solicitud     AS fecha_solicitud,
                        Fecha_Inicio        AS fecha_inicio,
                        Fecha_Fin           AS fecha_fin,
                        PID_Runner          AS pid_runner,
                        Heartbeat_Ultimo    AS heartbeat_ultimo,
                        Timeout_Segundos    AS timeout_segundos,
                        Mensaje_Final       AS mensaje_final,
                        ID_Log_Auditoria    AS id_log_auditoria
                    FROM Control.Corrida
                    WHERE ID_Corrida = :id
                """),
                {"id": id_corrida},
            ).fetchone()
            return dict(fila._mapping) if fila else None
    except SQLAlchemyError:
        log.exception("Error al obtener corrida", extra={"id_corrida": id_corrida})
        raise ErrorBaseDatos()


def listar_corridas(limite: int = 50, solo_activas: bool = False) -> list[dict]:
    """Lista las últimas corridas. Con solo_activas=True retorna solo PENDIENTE/EJECUTANDO."""
    filtro = "WHERE Estado IN ('PENDIENTE','EJECUTANDO')" if solo_activas else ""
    try:
        with obtener_engine().connect() as con:
            filas = con.execute(
                text(f"""
                    SELECT TOP (:limite)
                        ID_Corrida, Iniciado_Por, Comentario, Estado,
                        Intento_Numero, Max_Reintentos,
                        Fecha_Solicitud, Fecha_Inicio, Fecha_Fin,
                        Heartbeat_Ultimo, Mensaje_Final, ID_Log_Auditoria
                    FROM Control.Corrida
                    {filtro}
                    ORDER BY Fecha_Solicitud DESC
                """),
                {"limite": limite},
            ).fetchall()
            return [dict(f._mapping) for f in filas]
    except SQLAlchemyError:
        log.exception("Error al listar corridas")
        raise ErrorBaseDatos()


def actualizar_estado_corrida(
    id_corrida: str,
    estado: EstadoCorrida,
    mensaje_final: str | None = None,
    id_log_auditoria: int | None = None,
    pid_runner: int | None = None,
) -> None:
    """Actualiza el estado de la corrida. Establece Fecha_Inicio o Fecha_Fin según corresponde."""
    sets = ["Estado = :estado"]
    params: dict = {"id": id_corrida, "estado": estado}

    if estado == "EJECUTANDO":
        sets.append("Fecha_Inicio = :ahora")
        sets.append("PID_Runner = :pid")
        params["ahora"] = datetime.now()
        params["pid"]   = pid_runner

    elif estado in ("OK", "ERROR", "CANCELADO", "TIMEOUT"):
        sets.append("Fecha_Fin = :ahora")
        sets.append("PID_Runner = NULL")
        params["ahora"] = datetime.now()

    if mensaje_final is not None:
        sets.append("Mensaje_Final = :msg")
        params["msg"] = mensaje_final[:1000]

    if id_log_auditoria is not None:
        sets.append("ID_Log_Auditoria = :id_log")
        params["id_log"] = id_log_auditoria

    try:
        with obtener_engine().begin() as con:
            con.execute(
                text(f"UPDATE Control.Corrida SET {', '.join(sets)} WHERE ID_Corrida = :id"),
                params,
            )
    except SQLAlchemyError:
        log.exception("Error al actualizar estado de corrida", extra={"id_corrida": id_corrida})


def actualizar_heartbeat_corrida(id_corrida: str, pid: int) -> None:
    """Falla silenciosamente — no interrumpe el runner."""
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("""
                    UPDATE Control.Corrida
                    SET Heartbeat_Ultimo = :ahora, PID_Runner = :pid
                    WHERE ID_Corrida = :id
                """),
                {"ahora": datetime.now(), "pid": pid, "id": id_corrida},
            )
    except SQLAlchemyError:
        log.warning("No se pudo actualizar heartbeat", extra={"id_corrida": id_corrida})


def corrida_fue_cancelada(id_corrida: str) -> bool:
    """Consulta ligera: retorna True si alguien solicitó cancelación."""
    try:
        with obtener_engine().connect() as con:
            fila = con.execute(
                text("""
                    SELECT 1 FROM Control.Corrida
                    WHERE ID_Corrida = :id AND Estado = 'CANCELADO'
                """),
                {"id": id_corrida},
            ).fetchone()
            return fila is not None
    except SQLAlchemyError:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Control.Corrida_Evento
# ═══════════════════════════════════════════════════════════════════════════════

def insertar_evento(
    id_corrida: str,
    mensaje: str,
    tipo: TipoEvento = "LOG",
) -> None:
    """Persiste un evento de la corrida. Falla silenciosamente."""
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("""
                    INSERT INTO Control.Corrida_Evento
                        (ID_Corrida, Tipo, Mensaje, Fecha_Evento)
                    VALUES (:id, :tipo, :msg, :ahora)
                """),
                {
                    "id":   id_corrida,
                    "tipo": tipo,
                    "msg":  mensaje[:4000],
                    "ahora": datetime.now(),
                },
            )
    except SQLAlchemyError:
        log.warning("No se pudo persistir evento", extra={"id_corrida": id_corrida})


def listar_eventos(
    id_corrida: str,
    desde_id: int = 0,
    limite: int = 500,
) -> list[dict]:
    """
    Retorna eventos de la corrida desde `desde_id` (exclusivo).
    Usado por el endpoint SSE para polling incremental.
    """
    try:
        with obtener_engine().connect() as con:
            filas = con.execute(
                text("""
                    SELECT TOP (:limite)
                        ID_Evento    AS id_evento,
                        Tipo         AS tipo,
                        Mensaje      AS mensaje,
                        Fecha_Evento AS fecha_evento
                    FROM Control.Corrida_Evento
                    WHERE ID_Corrida = :id AND ID_Evento > :desde
                    ORDER BY ID_Evento ASC
                """),
                {"id": id_corrida, "desde": desde_id, "limite": limite},
            ).fetchall()
            return [dict(f._mapping) for f in filas]
    except SQLAlchemyError:
        log.exception("Error al listar eventos", extra={"id_corrida": id_corrida})
        return []


def ultimo_id_evento(id_corrida: str) -> int:
    """Retorna el ID máximo de evento de la corrida (0 si no hay ninguno)."""
    try:
        with obtener_engine().connect() as con:
            fila = con.execute(
                text("""
                    SELECT ISNULL(MAX(ID_Evento), 0)
                    FROM Control.Corrida_Evento
                    WHERE ID_Corrida = :id
                """),
                {"id": id_corrida},
            ).fetchone()
            return int(fila[0]) if fila else 0
    except SQLAlchemyError:
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# Control.Corrida_Paso
# ═══════════════════════════════════════════════════════════════════════════════

def insertar_paso(id_corrida: str, nombre_paso: str, orden: int = 1) -> int:
    """Registra un paso del pipeline. Retorna su ID."""
    try:
        with obtener_engine().begin() as con:
            resultado = con.execute(
                text("""
                    INSERT INTO Control.Corrida_Paso
                        (ID_Corrida, Nombre_Paso, Orden, Estado, Fecha_Inicio)
                    OUTPUT INSERTED.ID_Paso
                    VALUES (:id, :nombre, :orden, 'EJECUTANDO', :ahora)
                """),
                {
                    "id": id_corrida,
                    "nombre": nombre_paso,
                    "orden": orden,
                    "ahora": datetime.now(),
                },
            )
            return resultado.fetchone()[0]
    except SQLAlchemyError:
        log.exception("Error al insertar paso")
        return -1


def cerrar_paso(id_paso: int, estado: str, mensaje_error: str | None = None) -> None:
    """Cierra un paso del pipeline con su estado final."""
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("""
                    UPDATE Control.Corrida_Paso
                    SET Estado = :estado, Fecha_Fin = :ahora, Mensaje_Error = :err
                    WHERE ID_Paso = :id
                """),
                {"estado": estado, "ahora": datetime.now(), "err": mensaje_error, "id": id_paso},
            )
    except SQLAlchemyError:
        log.warning("No se pudo cerrar paso", extra={"id_paso": id_paso})


def listar_pasos_corrida(id_corrida: str) -> list[dict]:
    """Retorna la traza de pasos de una corrida en orden operativo."""
    try:
        with obtener_engine().connect() as con:
            filas = con.execute(
                text("""
                    SELECT
                        ID_Paso        AS id_paso,
                        ID_Corrida     AS id_corrida,
                        Nombre_Paso    AS nombre_paso,
                        Orden          AS orden,
                        Estado         AS estado,
                        Fecha_Inicio   AS fecha_inicio,
                        Fecha_Fin      AS fecha_fin,
                        Mensaje_Error  AS mensaje_error
                    FROM Control.Corrida_Paso
                    WHERE ID_Corrida = :id
                    ORDER BY Orden ASC, ID_Paso ASC
                """),
                {"id": id_corrida},
            ).fetchall()
            return [dict(fila._mapping) for fila in filas]
    except SQLAlchemyError:
        log.exception("Error al listar pasos de corrida", extra={"id_corrida": id_corrida})
        raise ErrorBaseDatos()


# ═══════════════════════════════════════════════════════════════════════════════
# Control.Bloqueo_Ejecucion
# ═══════════════════════════════════════════════════════════════════════════════

_HOSTNAME = socket.gethostname()

def adquirir_lock(id_corrida: str, timeout_lock_seg: int = 120) -> bool:
    """
    Intenta adquirir el lock de ejecución.
    Retorna True si tuvo éxito, False si ya hay otro runner activo
    (heartbeat reciente en el último timeout_lock_seg segundos).
    """
    try:
        with obtener_engine().begin() as con:
            rows = con.execute(
                text("""
                    UPDATE Control.Bloqueo_Ejecucion
                    SET
                        ID_Corrida_Activa = :corrida,
                        Adquirido_Por     = :host,
                        Fecha_Adquisicion = :ahora,
                        Heartbeat         = :ahora
                    WHERE ID_Lock = 1
                      AND (
                          ID_Corrida_Activa IS NULL
                          OR DATEDIFF(SECOND, Heartbeat, GETDATE()) > :timeout_lock
                      )
                """),
                {
                    "corrida":      id_corrida,
                    "host":         _HOSTNAME,
                    "ahora":        datetime.now(),
                    "timeout_lock": timeout_lock_seg,
                },
            ).rowcount
            return rows == 1
    except SQLAlchemyError:
        log.exception("Error al adquirir lock")
        return False


def liberar_lock(id_corrida: str) -> None:
    """Libera el lock de ejecución. Falla silenciosamente."""
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("""
                    UPDATE Control.Bloqueo_Ejecucion
                    SET ID_Corrida_Activa = NULL, Adquirido_Por = NULL,
                        Fecha_Adquisicion = NULL, Heartbeat = NULL
                    WHERE ID_Lock = 1 AND ID_Corrida_Activa = :corrida
                """),
                {"corrida": id_corrida},
            )
    except SQLAlchemyError:
        log.warning("No se pudo liberar el lock", extra={"id_corrida": id_corrida})


def actualizar_heartbeat_lock() -> None:
    """Renueva el heartbeat del lock activo. Falla silenciosamente."""
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("UPDATE Control.Bloqueo_Ejecucion SET Heartbeat = GETDATE() WHERE ID_Lock = 1")
            )
    except SQLAlchemyError:
        pass


def lock_activo() -> dict | None:
    """Retorna el estado actual del lock o None si está libre."""
    try:
        with obtener_engine().connect() as con:
            fila = con.execute(
                text("""
                    SELECT ID_Corrida_Activa, Adquirido_Por,
                           Fecha_Adquisicion, Heartbeat
                    FROM Control.Bloqueo_Ejecucion
                    WHERE ID_Lock = 1 AND ID_Corrida_Activa IS NOT NULL
                """)
            ).fetchone()
            return dict(fila._mapping) if fila else None
    except SQLAlchemyError:
        return None


def obtener_estado_lock(timeout_lock_seg: int = 120) -> dict:
    """
    Retorna el estado operativo del lock único del runner.
    Distingue lock libre, activo y heartbeat vencido.
    """
    try:
        with obtener_engine().connect() as con:
            fila = con.execute(
                text("""
                    SELECT
                        ID_Lock                 AS id_lock,
                        ID_Corrida_Activa       AS id_corrida_activa,
                        Adquirido_Por           AS adquirido_por,
                        Fecha_Adquisicion       AS fecha_adquisicion,
                        Heartbeat               AS heartbeat,
                        CASE
                            WHEN ID_Corrida_Activa IS NULL THEN 'LIBRE'
                            WHEN Heartbeat IS NULL THEN 'VENCIDO'
                            WHEN DATEDIFF(SECOND, Heartbeat, GETDATE()) > :timeout_lock THEN 'VENCIDO'
                            ELSE 'ACTIVO'
                        END AS estado_lock,
                        CASE
                            WHEN Heartbeat IS NULL THEN NULL
                            ELSE DATEDIFF(SECOND, Heartbeat, GETDATE())
                        END AS segundos_desde_heartbeat
                    FROM Control.Bloqueo_Ejecucion
                    WHERE ID_Lock = 1
                """),
                {"timeout_lock": timeout_lock_seg},
            ).fetchone()
            if not fila:
                raise ErrorRecursoNoEncontrado()
            return dict(fila._mapping)
    except SQLAlchemyError:
        log.exception("Error al consultar estado del lock")
        raise ErrorBaseDatos()


def obtener_resumen_control_plane() -> dict:
    """
    Retorna contadores operativos del control-plane ETL.
    """
    try:
        with obtener_engine().connect() as con:
            fila = con.execute(
                text("""
                    SELECT
                        (SELECT COUNT(*) FROM Control.Corrida
                         WHERE Estado IN ('PENDIENTE', 'EJECUTANDO')) AS corridas_activas,
                        (SELECT COUNT(*) FROM Control.Comando_Ejecucion
                         WHERE Estado_Cmd = 'PENDIENTE') AS comandos_pendientes,
                        (SELECT COUNT(*) FROM Control.Comando_Ejecucion
                         WHERE Estado_Cmd = 'PROCESANDO') AS comandos_procesando
                """)
            ).fetchone()
            if not fila:
                return {
                    "corridas_activas": 0,
                    "comandos_pendientes": 0,
                    "comandos_procesando": 0,
                }
            return dict(fila._mapping)
    except SQLAlchemyError:
        log.exception("Error al consultar resumen del control-plane")
        raise ErrorBaseDatos()


# ═══════════════════════════════════════════════════════════════════════════════
# Control.Comando_Ejecucion
# ═══════════════════════════════════════════════════════════════════════════════

def encolar_comando(
    id_corrida: str,
    iniciado_por: str,
    tipo_comando: TipoComando = "INICIAR",
    comentario: str | None = None,
    max_reintentos: int = 0,
    timeout_seg: int = 3600,
) -> int:
    """Inserta el comando en la cola. Retorna el ID generado."""
    try:
        with obtener_engine().begin() as con:
            resultado = con.execute(
                text("""
                    INSERT INTO Control.Comando_Ejecucion
                        (ID_Corrida, Tipo_Comando, Iniciado_Por, Comentario,
                         Max_Reintentos, Timeout_Seg, Estado_Cmd, Fecha_Comando)
                    OUTPUT INSERTED.ID_Comando
                    VALUES
                        (:corrida, :tipo, :usuario, :comentario,
                         :max_ret, :timeout, 'PENDIENTE', :ahora)
                """),
                {
                    "corrida":    id_corrida,
                    "tipo":       tipo_comando,
                    "usuario":    iniciado_por,
                    "comentario": comentario,
                    "max_ret":    max_reintentos,
                    "timeout":    timeout_seg,
                    "ahora":      datetime.now(),
                },
            )
            return resultado.fetchone()[0]
    except SQLAlchemyError:
        log.exception("Error al encolar comando")
        raise ErrorBaseDatos()


def tomar_comando_pendiente() -> dict | None:
    """
    Toma ATÓMICAMENTE el comando más antiguo en estado PENDIENTE.
    Lo marca como PROCESANDO para evitar que otro runner lo tome.
    Consume comandos INICIAR y REINTENTAR en orden FIFO por fecha/id.
    Retorna None si no hay comandos pendientes.
    """
    try:
        with obtener_engine().begin() as con:
            fila = con.execute(
                text("""
                    ;WITH siguiente AS (
                        SELECT TOP (1)
                            ID_Comando
                        FROM Control.Comando_Ejecucion WITH (UPDLOCK, READPAST, ROWLOCK)
                        WHERE Estado_Cmd = 'PENDIENTE'
                          AND Tipo_Comando IN ('INICIAR', 'REINTENTAR')
                        ORDER BY Fecha_Comando ASC, ID_Comando ASC
                    )
                    UPDATE ce
                    SET Estado_Cmd = 'PROCESANDO', Fecha_Proceso = GETDATE()
                    OUTPUT
                        INSERTED.ID_Comando,
                        INSERTED.ID_Corrida,
                        INSERTED.Tipo_Comando,
                        INSERTED.Iniciado_Por,
                        INSERTED.Comentario,
                        INSERTED.Max_Reintentos,
                        INSERTED.Timeout_Seg
                    FROM Control.Comando_Ejecucion ce
                    INNER JOIN siguiente
                        ON siguiente.ID_Comando = ce.ID_Comando
                """)
            ).fetchone()
            return dict(fila._mapping) if fila else None
    except SQLAlchemyError:
        log.exception("Error al tomar comando pendiente")
        return None


def marcar_comando(id_comando: int, estado: EstadoComando, mensaje: str | None = None) -> None:
    """Actualiza el estado final del comando. Falla silenciosamente."""
    try:
        with obtener_engine().begin() as con:
            con.execute(
                text("""
                    UPDATE Control.Comando_Ejecucion
                    SET Estado_Cmd = :estado, Mensaje_Error = :msg
                    WHERE ID_Comando = :id
                """),
                {"estado": estado, "msg": mensaje, "id": id_comando},
            )
    except SQLAlchemyError:
        log.warning("No se pudo marcar comando", extra={"id_comando": id_comando})


def solicitar_cancelacion(id_corrida: str, solicitado_por: str) -> bool:
    """
    Pide cancelar una corrida activa marcando su estado como CANCELADO.
    El runner detecta esto en su próximo ciclo de heartbeat.
    Retorna True si la corrida estaba activa (PENDIENTE o EJECUTANDO).
    """
    try:
        with obtener_engine().begin() as con:
            rows = con.execute(
                text("""
                    UPDATE Control.Corrida
                    SET Estado = 'CANCELADO',
                        Mensaje_Final = :msg,
                        Fecha_Fin = GETDATE()
                    WHERE ID_Corrida = :id
                      AND Estado IN ('PENDIENTE', 'EJECUTANDO')
                """),
                {
                    "id":  id_corrida,
                    "msg": f"Cancelado por {solicitado_por}",
                },
            ).rowcount
            return rows > 0
    except SQLAlchemyError:
        log.exception("Error al solicitar cancelación")
        return False

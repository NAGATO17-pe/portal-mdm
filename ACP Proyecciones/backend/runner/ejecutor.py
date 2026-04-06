"""
runner/ejecutor.py
==================
Ejecutor de un subproceso pipeline.py para una corrida específica.

Responsabilidades:
  - Lanzar pipeline.py como subprocess
  - Capturar stdout/stderr línea a línea
  - Persistir cada línea en Control.Corrida_Evento (no en memoria)
  - Publicar heartbeats periódicos al lock y a la corrida
  - Detectar cancelación en cada ciclo de heartbeat
  - Registrar inicio y fin en Auditoria.Log_Carga
  - Retornar el estado final (OK | ERROR | CANCELADO | TIMEOUT)
"""

from __future__ import annotations

import os
import sys
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Literal

from nucleo.auditoria import registrar_inicio_corrida, registrar_fin_corrida
from nucleo.logging import obtener_logger
import repositorios.repo_control as rc

log = obtener_logger(__name__)

EstadoFinal = Literal["OK", "ERROR", "CANCELADO", "TIMEOUT"]

_DIR_ETL = Path(__file__).resolve().parents[2] / "ETL"
_SCRIPT  = _DIR_ETL / "pipeline.py"


def _resolver_python() -> str:
    """Devuelve el ejecutable Python del mismo entorno virtual."""
    exe = sys.executable
    if "uvicorn" in exe.lower():
        return str(Path(exe).parent / "python.exe")
    return exe


def ejecutar_corrida(
    id_corrida: str,
    iniciado_por: str,
    timeout_segundos: int = 3600,
    heartbeat_intervalo_seg: int = 30,
) -> EstadoFinal:
    """
    Ejecuta pipeline.py para la corrida dada.

    Flujo:
        1. Registra el inicio en Auditoria.Log_Carga → obtiene id_log
        2. Actualiza Control.Corrida a EJECUTANDO
        3. Lanza subprocess con timeout de watchdog
        4. Lee stdout línea a línea → INSERT Control.Corrida_Evento
        5. Cada heartbeat_intervalo_seg verifica cancelación
        6. Al terminar: actualiza Control.Corrida y Auditoria.Log_Carga

    Retorna el estado final.
    """
    pid = os.getpid()
    estado_final: EstadoFinal = "ERROR"
    codigo_retorno = -1
    id_log: int | None = None

    # ── 1. Auditoría de inicio ─────────────────────────────────────────────────
    try:
        id_log = registrar_inicio_corrida(
            nombre_proceso="ETL_RUNNER",
            tabla_destino="PIPELINE_COMPLETO",
            nombre_archivo=f"corrida_{id_corrida[:8]}",
        )
    except Exception:
        log.warning("No se pudo registrar inicio en auditoría", extra={"id_corrida": id_corrida})

    # ── 2. Marcar corrida como EJECUTANDO ─────────────────────────────────────
    rc.actualizar_estado_corrida(
        id_corrida=id_corrida,
        estado="EJECUTANDO",
        pid_runner=pid,
        id_log_auditoria=id_log,
    )
    id_paso = rc.insertar_paso(id_corrida, "PIPELINE_COMPLETO", orden=1)
    rc.insertar_evento(id_corrida, f"[RUNNER] Inicio. PID={pid}", tipo="LOG")

    # ── 3. Lanzar subprocess ──────────────────────────────────────────────────
    proceso: subprocess.Popen | None = None
    cancelado_por_heartbeat = False

    try:
        proceso = subprocess.Popen(
            [_resolver_python(), str(_SCRIPT)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(_DIR_ETL),
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        # ── 4. Leer stdout + heartbeat en thread paralelo ──────────────────────
        ultimo_heartbeat = time.monotonic()
        deadline = time.monotonic() + timeout_segundos

        if proceso.stdout:
            for linea in proceso.stdout:
                ahora = time.monotonic()
                linea_limpia = linea.rstrip("\n")

                rc.insertar_evento(id_corrida, linea_limpia, tipo="LOG")

                # ── 5. Heartbeat y verificación de cancelación ─────────────────
                if ahora - ultimo_heartbeat >= heartbeat_intervalo_seg:
                    rc.actualizar_heartbeat_corrida(id_corrida, pid)
                    rc.actualizar_heartbeat_lock()
                    ultimo_heartbeat = ahora

                    if rc.corrida_fue_cancelada(id_corrida):
                        log.info("[RUNNER] Cancelación detectada, terminando proceso",
                                 extra={"id_corrida": id_corrida})
                        proceso.terminate()
                        cancelado_por_heartbeat = True
                        break

                if ahora > deadline:
                    log.warning("[RUNNER] Timeout alcanzado, terminando proceso",
                                extra={"id_corrida": id_corrida})
                    proceso.terminate()
                    estado_final = "TIMEOUT"
                    break

        proceso.wait(timeout=10)
        codigo_retorno = proceso.returncode

    except subprocess.TimeoutExpired:
        if proceso:
            proceso.kill()
        codigo_retorno = -9
        estado_final = "TIMEOUT"
    except Exception as exc:
        rc.insertar_evento(id_corrida, f"[RUNNER ERROR] {exc}", tipo="ERROR")
        log.exception("[RUNNER] Excepción al ejecutar pipeline", extra={"id_corrida": id_corrida})
        codigo_retorno = -99

    # ── 6. Determinar estado final ────────────────────────────────────────────
    if cancelado_por_heartbeat:
        estado_final = "CANCELADO"
    elif estado_final not in ("TIMEOUT",):
        estado_final = "OK" if codigo_retorno == 0 else "ERROR"

    msg_final = (
        "Pipeline finalizado con éxito."
        if estado_final == "OK"
        else f"Pipeline terminó con estado {estado_final}. Código: {codigo_retorno}."
    )

    # Evento de cierre
    rc.insertar_evento(id_corrida, f"[FIN] {msg_final}", tipo="FIN")

    # Cerrar el paso
    rc.cerrar_paso(
        id_paso,
        estado="OK" if estado_final == "OK" else "ERROR",
        mensaje_error=msg_final if estado_final != "OK" else None,
    )

    # Actualizar Control.Corrida
    rc.actualizar_estado_corrida(
        id_corrida=id_corrida,
        estado=estado_final,
        mensaje_final=msg_final,
    )

    # Actualizar Auditoria.Log_Carga
    if id_log is not None:
        try:
            registrar_fin_corrida(
                id_log=id_log,
                estado="OK" if estado_final == "OK" else "ERROR",
                mensaje_error=msg_final if estado_final != "OK" else None,
            )
        except Exception:
            log.warning("No se pudo actualizar auditoría al finalizar")

    log.info(
        "[RUNNER] Corrida finalizada",
        extra={"id_corrida": id_corrida, "estado": estado_final, "codigo": codigo_retorno},
    )
    return estado_final

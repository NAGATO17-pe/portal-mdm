"""
servicios/servicio_etl.py
=========================
Lógica de negocio para la ejecución del pipeline ETL.
Encapsula el lanzamiento del subproceso, el registro de auditoría
y la alimentación del broker SSE. El router solo llama a iniciar_corrida().
"""

import os
import sys
import uuid
import subprocess
import asyncio
from datetime import datetime

from nucleo.auditoria import registrar_inicio_corrida, registrar_fin_corrida
from broker.broker_sse import registrar_corrida, publicar_linea, finalizar_corrida

# Ruta al script del pipeline (un nivel arriba del backend → ETL/)
_DIR_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "ETL"))
_SCRIPT_PIPELINE = os.path.join(_DIR_BASE, "pipeline.py")

# Resuelve el ejecutable de Python del entorno virtual
def _resolver_python() -> str:
    """
    Devuelve la ruta al python.exe del entorno virtual,
    corrigiendo el caso en que sys.executable apunte a uvicorn.exe.
    """
    ejecutable = sys.executable
    if "uvicorn" in ejecutable.lower():
        return os.path.join(os.path.dirname(ejecutable), "python.exe")
    return ejecutable


def _ejecutar_pipeline_en_hilo(id_corrida: str, id_log: int | None) -> None:
    """
    Función SÍNCRONA que corre en un thread separado (asyncio.to_thread).
    Lanza el subproceso, lee stdout línea a línea y publica al broker SSE.
    Al terminar, registra el resultado en la auditoría y publica el sentinel.
    """
    proceso = None
    codigo_retorno = -1

    try:
        python = _resolver_python()
        proceso = subprocess.Popen(
            [python, _SCRIPT_PIPELINE],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=_DIR_BASE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        if proceso.stdout is not None:
            for linea in proceso.stdout:
                publicar_linea(id_corrida, linea)

        proceso.wait()
        codigo_retorno = proceso.returncode
    except Exception as error:
        publicar_linea(id_corrida, f"--- ERROR AL LANZAR PIPELINE: {error} ---")
    finally:
        estado_final = "OK" if codigo_retorno == 0 else "ERROR"
        mensaje_error = None if codigo_retorno == 0 else f"Pipeline terminó con código {codigo_retorno}"

        if id_log is not None:
            registrar_fin_corrida(
                id_log=id_log,
                estado=estado_final,
                mensaje_error=mensaje_error,
            )

        resumen = (
            "--- PIPELINE FINALIZADO CON ÉXITO ---"
            if codigo_retorno == 0
            else f"--- PIPELINE FINALIZÓ CON ERROR (código: {codigo_retorno}) ---"
        )
        publicar_linea(id_corrida, resumen)
        finalizar_corrida(id_corrida)


async def iniciar_corrida(iniciado_por: str, comentario: str | None) -> dict:
    """
    Punto de entrada del servicio. Retorna metadatos de la corrida
    para que el router construya la respuesta al cliente.

    1. Genera el id_corrida (UUID).
    2. Registra el inicio en Auditoria.Log_Carga.
    3. Registra la cola SSE en el broker.
    4. Lanza el subproceso en un thread (no bloquea el event loop).
    5. Retorna los datos para la respuesta HTTP inmediata.
    """
    id_corrida = str(uuid.uuid4())

    # Registro de auditoría antes de arrancar
    id_log = registrar_inicio_corrida(
        nombre_proceso="API_ETL_PIPELINE",
        tabla_destino="PIPELINE_COMPLETO",
        nombre_archivo=f"corrida_{id_corrida[:8]}",
    )

    # Crear la cola en el broker SSE
    registrar_corrida(id_corrida)

    # Lanzar subproceso sin bloquear: asyncio.to_thread lo manda a un ThreadPool
    asyncio.get_running_loop().run_in_executor(
        None,
        _ejecutar_pipeline_en_hilo,
        id_corrida,
        id_log,
    )

    return {
        "id_corrida":   id_corrida,
        "id_log":       id_log,
        "iniciado_por": iniciado_por,
        "fecha_inicio": datetime.now(),
    }

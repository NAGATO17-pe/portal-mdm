import asyncio
import sys
import os
from typing import AsyncGenerator

async def run_pipeline_and_stream() -> AsyncGenerator[str, None]:
    """
    Ejecuta el script ETL/pipeline.py como un subproceso asíncrono,
    capturando la salida estándar (stdout) y enviándola línea por línea.
    """
    # Determinamos la ruta base (D:\Proyecto2026\ACP_DWH\ACP Proyecciones)
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    base_dir = os.path.dirname(backend_dir)
    etl_dir = os.path.join(base_dir, "ETL")
    pipeline_script = os.path.join(etl_dir, "pipeline.py")

    if not os.path.exists(pipeline_script):
        yield f"ERROR: No se encontró el script de pipeline en {pipeline_script}\n"
        return

    # Invocamos el intérprete de Python actual (o corregimos si estamos corriendo desde uvicorn.exe)
    python_exe = sys.executable
    if "uvicorn.exe" in python_exe.lower():
        python_exe = os.path.join(os.path.dirname(python_exe), "python.exe")
    # Usamos subprocess.Popen nativo para evitar los bugs del EventLoop de Uvicorn en Windows
    import subprocess
    process = subprocess.Popen(
        [python_exe, pipeline_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=etl_dir,
        text=True,
        encoding='utf-8',
        errors='replace'
    )

    def read_line():
        return process.stdout.readline()

    # Leemos la salida línea a línea asíncronamente
    while True:
        line = await asyncio.to_thread(read_line)
        if not line:
            break
        yield line

    # Esperamos a que finalice el proceso
    await asyncio.to_thread(process.wait)
    
    # Emitimos el estado final
    if process.returncode == 0:
        yield "\n--- PIPELINE FINALIZADO CON ÉXITO ---\n"
    else:
        yield f"\n--- PIPELINE FINALIZÓ CON ERROR (Código: {process.returncode}) ---\n"

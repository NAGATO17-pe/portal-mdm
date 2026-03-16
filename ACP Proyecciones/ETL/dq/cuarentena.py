"""
cuarentena.py
=============
Envía filas rechazadas por DQ a MDM.Cuarentena.
Toda fila que no pasa validación llega aquí con su motivo.
"""

from datetime import datetime
from sqlalchemy.engine import Engine
from sqlalchemy import text


def enviar_a_cuarentena(engine: Engine,
                         tabla_origen: str,
                         filas: list[dict]) -> int:
    """
    Inserta filas rechazadas en MDM.Cuarentena.
    Retorna el número de filas enviadas.
    """
    if not filas:
        return 0

    ahora = datetime.now()

    with engine.begin() as conexion:
        for fila in filas:
            conexion.execute(text("""
                INSERT INTO MDM.Cuarentena (
                    Tabla_Origen,
                    Columna_Error,
                    Valor_Raw,
                    Motivo_Rechazo,
                    Severidad,
                    Revisado,
                    Fecha_Sistema
                ) VALUES (
                    :tabla_origen,
                    :columna_error,
                    :valor_raw,
                    :motivo_rechazo,
                    :severidad,
                    0,
                    :fecha_sistema
                )
            """), {
                'tabla_origen':   tabla_origen,
                'columna_error':  fila.get('columna',  'DESCONOCIDA'),
                'valor_raw':      str(fila.get('valor', '')),
                'motivo_rechazo': fila.get('motivo',   'Sin motivo'),
                'severidad':      fila.get('severidad','ALTO'),
                'fecha_sistema':  ahora,
            })

    return len(filas)

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

    Columnas DDL v2:
      Campo_Origen    (no Columna_Error)
      Valor_Recibido  (no Valor_Raw)
      Motivo          (no Motivo_Rechazo)
      Estado          (no Revisado — BIT)
    """
    if not filas:
        return 0

    ahora = datetime.now()
    payload = [{
        'tabla_origen':   tabla_origen,
        'campo_origen':   fila.get('columna', 'DESCONOCIDA'),
        'valor_recibido': str(fila.get('valor', '')),
        'motivo':         fila.get('motivo', 'Sin motivo'),
        'tipo_regla':     fila.get('tipo_regla', 'DQ'),
        'score':          fila.get('score_levenshtein', None),
        'fecha_ingreso':  ahora,
    } for fila in filas]

    sentencia = text("""
        INSERT INTO MDM.Cuarentena (
            Tabla_Origen,
            Campo_Origen,
            Valor_Recibido,
            Motivo,
            Tipo_Regla,
            Score_Levenshtein,
            Estado,
            Fecha_Ingreso
        ) VALUES (
            :tabla_origen,
            :campo_origen,
            :valor_recibido,
            :motivo,
            :tipo_regla,
            :score,
            'PENDIENTE',
            :fecha_ingreso
        )
    """)

    tam_lote = 2000
    with engine.begin() as conexion:
        for inicio in range(0, len(payload), tam_lote):
            conexion.execute(sentencia, payload[inicio:inicio + tam_lote])

    return len(filas)

"""
cuarentena.py
=============
Envía filas rechazadas por DQ a MDM.Cuarentena.
Toda fila que no pasa validación llega aquí con su motivo.
"""

from datetime import datetime
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.sql_lotes import ejecutar_en_lotes_con_engine


def enviar_a_cuarentena(
    engine: Engine,
    tabla_origen: str,
    filas: list[dict],
) -> int:
    """
    Inserta filas rechazadas en MDM.Cuarentena.
    Retorna el número de filas enviadas.

    Compatibilidad:
    - Si una fila no trae id_registro_origen, se inserta NULL.
    """
    if not filas:
        return 0

    ahora = datetime.now()
    payload = [
        {
            'tabla_origen': tabla_origen,
            'campo_origen': fila.get('columna', 'DESCONOCIDA'),
            'valor_recibido': str(fila.get('valor', '')),
            'motivo': fila.get('motivo', 'Sin motivo'),
            'tipo_regla': fila.get('tipo_regla', 'DQ'),
            'score': fila.get('score_levenshtein', None),
            'id_registro_origen': fila.get('id_registro_origen', None),
            'fecha_ingreso': ahora,
        }
        for fila in filas
    ]

    sentencia = text("""
        INSERT INTO MDM.Cuarentena (
            Tabla_Origen,
            Campo_Origen,
            Valor_Recibido,
            Motivo,
            Tipo_Regla,
            Score_Levenshtein,
            Estado,
            ID_Registro_Origen,
            Fecha_Ingreso
        ) VALUES (
            :tabla_origen,
            :campo_origen,
            :valor_recibido,
            :motivo,
            :tipo_regla,
            :score,
            'PENDIENTE',
            :id_registro_origen,
            :fecha_ingreso
        )
    """)

    ejecutar_en_lotes_con_engine(engine, sentencia, payload)
    return len(filas)

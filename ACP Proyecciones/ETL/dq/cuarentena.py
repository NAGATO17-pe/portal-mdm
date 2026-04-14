"""
cuarentena.py
=============
Envía filas rechazadas por DQ a MDM.Cuarentena.
Toda fila que no pasa validación llega aquí con su motivo.
"""

from datetime import datetime
from sqlalchemy import text

from utils.contexto_transaccional import RecursoDB, administrar_recurso_db
from utils.sql_lotes import TAM_LOTE_DEFECTO


def _normalizar_payload_cuarentena(tabla_origen: str, fila: dict, fecha_ingreso: datetime) -> dict:
    return {
        'tabla_origen': tabla_origen,
        'campo_origen': str(fila.get('columna', 'DESCONOCIDA') or 'DESCONOCIDA'),
        'valor_recibido': str(fila.get('valor', '')),
        'motivo': str(fila.get('motivo', 'Sin motivo') or 'Sin motivo'),
        'tipo_regla': str(fila.get('tipo_regla', 'DQ') or 'DQ'),
        'score': fila.get('score_levenshtein', None),
        'id_registro_origen': fila.get('id_registro_origen', None),
        'fecha_ingreso': fecha_ingreso,
    }


def _clave_dedupe_pendiente(payload: dict) -> tuple:
    return (
        payload['tabla_origen'],
        payload['campo_origen'],
        payload['valor_recibido'],
        payload['motivo'],
        payload['id_registro_origen'],
    )


def _deduplicar_payload_pendiente(payload: list[dict]) -> list[dict]:
    deduplicados = []
    vistos = set()
    for fila in payload:
        clave = _clave_dedupe_pendiente(fila)
        if clave in vistos:
            continue
        vistos.add(clave)
        deduplicados.append(fila)
    return deduplicados


def enviar_a_cuarentena(
    recurso_db: RecursoDB,
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
    payload = _deduplicar_payload_pendiente([
        _normalizar_payload_cuarentena(tabla_origen, fila, ahora)
        for fila in filas
    ])

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
        )
        SELECT
            :tabla_origen,
            :campo_origen,
            :valor_recibido,
            :motivo,
            :tipo_regla,
            :score,
            'PENDIENTE',
            :id_registro_origen,
            :fecha_ingreso
        WHERE NOT EXISTS (
            SELECT 1
            FROM MDM.Cuarentena q
            WHERE q.Tabla_Origen = :tabla_origen
              AND q.Campo_Origen = :campo_origen
              AND q.Valor_Recibido = :valor_recibido
              AND q.Motivo = :motivo
              AND q.Estado = 'PENDIENTE'
              AND (
                    q.ID_Registro_Origen = :id_registro_origen
                    OR (q.ID_Registro_Origen IS NULL AND :id_registro_origen IS NULL)
              )
        )
    """)

    insertadas = 0
    with administrar_recurso_db(recurso_db) as conexion:
        for inicio in range(0, len(payload), TAM_LOTE_DEFECTO):
            resultado = conexion.execute(
                sentencia,
                payload[inicio:inicio + TAM_LOTE_DEFECTO],
            )
            insertadas += int(resultado.rowcount or 0)
    return insertadas

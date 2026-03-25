"""
sql_lotes.py
============
Utilidades reutilizables para ejecutar sentencias SQL en lotes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.elements import TextClause


TAM_LOTE_DEFECTO = 2000


def _normalizar_sentencia(sentencia: str | TextClause) -> TextClause:
    return text(sentencia) if isinstance(sentencia, str) else sentencia


def ejecutar_en_lotes(
    conexion: Connection,
    sentencia: str | TextClause,
    payload: Sequence[Mapping],
    tam_lote: int = TAM_LOTE_DEFECTO,
) -> int:
    if not payload:
        return 0

    sentencia_sql = _normalizar_sentencia(sentencia)
    for inicio in range(0, len(payload), tam_lote):
        conexion.execute(sentencia_sql, list(payload[inicio:inicio + tam_lote]))

    return len(payload)


def ejecutar_en_lotes_con_engine(
    engine: Engine,
    sentencia: str | TextClause,
    payload: Sequence[Mapping],
    tam_lote: int = TAM_LOTE_DEFECTO,
) -> int:
    if not payload:
        return 0

    with engine.begin() as conexion:
        return ejecutar_en_lotes(conexion, sentencia, payload, tam_lote)


def marcar_estado_carga_por_ids(
    engine: Engine,
    tabla_origen: str,
    columna_id: str,
    ids: Sequence[int | None],
    estado: str = 'PROCESADO',
    tam_lote: int = TAM_LOTE_DEFECTO,
) -> int:
    payload = [
        {'estado_carga': estado, 'id_origen': int(id_origen)}
        for id_origen in ids
        if id_origen is not None
    ]
    if not payload:
        return 0

    sentencia = f"""
        UPDATE {tabla_origen}
        SET Estado_Carga = :estado_carga
        WHERE {columna_id} = :id_origen
    """
    return ejecutar_en_lotes_con_engine(engine, sentencia, payload, tam_lote)

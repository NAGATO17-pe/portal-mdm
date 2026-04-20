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

from utils.contexto_transaccional import RecursoDB, administrar_recurso_db


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


def ejecutar_en_lotes_con_recurso(
    recurso_db: RecursoDB,
    sentencia: str | TextClause,
    payload: Sequence[Mapping],
    tam_lote: int = TAM_LOTE_DEFECTO,
) -> int:
    if not payload:
        return 0

    with administrar_recurso_db(recurso_db) as conexion:
        return ejecutar_en_lotes(conexion, sentencia, payload, tam_lote)


def ejecutar_en_lotes_con_engine(
    engine: Engine,
    sentencia: str | TextClause,
    payload: Sequence[Mapping],
    tam_lote: int = TAM_LOTE_DEFECTO,
) -> int:
    return ejecutar_en_lotes_con_recurso(engine, sentencia, payload, tam_lote)


def marcar_estado_carga_por_ids(
    recurso_db: RecursoDB,
    tabla_origen: str,
    columna_id: str,
    ids: Sequence[int | None],
    estado: str = 'PROCESADO',
    tam_lote: int = TAM_LOTE_DEFECTO,
) -> int:
    ids_limpios = list(set([int(i) for i in ids if i is not None]))
    if not ids_limpios:
        return 0

    with administrar_recurso_db(recurso_db) as conexion:
        nombre_temp = f"#Temp_Update_{tabla_origen.split('.')[-1]}"
        
        conexion.execute(text(f"IF OBJECT_ID('tempdb..{nombre_temp}') IS NOT NULL DROP TABLE {nombre_temp}"))
        conexion.execute(text(f"CREATE TABLE {nombre_temp} (id_origen BIGINT PRIMARY KEY)"))
        
        sql_insert = f"INSERT INTO {nombre_temp} (id_origen) VALUES (?)"
        
        try:
            raw_conn = conexion.connection
            cursor = raw_conn.cursor()
            cursor.fast_executemany = True
            datos = [(i,) for i in ids_limpios]
            cursor.executemany(sql_insert, datos)
            cursor.close()
        except AttributeError:
             # Fallback just in case we are in sqlite mocked engine for some reason
             for inicio in range(0, len(ids_limpios), tam_lote):
                 conexion.execute(
                     text(f"INSERT INTO {nombre_temp} (id_origen) VALUES (:id_origen)"),
                     [{"id_origen": i} for i in ids_limpios[inicio:inicio + tam_lote]]
                 )

        sql_update = text(f"""
            UPDATE orig
            SET Estado_Carga = :estado
            FROM {tabla_origen} orig
            INNER JOIN {nombre_temp} tmp ON orig.{columna_id} = tmp.id_origen
        """)
        resultado = conexion.execute(sql_update, {"estado": estado})
        
        conexion.execute(text(f"IF OBJECT_ID('tempdb..{nombre_temp}') IS NOT NULL DROP TABLE {nombre_temp}"))
        
        return resultado.rowcount

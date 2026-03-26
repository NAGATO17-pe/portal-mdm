"""
fact_fisiologia.py
==================
Carga Silver.Fact_Fisiologia desde Bronce.Fisiologia.

Grain: Geo + Tiempo + Variedad + Tercio
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block, mayusculas
from utils.sql_lotes import marcar_estado_carga_por_ids
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_ORIGEN  = 'Bronce.Fisiologia'
TABLA_DESTINO = 'Silver.Fact_Fisiologia'


def _a_int(valor) -> int | None:
    try:
        return int(float(str(valor)))
    except (ValueError, TypeError):
        return None


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            WITH LoteActual AS (
                SELECT TOP (1)
                    Fecha_Sistema,
                    Nombre_Archivo
                FROM {TABLA_ORIGEN}
                WHERE Estado_Carga = 'CARGADO'
                  AND CAST(Fecha_Sistema AS DATE) = CAST(SYSDATETIME() AS DATE)
                ORDER BY Fecha_Sistema DESC, ID_Fisiologia DESC
            )
            SELECT
                ID_Fisiologia,
                Fecha_Raw, Fundo_Raw, Modulo_Raw,
                Variedad_Raw, Tercio_Raw,
                Hinchadas_Raw, Productivas_Raw,
                Total_Org_Raw, Brote_Raw
            FROM {TABLA_ORIGEN} f
            INNER JOIN LoteActual l
                ON f.Fecha_Sistema = l.Fecha_Sistema
               AND f.Nombre_Archivo = l.Nombre_Archivo
            WHERE f.Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_fact_fisiologia(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return resumen

    df, cuar_var = homologar_columna(
        df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, engine
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_procesados = []

    with engine.begin() as conexion:
        for _, fila in df.iterrows():
            fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not valida:
                resumen['rechazados'] += 1
                continue

            modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            id_geo = obtener_id_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
            id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo or not id_var:
                resumen['rechazados'] += 1
                continue

            # Tercio: normalizar BAJO/MEDIO/ALTO
            tercio_raw = str(fila.get('Tercio_Raw', '')).strip().upper()
            tercio_map = {
                'BAJO': 'BAJO', 'B': 'BAJO', 'LOW': 'BAJO',
                'MEDIO': 'MEDIO', 'M': 'MEDIO', 'MID': 'MEDIO',
                'ALTO': 'ALTO', 'A': 'ALTO', 'HIGH': 'ALTO',
            }
            tercio = tercio_map.get(tercio_raw, tercio_raw or None)

            # Brote_Raw — puede venir como un número único (productivos)
            # o como texto combinado. Se guarda en Brotes_Productivos.
            brotes_prod = _a_int(fila.get('Brote_Raw'))

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Fisiologia (
                    ID_Geografia, ID_Tiempo, ID_Variedad,
                    Tercio, Brotes_Productivos, Brotes_Vegetativos,
                    Hinchadas, Productivas, Total_Organos,
                    Fecha_Evento, Fecha_Sistema, Estado_DQ
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad,
                    :tercio, :brotes_prod, NULL,
                    :hinchadas, :productivas, :total_organos,
                    :fecha_evento, SYSDATETIME(), 'OK'
                )
            """), {
                'id_geo':        id_geo,
                'id_tiempo':     obtener_id_tiempo(fecha),
                'id_variedad':   id_var,
                'tercio':        tercio,
                'brotes_prod':   brotes_prod,
                'hinchadas':     _a_int(fila.get('Hinchadas_Raw')),
                'productivas':   _a_int(fila.get('Productivas_Raw')),
                'total_organos': _a_int(fila.get('Total_Org_Raw')),
                'fecha_evento':  fecha,
            })
            ids_procesados.append(int(fila['ID_Fisiologia']))
            resumen['insertados'] += 1

    marcar_estado_carga_por_ids(
        engine, TABLA_ORIGEN, 'ID_Fisiologia', ids_procesados
    )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

"""
fact_evaluacion_vegetativa.py
==============================
Carga Silver.Fact_Evaluacion_Vegetativa desde Bronce.Evaluacion_Vegetativa.

Grain: Fecha + Geo + DNI
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_ORIGEN  = 'Bronce.Evaluacion_Vegetativa'


def _a_decimal(valor) -> float | None:
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Evaluacion_Vegetativa,
                Fecha_Raw, Fundo_Raw, Modulo_Raw, Variedad_Raw,
                Semanas_Poda_Raw,
                Altura_Raw, TallosBasales_Raw, TallosBasalesNuevos_Raw,
                BrotesGenerales_Raw, BrotesProductivos_Raw, DiametroBrote_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_fact_evaluacion_vegetativa(engine: Engine) -> dict:
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

            try:
                semanas = int(float(str(fila.get('Semanas_Poda_Raw', 0))))
            except (ValueError, TypeError):
                semanas = None

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Evaluacion_Vegetativa (
                    ID_Geografia, ID_Tiempo, ID_Variedad,
                    Semanas_Despues_Poda,
                    Promedio_Altura, Promedio_Tallos_Basales,
                    Promedio_Tallos_Basales_Nuevos,
                    Promedio_Brotes_Generales, Promedio_Brotes_Productivos,
                    Promedio_Diametro_Brote,
                    Fecha_Evento, Fecha_Sistema, Estado_DQ
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad,
                    :semanas,
                    :altura, :tallos_bas,
                    :tallos_nuevos,
                    :brotes_gen, :brotes_prod,
                    :diametro,
                    :fecha_evento, SYSDATETIME(), 'OK'
                )
            """), {
                'id_geo':       id_geo,
                'id_tiempo':    obtener_id_tiempo(fecha, engine),
                'id_variedad':  id_var,
                'semanas':      semanas,
                'altura':       _a_decimal(fila.get('Altura_Raw')),
                'tallos_bas':   _a_decimal(fila.get('TallosBasales_Raw')),
                'tallos_nuevos':_a_decimal(fila.get('TallosBasalesNuevos_Raw')),
                'brotes_gen':   _a_decimal(fila.get('BrotesGenerales_Raw')),
                'brotes_prod':  _a_decimal(fila.get('BrotesProductivos_Raw')),
                'diametro':     _a_decimal(fila.get('DiametroBrote_Raw')),
                'fecha_evento': fecha,
            })
            ids_procesados.append(int(fila['ID_Evaluacion_Vegetativa']))
            resumen['insertados'] += 1

    if ids_procesados:
        with engine.begin() as conexion:
            conexion.execute(
                text(f"UPDATE {TABLA_ORIGEN} SET Estado_Carga = 'PROCESADO' WHERE ID_Evaluacion_Vegetativa IN :ids")
                .bindparams(ids=tuple(ids_procesados))
            )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

"""
fact_sanidad_activo.py
======================
Carga Silver.Fact_Sanidad_Activo desde Bronce.Seguimiento_Errores.

Validación crítica: Total_Plantas >= 1 (evita división por cero en Pct_Mortalidad PERSISTED)
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas  import procesar_fecha, obtener_id_tiempo
from utils.texto   import normalizar_modulo, es_test_block
from dq.validador  import validar_total_plantas
from dq.cuarentena import enviar_a_cuarentena
from mdm.lookup    import obtener_id_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_ORIGEN = 'Bronce.Seguimiento_Errores'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Seguimiento_Errores,
                Fecha_Raw, Fundo_Raw, Modulo_Raw, Variedad_Raw,
                Plantas_Vivas_Raw, Plantas_Muertas_Raw, Total_Plantas_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_fact_sanidad_activo(engine: Engine) -> dict:
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

            # Validación crítica — evita división por cero en PERSISTED
            total, error_total = validar_total_plantas(fila.get('Total_Plantas_Raw'))
            if error_total:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append(error_total)
                continue

            def a_int(v):
                try:
                    return int(float(str(v)))
                except (ValueError, TypeError):
                    return None

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Sanidad_Activo (
                    ID_Geografia, ID_Tiempo, ID_Variedad,
                    Plantas_Vivas, Plantas_Muertas, Total_Plantas,
                    Fecha_Evento, Fecha_Sistema
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad,
                    :vivas, :muertas, :total,
                    :fecha_evento, SYSDATETIME()
                )
            """), {
                'id_geo':      id_geo,
                'id_tiempo':   obtener_id_tiempo(fecha),
                'id_variedad': id_var,
                'vivas':       a_int(fila.get('Plantas_Vivas_Raw')),
                'muertas':     a_int(fila.get('Plantas_Muertas_Raw')),
                'total':       total,
                'fecha_evento':fecha,
            })
            ids_procesados.append(int(fila['ID_Seguimiento_Errores']))
            resumen['insertados'] += 1

    if ids_procesados:
        with engine.begin() as conexion:
            conexion.execute(
                text(f"UPDATE {TABLA_ORIGEN} SET Estado_Carga = 'PROCESADO' WHERE ID_Seguimiento_Errores IN :ids")
                .bindparams(ids=tuple(ids_procesados))
            )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

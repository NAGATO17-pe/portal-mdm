"""
fact_peladas.py
===============
Carga Silver.Fact_Peladas desde Bronce.Peladas.

Grain: Fecha + Geo + Variedad + Punto
Validación crítica: Muestras >= 1 (evita división por cero)
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.dni       import procesar_dni
from utils.sql_lotes import marcar_estado_carga_por_ids
from dq.validador    import validar_muestras
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia, obtener_id_variedad, obtener_id_personal
from mdm.homologador import homologar_columna


TABLA_ORIGEN  = 'Bronce.Peladas'
TABLA_DESTINO = 'Silver.Fact_Peladas'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Peladas,
                Fecha_Raw, Fundo_Raw, Modulo_Raw,
                Variedad_Raw, DNI_Raw,
                Punto_Raw, Muestras_Raw,
                BotonesFlorales_Raw, Flores_Raw,
                BayasPequenas_Raw, BayasGrandes_Raw,
                Fase1_Raw, Fase2_Raw,
                BayasCremas_Raw, BayasMaduras_Raw,
                BayasCosechables_Raw,
                PlantasProductivas_Raw, PlantasNoProductivas_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _a_int(valor, default: int = 0) -> int:
    try:
        return max(0, int(float(str(valor))))
    except (ValueError, TypeError):
        return default


def cargar_fact_peladas(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return resumen
    resumen['leidos'] = len(df)

    df, cuar_var = homologar_columna(
        df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, engine
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_procesados = []

    with engine.begin() as conexion:
        for _, fila in df.iterrows():

            fecha, valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='peladas',
            )
            if not valida:
                resumen['rechazados'] += 1
                continue

            modulo  = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            id_geo  = obtener_id_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
            id_var  = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo or not id_var:
                resumen['rechazados'] += 1
                continue

            dni, _      = procesar_dni(fila.get('DNI_Raw'))
            id_personal = obtener_id_personal(dni, engine)

            # Validación crítica — Muestras >= 1
            muestras, error_muestras = validar_muestras(fila.get('Muestras_Raw'))
            if error_muestras:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append(error_muestras)
                continue

            try:
                punto = int(float(str(fila.get('Punto_Raw', 1))))
            except (ValueError, TypeError):
                punto = 1

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Peladas (
                    ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                    Punto, Muestras,
                    Botones_Florales, Flores,
                    Bayas_Pequenas, Bayas_Grandes,
                    Fase_1, Fase_2,
                    Bayas_Cremas, Bayas_Maduras, Bayas_Cosechables,
                    Plantas_Productivas, Plantas_No_Productivas,
                    Fecha_Evento, Fecha_Sistema, Estado_DQ
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad, :id_personal,
                    :punto, :muestras,
                    :botones, :flores,
                    :pequenas, :grandes,
                    :fase1, :fase2,
                    :cremas, :maduras, :cosechables,
                    :productivas, :no_productivas,
                    :fecha_evento, SYSDATETIME(), 'OK'
                )
            """), {
                'id_geo':          id_geo,
                'id_tiempo':       obtener_id_tiempo(fecha),
                'id_variedad':     id_var,
                'id_personal':     id_personal,
                'punto':           punto,
                'muestras':        muestras,
                'botones':         _a_int(fila.get('BotonesFlorales_Raw')),
                'flores':          _a_int(fila.get('Flores_Raw')),
                'pequenas':        _a_int(fila.get('BayasPequenas_Raw')),
                'grandes':         _a_int(fila.get('BayasGrandes_Raw')),
                'fase1':           _a_int(fila.get('Fase1_Raw')),
                'fase2':           _a_int(fila.get('Fase2_Raw')),
                'cremas':          _a_int(fila.get('BayasCremas_Raw')),
                'maduras':         _a_int(fila.get('BayasMaduras_Raw')),
                'cosechables':     _a_int(fila.get('BayasCosechables_Raw')),
                'productivas':     _a_int(fila.get('PlantasProductivas_Raw')),
                'no_productivas':  _a_int(fila.get('PlantasNoProductivas_Raw')),
                'fecha_evento':    fecha,
            })

            ids_procesados.append(int(fila['ID_Peladas']))
            resumen['insertados'] += 1

    marcar_estado_carga_por_ids(
        engine, TABLA_ORIGEN, 'ID_Peladas', ids_procesados
    )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

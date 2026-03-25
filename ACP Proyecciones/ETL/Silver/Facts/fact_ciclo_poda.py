"""
fact_ciclo_poda.py
==================
Carga Silver.Fact_Ciclo_Poda desde:
  - Bronce.Evaluacion_Calidad_Poda
  - Bronce.Ciclos_Fenologicos

Grain: Fecha + Geo + DNI
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block, titulo
from utils.sql_lotes import marcar_estado_carga_por_ids
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_PODA      = 'Bronce.Evaluacion_Calidad_Poda'
TABLA_CICLOS    = 'Bronce.Ciclos_Fenologicos'
TABLA_DESTINO   = 'Silver.Fact_Ciclo_Poda'


def _a_decimal(valor) -> float | None:
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def _leer_bronce_poda(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Evaluacion_Poda,
                ID_Evaluacion_Calidad_Poda,
                Fecha_Raw, Fundo_Raw, Modulo_Raw, Turno_Raw, Valvula_Raw, Variedad_Raw,
                Tipo_Evaluacion_Raw,
                TallosPlanta_Raw, LongitudTallo_Raw, DiametroTallo_Raw,
                RamillaPlanta_Raw, ToconesPlanta_Raw,
                CortesDefectuosos_Raw, AlturaPoda_Raw
            FROM {TABLA_PODA}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _insertar(conexion, campos: dict) -> None:
    conexion.execute(text("""
        INSERT INTO Silver.Fact_Ciclo_Poda (
            ID_Geografia, ID_Tiempo, ID_Variedad,
            Tipo_Evaluacion,
            Promedio_Tallos_Planta, Promedio_Longitud_Tallo,
            Promedio_Diametro_Tallo, Promedio_Ramilla_Planta,
            Promedio_Tocones_Planta, Promedio_Cortes_Defectuosos,
            Promedio_Altura_Poda,
            Fecha_Evento, Fecha_Sistema, Estado_DQ
        ) VALUES (
            :id_geo, :id_tiempo, :id_variedad,
            :tipo_eval,
            :tallos, :longitud,
            :diametro, :ramilla,
            :tocones, :cortes,
            :altura,
            :fecha_evento, SYSDATETIME(), 'OK'
        )
    """), campos)


def cargar_fact_ciclo_poda(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    # ── Evaluacion Calidad Poda ───────────────────────────────
    df_poda = _leer_bronce_poda(engine)
    df_poda, cuar_var = homologar_columna(
        df_poda, 'Variedad_Raw', 'Variedad_Canonica', TABLA_PODA, engine,
        columna_id_origen='ID_Evaluacion_Poda'
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_poda = []
    with engine.begin() as conexion:
        for _, fila in df_poda.iterrows():
            fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not valida:
                resumen['rechazados'] += 1
                continue

            modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            id_geo = obtener_id_geografia(
                fila.get('Fundo_Raw'),
                None,
                modulo,
                engine,
                turno=fila.get('Turno_Raw'),
                valvula=fila.get('Valvula_Raw'),
                cama=None,
            )
            id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo or not id_var:
                resumen['rechazados'] += 1
                continue

            _insertar(conexion, {
                'id_geo':      id_geo,
                'id_tiempo':   obtener_id_tiempo(fecha),
                'id_variedad': id_var,
                'tipo_eval':   titulo(fila.get('Tipo_Evaluacion_Raw')),
                'tallos':      _a_decimal(fila.get('TallosPlanta_Raw')),
                'longitud':    _a_decimal(fila.get('LongitudTallo_Raw')),
                'diametro':    _a_decimal(fila.get('DiametroTallo_Raw')),
                'ramilla':     _a_decimal(fila.get('RamillaPlanta_Raw')),
                'tocones':     _a_decimal(fila.get('ToconesPlanta_Raw')),
                'cortes':      _a_decimal(fila.get('CortesDefectuosos_Raw')),
                'altura':      _a_decimal(fila.get('AlturaPoda_Raw')),
                'fecha_evento':fecha,
            })
            ids_poda.append(int(fila['ID_Evaluacion_Poda']))
            resumen['insertados'] += 1

    marcar_estado_carga_por_ids(
        engine, TABLA_PODA, 'ID_Evaluacion_Poda', ids_poda
    )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_PODA, resumen['cuarentena'])

    return resumen

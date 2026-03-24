"""
fact_telemetria_clima.py
========================
Carga Silver.Fact_Telemetria_Clima desde:
  - Bronce.Reporte_Clima
  - Bronce.Variables_Meteorologicas

Grain: Fecha + Hora + Geografía
Transformación crítica: Humedad proporción (0-1) → porcentaje (0-100)
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo
from dq.validador    import normalizar_humedad
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia


TABLA_CLIMA       = 'Bronce.Reporte_Clima'
TABLA_VARIABLES   = 'Bronce.Variables_Meteorologicas'
TABLA_DESTINO     = 'Silver.Fact_Telemetria_Clima'


def _leer_bronce_clima(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Reporte_Clima,
                Fecha_Raw, Hora_Raw, Sector_Raw,
                TempMax_Raw, TempMin_Raw,
                Humedad_Raw, Precipitacion_Raw
            FROM {TABLA_CLIMA}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _leer_bronce_variables(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Variables_Meteorologicas,
                Fecha_Raw, Sector_Raw,
                VPD_Raw, Radiacion_Raw,
                TempMax_Raw, TempMin_Raw, Humedad_Raw
            FROM {TABLA_VARIABLES}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _a_decimal(valor) -> float | None:
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def _marcar_procesado(engine: Engine,
                       tabla: str, col_id: str,
                       ids: list[int]) -> None:
    if not ids:
        return
    with engine.begin() as conexion:
        conexion.execute(
            text(f"UPDATE {tabla} SET Estado_Carga = 'PROCESADO' WHERE {col_id} IN :ids")
            .bindparams(ids=tuple(ids))
        )


def cargar_fact_telemetria_clima(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    # ── Reporte Clima ─────────────────────────────────────────
    df_clima = _leer_bronce_clima(engine)
    ids_clima = []

    with engine.begin() as conexion:
        for _, fila in df_clima.iterrows():
            fecha_str = f"{fila.get('Fecha_Raw')} {fila.get('Hora_Raw', '00:00')}"
            fecha, valida = procesar_fecha(fecha_str)
            if not valida:
                resumen['rechazados'] += 1
                continue

            id_geo = obtener_id_geografia(None, fila.get('Sector_Raw'), None, engine)
            if not id_geo:
                resumen['rechazados'] += 1
                continue

            humedad, error_hum = normalizar_humedad(fila.get('Humedad_Raw'))
            if error_hum:
                resumen['cuarentena'].append(error_hum)

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Telemetria_Clima (
                    ID_Geografia, ID_Tiempo,
                    Temperatura_Max_C, Temperatura_Min_C,
                    Humedad_Relativa_Pct, Precipitacion_mm,
                    VPD, Radiacion_Solar,
                    Fecha_Evento, Fecha_Sistema
                ) VALUES (
                    :id_geo, :id_tiempo,
                    :temp_max, :temp_min,
                    :humedad, :precipitacion,
                    NULL, NULL,
                    :fecha_evento, SYSDATETIME()
                )
            """), {
                'id_geo':        id_geo,
                'id_tiempo':     obtener_id_tiempo(fecha),
                'temp_max':      _a_decimal(fila.get('TempMax_Raw')),
                'temp_min':      _a_decimal(fila.get('TempMin_Raw')),
                'humedad':       humedad,
                'precipitacion': _a_decimal(fila.get('Precipitacion_Raw')),
                'fecha_evento':  fecha,
            })
            ids_clima.append(int(fila['ID_Reporte_Clima']))
            resumen['insertados'] += 1

    _marcar_procesado(engine, TABLA_CLIMA, 'ID_Reporte_Clima', ids_clima)

    # ── Variables Meteorológicas (VPD + Radiación) ────────────
    df_vars = _leer_bronce_variables(engine)
    ids_vars = []

    with engine.begin() as conexion:
        for _, fila in df_vars.iterrows():
            fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not valida:
                resumen['rechazados'] += 1
                continue

            id_geo = obtener_id_geografia(None, fila.get('Sector_Raw'), None, engine)
            if not id_geo:
                resumen['rechazados'] += 1
                continue

            humedad, _ = normalizar_humedad(fila.get('Humedad_Raw'))

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Telemetria_Clima (
                    ID_Geografia, ID_Tiempo,
                    Temperatura_Max_C, Temperatura_Min_C,
                    Humedad_Relativa_Pct, Precipitacion_mm,
                    VPD, Radiacion_Solar,
                    Fecha_Evento, Fecha_Sistema
                ) VALUES (
                    :id_geo, :id_tiempo,
                    :temp_max, :temp_min,
                    :humedad, NULL,
                    :vpd, :radiacion,
                    :fecha_evento, SYSDATETIME()
                )
            """), {
                'id_geo':       id_geo,
                'id_tiempo':    obtener_id_tiempo(fecha),
                'temp_max':     _a_decimal(fila.get('TempMax_Raw')),
                'temp_min':     _a_decimal(fila.get('TempMin_Raw')),
                'humedad':      humedad,
                'vpd':          _a_decimal(fila.get('VPD_Raw')),
                'radiacion':    _a_decimal(fila.get('Radiacion_Raw')),
                'fecha_evento': fecha,
            })
            ids_vars.append(int(fila['ID_Variables_Meteorologicas']))
            resumen['insertados'] += 1

    _marcar_procesado(engine, TABLA_VARIABLES, 'ID_Variables_Meteorologicas', ids_vars)

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_CLIMA, resumen['cuarentena'])

    return resumen

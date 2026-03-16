"""
fact_conteo_fenologico.py
=========================
Carga Silver.Fact_Conteo_Fenologico desde Bronce.Conteo_Fruta.

Grain: Fecha + Geo + Variedad + Cinta + Estado
FKs obligatorias: ID_Tiempo, ID_Geografia, ID_Variedad, ID_Cinta, ID_Estado_Fenologico
FK nullable: ID_Personal (-1 si sin evaluador)
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas import procesar_fecha, obtener_id_tiempo
from utils.texto  import normalizar_variedad, normalizar_modulo, es_test_block
from utils.dni    import procesar_dni
from mdm.lookup   import (
    obtener_id_geografia,
    obtener_id_variedad,
    obtener_id_personal,
    obtener_id_estado_fenologico,
    obtener_id_cinta,
)
from mdm.homologador import homologar_columna
from dq.cuarentena   import enviar_a_cuarentena


TABLA_ORIGEN  = 'Bronce.Conteo_Fruta'
TABLA_DESTINO = 'Silver.Fact_Conteo_Fenologico'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Conteo_Fruta,
                Fecha_Raw,
                Fundo_Raw,
                Sector_Raw,
                Modulo_Raw,
                Turno_Raw,
                Variedad_Raw,
                Evaluador_Raw,
                Color_Cinta_Raw,
                Estado_Raw,
                Cantidad_Organos_Raw,
                Tipo_Evaluacion_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _marcar_procesado(engine: Engine, ids: list[int]) -> None:
    if not ids:
        return
    with engine.begin() as conexion:
        conexion.execute(text(f"""
            UPDATE {TABLA_ORIGEN}
            SET Estado_Carga = 'PROCESADO'
            WHERE ID_Conteo_Fruta IN :ids
        """).bindparams(ids=tuple(ids)))


def cargar_fact_conteo_fenologico(engine: Engine) -> dict:
    """
    Lee Bronce.Conteo_Fruta y carga Silver.Fact_Conteo_Fenologico.
    Retorna resumen de operaciones.
    """
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return resumen

    # Homologar variedades
    df, cuarentenas_var = homologar_columna(
        df, 'Variedad_Raw', 'Variedad_Canonica',
        TABLA_ORIGEN, engine
    )
    resumen['cuarentena'].extend(cuarentenas_var)

    ids_procesados = []

    with engine.begin() as conexion:
        for _, fila in df.iterrows():

            # ── Fecha ─────────────────────────────────────────
            fecha, fecha_valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not fecha_valida:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna':   'Fecha_Raw',
                    'valor':     fila.get('Fecha_Raw'),
                    'motivo':    'Fecha inválida o fuera de campaña',
                    'severidad': 'ALTO',
                })
                continue

            id_tiempo = obtener_id_tiempo(obtener_id_tiempo(fecha), engine)

            # ── Geografía ─────────────────────────────────────
            modulo_raw = fila.get('Modulo_Raw')
            test_block = es_test_block(modulo_raw)
            modulo     = None if test_block else normalizar_modulo(modulo_raw)

            id_geo = obtener_id_geografia(
                fila.get('Fundo_Raw'),
                fila.get('Sector_Raw'),
                modulo,
                engine
            )
            if not id_geo:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna':   'Modulo_Raw',
                    'valor':     modulo_raw,
                    'motivo':    'Geografía no encontrada en Dim_Geografia',
                    'severidad': 'ALTO',
                })
                continue

            # ── Variedad ──────────────────────────────────────
            id_variedad = obtener_id_variedad(
                fila.get('Variedad_Canonica'), engine
            )
            if not id_variedad:
                resumen['rechazados'] += 1
                continue

            # ── Personal ──────────────────────────────────────
            dni, _ = procesar_dni(fila.get('Evaluador_Raw'))
            id_personal = obtener_id_personal(dni, engine)

            # ── Cinta ─────────────────────────────────────────
            id_cinta = obtener_id_cinta(fila.get('Color_Cinta_Raw'), engine)
            if not id_cinta:
                resumen['rechazados'] += 1
                continue

            # ── Estado fenológico ─────────────────────────────
            id_estado = obtener_id_estado_fenologico(
                fila.get('Estado_Raw'), engine
            )
            if not id_estado:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna':   'Estado_Raw',
                    'valor':     fila.get('Estado_Raw'),
                    'motivo':    'Estado fenológico no reconocido',
                    'severidad': 'ALTO',
                })
                continue

            # ── Cantidad órganos ──────────────────────────────
            try:
                cantidad = int(float(str(fila.get('Cantidad_Organos_Raw', 0))))
            except (ValueError, TypeError):
                cantidad = 0

            # ── INSERT ────────────────────────────────────────
            conexion.execute(text("""
                INSERT INTO Silver.Fact_Conteo_Fenologico (
                    ID_Geografia, ID_Tiempo, ID_Variedad,
                    ID_Personal, ID_Cinta, ID_Estado_Fenologico,
                    Cantidad_Organos,
                    Fecha_Evento, Fecha_Sistema, Estado_DQ
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad,
                    :id_personal, :id_cinta, :id_estado,
                    :cantidad,
                    :fecha_evento, SYSDATETIME(), 'OK'
                )
            """), {
                'id_geo':       id_geo,
                'id_tiempo':    id_tiempo,
                'id_variedad':  id_variedad,
                'id_personal':  id_personal,
                'id_cinta':     id_cinta,
                'id_estado':    id_estado,
                'cantidad':     cantidad,
                'fecha_evento': fecha,
            })

            ids_procesados.append(int(fila['ID_Conteo_Fruta']))
            resumen['insertados'] += 1

    # Marcar como procesados en Bronce
    _marcar_procesado(engine, ids_procesados)

    # Enviar cuarentenas
    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

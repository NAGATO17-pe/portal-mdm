"""
fact_evaluacion_pesos.py
========================
Carga Silver.Fact_Evaluacion_Pesos desde Bronce.Evaluacion_Pesos.

Grain: Fecha + Geo + Personal + Variedad
FKs obligatorias: ID_Tiempo, ID_Geografia, ID_Variedad, ID_Personal
Validación crítica: Peso_Baya_g BETWEEN 0.5 AND 8.0
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.dni       import procesar_dni
from dq.validador    import validar_peso_baya
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import (
    obtener_id_geografia,
    obtener_id_variedad,
    obtener_id_personal,
)
from mdm.homologador import homologar_columna


TABLA_ORIGEN  = 'Bronce.Evaluacion_Pesos'
TABLA_DESTINO = 'Silver.Fact_Evaluacion_Pesos'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Evaluacion_Pesos,
                Fecha_Raw,
                Fundo_Raw,
                Modulo_Raw,
                Variedad_Raw,
                DNI_Raw,
                PesoBaya_Raw,
                CantMuestra_Raw
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
            WHERE ID_Evaluacion_Pesos IN :ids
        """).bindparams(ids=tuple(ids)))


def cargar_fact_evaluacion_pesos(engine: Engine) -> dict:
    """
    Lee Bronce.Evaluacion_Pesos y carga Silver.Fact_Evaluacion_Pesos.
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

            id_tiempo = obtener_id_tiempo(fecha, engine)

            # ── Geografía ─────────────────────────────────────
            modulo_raw = fila.get('Modulo_Raw')
            modulo     = None if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
            id_geo     = obtener_id_geografia(
                fila.get('Fundo_Raw'), None, modulo, engine
            )
            if not id_geo:
                resumen['rechazados'] += 1
                continue

            # ── Variedad ──────────────────────────────────────
            id_variedad = obtener_id_variedad(
                fila.get('Variedad_Canonica'), engine
            )
            if not id_variedad:
                resumen['rechazados'] += 1
                continue

            # ── Personal ──────────────────────────────────────
            dni, _ = procesar_dni(fila.get('DNI_Raw'))
            id_personal = obtener_id_personal(dni, engine)

            # ── Peso baya — validación crítica ─────────────────
            peso, error_peso = validar_peso_baya(fila.get('PesoBaya_Raw'))
            if error_peso:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append(error_peso)
                continue

            # ── Cantidad bayas ────────────────────────────────
            try:
                cantidad = int(float(str(fila.get('CantMuestra_Raw', 0))))
            except (ValueError, TypeError):
                cantidad = None

            # ── INSERT ────────────────────────────────────────
            conexion.execute(text("""
                INSERT INTO Silver.Fact_Evaluacion_Pesos (
                    ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                    Peso_Promedio_Baya_g, Cantidad_Bayas_Muestra,
                    Fecha_Evento, Fecha_Sistema, Estado_DQ
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad, :id_personal,
                    :peso, :cantidad,
                    :fecha_evento, SYSDATETIME(), 'OK'
                )
            """), {
                'id_geo':      id_geo,
                'id_tiempo':   id_tiempo,
                'id_variedad': id_variedad,
                'id_personal': id_personal,
                'peso':        peso,
                'cantidad':    cantidad,
                'fecha_evento': fecha,
            })

            ids_procesados.append(int(fila['ID_Evaluacion_Pesos']))
            resumen['insertados'] += 1

    _marcar_procesado(engine, ids_procesados)

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

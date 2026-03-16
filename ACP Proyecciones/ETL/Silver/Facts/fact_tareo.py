"""
fact_tareo.py
=============
Carga Silver.Fact_Tareo desde Bronce.Consolidado_Tareos.

Grain: Fecha + DNI + Actividad + Geo
FKs obligatorias: ID_Tiempo, ID_Personal, ID_Actividad_Operativa, ID_Geografia
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.dni       import procesar_dni
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import (
    obtener_id_geografia,
    obtener_id_personal,
    obtener_id_actividad,
)


TABLA_ORIGEN  = 'Bronce.Consolidado_Tareos'
TABLA_DESTINO = 'Silver.Fact_Tareo'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Consolidado_Tareos,
                Fecha_Raw,
                Fundo_Raw,
                Modulo_Raw,
                DNIResponsable_Raw,
                IDPersonalGeneral_Raw,
                Labor_Raw,
                HorasTrabajadas_Raw,
                IDPlanilla_Raw
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
            WHERE ID_Consolidado_Tareos IN :ids
        """).bindparams(ids=tuple(ids)))


def cargar_fact_tareo(engine: Engine) -> dict:
    """
    Lee Bronce.Consolidado_Tareos y carga Silver.Fact_Tareo.
    """
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return resumen

    ids_procesados = []

    with engine.begin() as conexion:
        for _, fila in df.iterrows():

            # ── Fecha ─────────────────────────────────────────
            fecha, fecha_valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not fecha_valida:
                resumen['rechazados'] += 1
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

            # ── Personal operario ─────────────────────────────
            dni_operario, _ = procesar_dni(fila.get('DNIResponsable_Raw'))
            id_personal     = obtener_id_personal(dni_operario, engine)

            # ── Personal supervisor ───────────────────────────
            dni_supervisor, _ = procesar_dni(fila.get('IDPersonalGeneral_Raw'))
            id_supervisor     = obtener_id_personal(dni_supervisor, engine)
            # Supervisor -1 no tiene sentido — usar NULL
            if id_supervisor == -1:
                id_supervisor = None

            # ── Actividad ─────────────────────────────────────
            id_actividad = obtener_id_actividad(
                fila.get('Labor_Raw'), engine
            )
            if not id_actividad:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna':   'Labor_Raw',
                    'valor':     fila.get('Labor_Raw'),
                    'motivo':    'Actividad no reconocida en Dim_Actividad_Operativa',
                    'severidad': 'ALTO',
                })
                continue

            # ── Horas trabajadas ──────────────────────────────
            try:
                horas = float(str(fila.get('HorasTrabajadas_Raw', 0)).replace(',', '.'))
            except (ValueError, TypeError):
                horas = 0.0

            planilla = str(fila.get('IDPlanilla_Raw', '')) or None

            # ── INSERT ────────────────────────────────────────
            conexion.execute(text("""
                INSERT INTO Silver.Fact_Tareo (
                    ID_Geografia, ID_Tiempo, ID_Personal,
                    ID_Actividad_Operativa, ID_Personal_Supervisor,
                    Horas_Trabajadas, ID_Planilla, Es_Observado_SAP,
                    Fecha_Evento, Fecha_Sistema
                ) VALUES (
                    :id_geo, :id_tiempo, :id_personal,
                    :id_actividad, :id_supervisor,
                    :horas, :planilla, 0,
                    :fecha_evento, SYSDATETIME()
                )
            """), {
                'id_geo':        id_geo,
                'id_tiempo':     id_tiempo,
                'id_personal':   id_personal,
                'id_actividad':  id_actividad,
                'id_supervisor': id_supervisor,
                'horas':         horas,
                'planilla':      planilla,
                'fecha_evento':  fecha,
            })

            ids_procesados.append(int(fila['ID_Consolidado_Tareos']))
            resumen['insertados'] += 1

    _marcar_procesado(engine, ids_procesados)

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

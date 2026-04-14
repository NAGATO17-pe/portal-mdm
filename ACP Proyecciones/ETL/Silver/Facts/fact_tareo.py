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
from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.dni       import procesar_dni
from mdm.lookup      import (
    resolver_geografia,
    obtener_id_personal,
    obtener_id_actividad,
)
from silver.facts._helpers_fact_comunes import (
    finalizar_resumen_fact as _finalizar_resumen_fact,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
)

TABLA_ORIGEN  = 'Bronce.Consolidado_Tareos'
TABLA_DESTINO = 'Silver.Fact_Tareo'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Tareo,
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


def _es_fila_no_operativa(fila) -> bool:
    fecha_raw = str(fila.get('Fecha_Raw') or '').strip()
    supervisor_raw = str(fila.get('IDPersonalGeneral_Raw') or '').strip().upper()
    tokens_fecha_descartar = {'', 'NONE', 'PERSONAS', 'HORAS'}
    tokens_supervisor_descartar = {'AREA:', 'FECHA:', 'TURNO:', 'DIA:', 'DÍA:', 'NOCHE:', 'TOTAL'}
    return fecha_raw.upper() in tokens_fecha_descartar or supervisor_raw in tokens_supervisor_descartar


def cargar_fact_tareo(engine: Engine) -> dict:
    """
    Lee Bronce.Consolidado_Tareos y carga Silver.Fact_Tareo.
    """
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df)

    ids_leidos = []
    ids_insertados = []
    ids_rechazados = []
    ids_descartados = []

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()
        for _, fila in df.iterrows():
            id_origen = None
            try:
                id_origen = int(fila['ID_Tareo'])
                ids_leidos.append(id_origen)
            except (ValueError, TypeError):
                pass

            if _es_fila_no_operativa(fila):
                if id_origen is not None:
                    ids_descartados.append(id_origen)
                continue

            # ── Fecha ─────────────────────────────────────────
            fecha, fecha_valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='tareo',
            )
            if not fecha_valida:
                resumen['rechazados'] += 1
                if id_origen is not None:
                    ids_rechazados.append(id_origen)
                resumen['cuarentena'].append({
                    'columna': 'Fecha_Raw',
                    'valor': fila.get('Fecha_Raw'),
                    'motivo': 'Fecha invalida o fuera de campana',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            id_tiempo = obtener_id_tiempo(fecha)

            # ── Geografía ─────────────────────────────────────
            modulo_raw = fila.get('Modulo_Raw')
            modulo     = None if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
            resultado_geo = resolver_geografia(
                fila.get('Fundo_Raw'),
                None,
                modulo,
                engine,
            )
            id_geo = resultado_geo.get('id_geografia')
            if not id_geo:
                resumen['rechazados'] += 1
                if id_origen is not None:
                    ids_rechazados.append(id_origen)
                resumen['cuarentena'].append({
                    'columna': 'Modulo_Raw',
                    'valor': f"Fundo={fila.get('Fundo_Raw')} | Modulo={fila.get('Modulo_Raw')}",
                    'motivo': _motivo_cuarentena_geografia(resultado_geo),
                    'tipo_regla': 'MDM',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
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
                if id_origen is not None:
                    ids_rechazados.append(id_origen)
                resumen['cuarentena'].append({
                    'columna':   'Labor_Raw',
                    'valor':     fila.get('Labor_Raw'),
                    'motivo':    'Actividad no reconocida en Dim_Actividad_Operativa',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            # ── Horas trabajadas ──────────────────────────────
            try:
                horas = float(str(fila.get('HorasTrabajadas_Raw', 0)).replace(',', '.'))
            except (ValueError, TypeError):
                horas = 0.0

            planilla = str(fila.get('IDPlanilla_Raw', '')) or None

            # ── INSERT ───────────────
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

            if id_origen is not None:
                ids_insertados.append(id_origen)
            resumen['insertados'] += 1

        if ids_insertados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Tareo', ids_insertados)
        if ids_rechazados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Tareo', ids_rechazados, estado='RECHAZADO')
        if ids_descartados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Tareo', ids_descartados, estado='DESCARTADO')

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_ORIGEN, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)

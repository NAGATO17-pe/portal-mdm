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

from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block, titulo
from mdm.lookup      import resolver_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna
from silver.facts._helpers_fact_comunes import (
    finalizar_resumen_fact as _finalizar_resumen_fact,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    registrar_rechazo as _registrar_rechazo,
)


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
    if df_poda.empty:
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df_poda)
    df_poda, cuar_var = homologar_columna(
        df_poda, 'Variedad_Raw', 'Variedad_Canonica', TABLA_PODA, engine,
        columna_id_origen='ID_Evaluacion_Poda'
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_poda = []
    ids_rechazados = []
    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()
        for _, fila in df_poda.iterrows():
            id_origen = int(fila['ID_Evaluacion_Poda'])
            fecha, valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='ciclo_poda',
            )
            if not valida:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha invalida o fuera de campana',
                )
                continue

            modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            resultado_geo = resolver_geografia(
                fila.get('Fundo_Raw'),
                None,
                modulo,
                engine,
                turno=fila.get('Turno_Raw'),
                valvula=fila.get('Valvula_Raw'),
                cama=None,
            )
            id_geo = resultado_geo.get('id_geografia')
            id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Modulo_Raw',
                    valor=(
                        f"Fundo={fila.get('Fundo_Raw')} | Modulo={fila.get('Modulo_Raw')} | "
                        f"Turno={fila.get('Turno_Raw')} | Valvula={fila.get('Valvula_Raw')}"
                    ),
                    motivo=_motivo_cuarentena_geografia(resultado_geo),
                    tipo_regla='MDM',
                )
                continue

            if not id_var:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Variedad_Raw',
                    valor=fila.get('Variedad_Raw'),
                    motivo='Variedad sin match en Dim_Variedad',
                    tipo_regla='MDM',
                )
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
            ids_poda.append(id_origen)
            resumen['insertados'] += 1

        if ids_poda:
            contexto.marcar_estado_carga(TABLA_PODA, 'ID_Evaluacion_Poda', ids_poda)
        if ids_rechazados:
            contexto.marcar_estado_carga(TABLA_PODA, 'ID_Evaluacion_Poda', ids_rechazados, estado='RECHAZADO')

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_PODA, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)

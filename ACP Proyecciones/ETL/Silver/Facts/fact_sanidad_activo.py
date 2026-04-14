"""
fact_sanidad_activo.py
======================
Carga Silver.Fact_Sanidad_Activo desde Bronce.Seguimiento_Errores.

Validación crítica: Total_Plantas >= 1 (evita división por cero en Pct_Mortalidad PERSISTED)
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.fechas  import procesar_fecha, obtener_id_tiempo
from utils.texto   import normalizar_modulo, es_test_block
from dq.validador  import validar_total_plantas
from mdm.lookup    import resolver_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna
from silver.facts._helpers_fact_comunes import (
    finalizar_resumen_fact as _finalizar_resumen_fact,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    registrar_rechazo as _registrar_rechazo,
)


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
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df)

    df, cuar_var = homologar_columna(
        df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, engine
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_procesados = []
    ids_rechazados = []

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()
        for _, fila in df.iterrows():
            id_origen = int(fila['ID_Seguimiento_Errores'])
            fecha, valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='sanidad_activo',
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
            resultado_geo = resolver_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
            id_geo = resultado_geo.get('id_geografia')
            id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Modulo_Raw',
                    valor=f"Fundo={fila.get('Fundo_Raw')} | Modulo={fila.get('Modulo_Raw')}",
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

            # Validación crítica — evita división por cero en PERSISTED
            total, error_total = validar_total_plantas(fila.get('Total_Plantas_Raw'))
            if error_total:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna=error_total.get('columna', 'Total_Plantas_Raw'),
                    valor=error_total.get('valor'),
                    motivo=error_total.get('motivo', 'Total_Plantas invalido'),
                    severidad=error_total.get('severidad', 'ALTO'),
                )
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
            ids_procesados.append(id_origen)
            resumen['insertados'] += 1

        if ids_procesados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Seguimiento_Errores', ids_procesados)
        if ids_rechazados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Seguimiento_Errores', ids_rechazados, estado='RECHAZADO')

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_ORIGEN, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)

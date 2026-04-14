"""
fact_cosecha_sap.py
===================
Carga Silver.Fact_Cosecha_SAP desde Bronce.Reporte_Cosecha y Bronce.Data_SAP.

Grain: Fecha + Geografía + Variedad + Campaña
FKs obligatorias: ID_Tiempo, ID_Geografia, ID_Variedad, ID_Condicion_Cultivo
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from config.parametros import obtener_int
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


TABLA_COSECHA  = 'Bronce.Reporte_Cosecha'
TABLA_SAP      = 'Bronce.Data_SAP'
TABLA_DESTINO  = 'Silver.Fact_Cosecha_SAP'


def _obtener_id_condicion_default() -> int:
    try:
        return obtener_int('ID_CONDICION_CULTIVO_DEFAULT', 1)
    except Exception:
        return 1


def _leer_bronce_cosecha(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Reporte_Cosecha,
                Fecha_Raw,
                Fundo_Raw,
                Modulo_Raw,
                Variedad_Raw,
                KgNeto_Raw,
                Jabas_Raw,
                Lote_Raw,
                Responsable_Raw
            FROM {TABLA_COSECHA}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _leer_bronce_sap(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Data_SAP,
                Fecha_Raw,
                Fundo_Raw,
                Modulo_Raw,
                Variedad_Raw,
                Peso_Bruto_Raw,
                Peso_Neto_Raw,
                Cantidad_Jabas_Raw,
                Lote_Raw,
                Almacen_Raw,
                Doc_Remision_Raw,
                Codigo_Cliente_Raw,
                Responsable_Raw,
                Descripcion_Material_Raw,
                Material_Codigo_Raw,
                Fecha_Recepcion_Raw
            FROM {TABLA_SAP}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _insertar_fila(conexion, campos: dict) -> None:
    conexion.execute(text("""
        INSERT INTO Silver.Fact_Cosecha_SAP (
            ID_Geografia, ID_Tiempo, ID_Variedad, ID_Condicion_Cultivo,
            Kg_Brutos, Kg_Neto_MP, Cantidad_Jabas,
            Lote, Almacen, Doc_Remision, Codigo_Cliente,
            Responsable, Descripcion_Material, Codigo_SAP_Material,
            Fecha_Recepcion, Fecha_Evento, Fecha_Sistema, Estado_DQ
        ) VALUES (
            :id_geo, :id_tiempo, :id_variedad, :id_condicion,
            :kg_brutos, :kg_neto, :jabas,
            :lote, :almacen, :doc_remision, :codigo_cliente,
            :responsable, :descripcion, :codigo_sap,
            :fecha_recepcion, :fecha_evento, SYSDATETIME(), 'OK'
        )
    """), campos)


def cargar_fact_cosecha_sap(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}
    id_condicion_default = _obtener_id_condicion_default()

    df_cosecha = _leer_bronce_cosecha(engine)
    df_sap = _leer_bronce_sap(engine)
    resumen['leidos'] = len(df_cosecha) + len(df_sap)
    if df_cosecha.empty and df_sap.empty:
        return _finalizar_resumen_fact(resumen)

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()

        cuarentena_cosecha = []
        cuarentena_sap = []

        df_cosecha, cuar_var = homologar_columna(
            df_cosecha, 'Variedad_Raw', 'Variedad_Canonica',
            TABLA_COSECHA, conexion
        )
        resumen['cuarentena'].extend(cuar_var)
        cuarentena_cosecha.extend(cuar_var)

        ids_cosecha = []
        ids_cosecha_rechazados = []
        for _, fila in df_cosecha.iterrows():
            id_origen = int(fila['ID_Reporte_Cosecha'])
            fecha, valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='cosecha_sap',
            )
            if not valida:
                _registrar_rechazo(
                    resumen,
                    ids_cosecha_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha invalida o fuera de campana',
                )
                cuarentena_cosecha.append(resumen['cuarentena'][-1])
                continue

            modulo  = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            resultado_geo = resolver_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
            id_geo = resultado_geo.get('id_geografia')
            id_var  = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo:
                _registrar_rechazo(
                    resumen,
                    ids_cosecha_rechazados,
                    id_origen,
                    columna='Modulo_Raw',
                    valor=f"Fundo={fila.get('Fundo_Raw')} | Modulo={fila.get('Modulo_Raw')}",
                    motivo=_motivo_cuarentena_geografia(resultado_geo),
                    tipo_regla='MDM',
                )
                cuarentena_cosecha.append(resumen['cuarentena'][-1])
                continue

            if not id_var:
                _registrar_rechazo(
                    resumen,
                    ids_cosecha_rechazados,
                    id_origen,
                    columna='Variedad_Raw',
                    valor=fila.get('Variedad_Raw'),
                    motivo='Variedad sin match en Dim_Variedad',
                    tipo_regla='MDM',
                )
                cuarentena_cosecha.append(resumen['cuarentena'][-1])
                continue

            try:
                kg_neto = float(str(fila.get('KgNeto_Raw', 0)).replace(',', '.'))
            except (ValueError, TypeError):
                kg_neto = None

            try:
                jabas = int(float(str(fila.get('Jabas_Raw', 0))))
            except (ValueError, TypeError):
                jabas = None

            _insertar_fila(conexion, {
                'id_geo':         id_geo,
                'id_tiempo':      obtener_id_tiempo(fecha),
                'id_variedad':    id_var,
                'id_condicion':   id_condicion_default,
                'kg_brutos':      None,
                'kg_neto':        kg_neto,
                'jabas':          jabas,
                'lote':           fila.get('Lote_Raw'),
                'almacen':        None,
                'doc_remision':   None,
                'codigo_cliente': None,
                'responsable':    titulo(fila.get('Responsable_Raw')),
                'descripcion':    None,
                'codigo_sap':     None,
                'fecha_recepcion':None,
                'fecha_evento':   fecha,
            })
            ids_cosecha.append(id_origen)
            resumen['insertados'] += 1

        if ids_cosecha:
            contexto.marcar_estado_carga(
                TABLA_COSECHA, 'ID_Reporte_Cosecha', ids_cosecha
            )
        if ids_cosecha_rechazados:
            contexto.marcar_estado_carga(
                TABLA_COSECHA, 'ID_Reporte_Cosecha', ids_cosecha_rechazados, estado='RECHAZADO'
            )

        df_sap, cuar_sap = homologar_columna(
            df_sap, 'Variedad_Raw', 'Variedad_Canonica',
            TABLA_SAP, conexion
        )
        resumen['cuarentena'].extend(cuar_sap)
        cuarentena_sap.extend(cuar_sap)

        ids_sap = []
        ids_sap_rechazados = []
        for _, fila in df_sap.iterrows():
            id_origen = int(fila['ID_Data_SAP'])
            fecha, valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='cosecha_sap',
            )
            if not valida:
                _registrar_rechazo(
                    resumen,
                    ids_sap_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha invalida o fuera de campana',
                )
                cuarentena_sap.append(resumen['cuarentena'][-1])
                continue

            modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            resultado_geo = resolver_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
            id_geo = resultado_geo.get('id_geografia')
            id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo:
                _registrar_rechazo(
                    resumen,
                    ids_sap_rechazados,
                    id_origen,
                    columna='Modulo_Raw',
                    valor=f"Fundo={fila.get('Fundo_Raw')} | Modulo={fila.get('Modulo_Raw')}",
                    motivo=_motivo_cuarentena_geografia(resultado_geo),
                    tipo_regla='MDM',
                )
                cuarentena_sap.append(resumen['cuarentena'][-1])
                continue

            if not id_var:
                _registrar_rechazo(
                    resumen,
                    ids_sap_rechazados,
                    id_origen,
                    columna='Variedad_Raw',
                    valor=fila.get('Variedad_Raw'),
                    motivo='Variedad sin match en Dim_Variedad',
                    tipo_regla='MDM',
                )
                cuarentena_sap.append(resumen['cuarentena'][-1])
                continue

            def a_decimal(v):
                try:
                    return float(str(v).replace(',', '.'))
                except (ValueError, TypeError):
                    return None

            fecha_recepcion, _ = procesar_fecha(
                fila.get('Fecha_Recepcion_Raw'),
                dominio='historico',
            )

            _insertar_fila(conexion, {
                'id_geo':         id_geo,
                'id_tiempo':      obtener_id_tiempo(fecha),
                'id_variedad':    id_var,
                'id_condicion':   id_condicion_default,
                'kg_brutos':      a_decimal(fila.get('Peso_Bruto_Raw')),
                'kg_neto':        a_decimal(fila.get('Peso_Neto_Raw')),
                'jabas':          int(float(str(fila.get('Cantidad_Jabas_Raw', 0)))) if fila.get('Cantidad_Jabas_Raw') else None,
                'lote':           fila.get('Lote_Raw'),
                'almacen':        fila.get('Almacen_Raw'),
                'doc_remision':   fila.get('Doc_Remision_Raw'),
                'codigo_cliente': fila.get('Codigo_Cliente_Raw'),
                'responsable':    titulo(fila.get('Responsable_Raw')),
                'descripcion':    fila.get('Descripcion_Material_Raw'),
                'codigo_sap':     fila.get('Material_Codigo_Raw'),
                'fecha_recepcion':fecha_recepcion,
                'fecha_evento':   fecha,
            })
            ids_sap.append(id_origen)
            resumen['insertados'] += 1

        if ids_sap:
            contexto.marcar_estado_carga(
                TABLA_SAP, 'ID_Data_SAP', ids_sap
            )
        if ids_sap_rechazados:
            contexto.marcar_estado_carga(
                TABLA_SAP, 'ID_Data_SAP', ids_sap_rechazados, estado='RECHAZADO'
            )

        if cuarentena_cosecha:
            contexto.enviar_cuarentena(TABLA_COSECHA, cuarentena_cosecha)
        if cuarentena_sap:
            contexto.enviar_cuarentena(TABLA_SAP, cuarentena_sap)

    return _finalizar_resumen_fact(resumen)

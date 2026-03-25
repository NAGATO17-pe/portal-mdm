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

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block, titulo
from utils.sql_lotes import marcar_estado_carga_por_ids
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_COSECHA  = 'Bronce.Reporte_Cosecha'
TABLA_SAP      = 'Bronce.Data_SAP'
TABLA_DESTINO  = 'Silver.Fact_Cosecha_SAP'
ID_CONDICION_DEFAULT = 1  # Suelo + GlobalGAP + Goteo


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

    # ── Reporte Cosecha ───────────────────────────────────────
    df_cosecha = _leer_bronce_cosecha(engine)
    df_cosecha, cuar_var = homologar_columna(
        df_cosecha, 'Variedad_Raw', 'Variedad_Canonica',
        TABLA_COSECHA, engine
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_cosecha = []
    with engine.begin() as conexion:
        for _, fila in df_cosecha.iterrows():
            fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not valida:
                resumen['rechazados'] += 1
                continue

            modulo  = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            id_geo  = obtener_id_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
            id_var  = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo or not id_var:
                resumen['rechazados'] += 1
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
                'id_condicion':   ID_CONDICION_DEFAULT,
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
            ids_cosecha.append(int(fila['ID_Reporte_Cosecha']))
            resumen['insertados'] += 1

    marcar_estado_carga_por_ids(
        engine, TABLA_COSECHA, 'ID_Reporte_Cosecha', ids_cosecha
    )

    # ── Data SAP ──────────────────────────────────────────────
    df_sap = _leer_bronce_sap(engine)
    df_sap, cuar_sap = homologar_columna(
        df_sap, 'Variedad_Raw', 'Variedad_Canonica',
        TABLA_SAP, engine
    )
    resumen['cuarentena'].extend(cuar_sap)

    ids_sap = []
    with engine.begin() as conexion:
        for _, fila in df_sap.iterrows():
            fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not valida:
                resumen['rechazados'] += 1
                continue

            modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
            id_geo = obtener_id_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
            id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo or not id_var:
                resumen['rechazados'] += 1
                continue

            def a_decimal(v):
                try:
                    return float(str(v).replace(',', '.'))
                except (ValueError, TypeError):
                    return None

            fecha_recepcion, _ = procesar_fecha(fila.get('Fecha_Recepcion_Raw'))

            _insertar_fila(conexion, {
                'id_geo':         id_geo,
                'id_tiempo':      obtener_id_tiempo(fecha),
                'id_variedad':    id_var,
                'id_condicion':   ID_CONDICION_DEFAULT,
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
            ids_sap.append(int(fila['ID_Data_SAP']))
            resumen['insertados'] += 1

    marcar_estado_carga_por_ids(
        engine, TABLA_SAP, 'ID_Data_SAP', ids_sap
    )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_COSECHA, resumen['cuarentena'])

    return resumen

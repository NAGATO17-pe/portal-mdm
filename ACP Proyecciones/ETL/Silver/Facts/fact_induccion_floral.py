"""
fact_induccion_floral.py
========================
Carga Silver.Fact_Induccion_Floral desde Bronce.Induccion_Floral.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from dq.cuarentena import enviar_a_cuarentena
from mdm.homologador import homologar_columna
from mdm.lookup import (
    obtener_id_personal,
    obtener_id_tiempo,
    obtener_id_variedad,
    resolver_geografia,
)
from utils.dni import procesar_dni
from utils.fechas import obtener_id_tiempo as construir_id_tiempo, procesar_fecha
from utils.sql_lotes import ejecutar_en_lotes_con_engine, marcar_estado_carga_por_ids
from utils.texto import es_test_block, normalizar_modulo
from silver.facts._helpers_fact_comunes import (
    a_entero_nulo as _a_entero_nulo,
    a_entero_no_negativo as _a_entero_no_negativo,
    texto_nulo as _texto_nulo,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    validar_layout_migrado as _validar_layout_migrado_helper,
)


TABLA_ORIGEN = 'Bronce.Induccion_Floral'
TABLA_DESTINO = 'Silver.Fact_Induccion_Floral'

SQL_INSERT_FACT = text("""
    INSERT INTO Silver.Fact_Induccion_Floral (
        ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
        Tipo_Evaluacion, Codigo_Consumidor,
        Cantidad_Plantas_Por_Cama, Cantidad_Plantas_Con_Induccion,
        Cantidad_Brotes_Con_Induccion, Cantidad_Brotes_Totales,
        Cantidad_Brotes_Con_Flor,
        Pct_Plantas_Con_Induccion, Pct_Brotes_Con_Induccion, Pct_Brotes_Con_Flor,
        Fecha_Evento, Fecha_Sistema, Estado_DQ
    ) VALUES (
        :id_geo, :id_tiempo, :id_variedad, :id_personal,
        :tipo_evaluacion, :codigo_consumidor,
        :plantas_por_cama, :plantas_con_induccion,
        :brotes_con_induccion, :brotes_totales,
        :brotes_con_flor,
        :pct_plantas_induccion, :pct_brotes_induccion, :pct_brotes_flor,
        :fecha_evento, SYSDATETIME(), 'OK'
    )
""")


def _pct(parte: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round((parte / total) * 100.0, 2)


def _validar_layout_migrado(engine: Engine) -> str:
    return _validar_layout_migrado_helper(
        engine,
        tabla_origen=TABLA_ORIGEN,
        tabla_destino=TABLA_DESTINO,
        columna_id='ID_Induccion_Floral',
        columnas_bronce_requeridas={
            'ID_Induccion_Floral',
            'Fecha_Raw',
            'DNI_Raw',
            'Consumidor_Raw',
            'Modulo_Raw',
            'Turno_Raw',
            'Valvula_Raw',
            'Cama_Raw',
            'Descripcion_Raw',
            'PlantasPorCama_Raw',
            'PlantasConInduccion_Raw',
            'BrotesConInduccion_Raw',
            'BrotesTotales_Raw',
            'BrotesConFlor_Raw',
            'Estado_Carga',
        },
        columnas_silver_requeridas={
            'ID_Geografia',
            'ID_Tiempo',
            'ID_Variedad',
            'ID_Personal',
            'Codigo_Consumidor',
            'Cantidad_Plantas_Por_Cama',
            'Cantidad_Plantas_Con_Induccion',
            'Cantidad_Brotes_Con_Induccion',
            'Cantidad_Brotes_Totales',
            'Cantidad_Brotes_Con_Flor',
        },
        nombre_layout='Induccion_Floral',
    )


def _leer_bronce(engine: Engine, columna_id: str) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                {columna_id} AS ID_Registro_Origen,
                Fecha_Raw,
                DNI_Raw,
                Fecha_Subida_Raw,
                Consumidor_Raw,
                Modulo_Raw,
                Turno_Raw,
                Valvula_Raw,
                Tipo_Evaluacion_Raw,
                Cama_Raw,
                Descripcion_Raw,
                Variedad_Raw,
                PlantasPorCama_Raw,
                PlantasConInduccion_Raw,
                BrotesConInduccion_Raw,
                BrotesTotales_Raw,
                BrotesConFlor_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_fact_induccion_floral(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    columna_id = _validar_layout_migrado(engine)
    df = _leer_bronce(engine, columna_id)
    if df.empty:
        return resumen
    resumen['leidos'] = len(df)

    df['Variedad_Fuente_Raw'] = df['Variedad_Raw'].where(
        df['Variedad_Raw'].notna(),
        df['Descripcion_Raw'],
    )
    df, cuar_var = homologar_columna(
        df,
        'Variedad_Fuente_Raw',
        'Variedad_Canonica',
        TABLA_ORIGEN,
        engine,
        columna_id_origen='ID_Registro_Origen',
    )
    resumen['cuarentena'].extend(cuar_var)

    payload_inserts = []
    ids_insertados: list[int] = []
    ids_rechazados: list[int] = []

    for _, fila in df.iterrows():
        id_origen = _a_entero_nulo(fila.get('ID_Registro_Origen'))

        fecha, valida = procesar_fecha(
            fila.get('Fecha_Raw'),
            dominio='induccion_floral',
        )
        if not valida:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'Fecha_Raw',
                'valor': fila.get('Fecha_Raw'),
                'motivo': 'Fecha invalida en induccion floral',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
        resultado_geo = resolver_geografia(
            None,
            None,
            modulo,
            engine,
            turno=fila.get('Turno_Raw'),
            valvula=fila.get('Valvula_Raw'),
            cama=fila.get('Cama_Raw'),
        )
        id_geo = resultado_geo.get('id_geografia')
        if not id_geo:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'Modulo_Raw',
                'valor': (
                    f"Modulo={fila.get('Modulo_Raw')} | Turno={fila.get('Turno_Raw')} | "
                    f"Valvula={fila.get('Valvula_Raw')} | Cama={fila.get('Cama_Raw')}"
                ),
                'motivo': _motivo_cuarentena_geografia(resultado_geo),
                'tipo_regla': 'MDM',
                'id_registro_origen': id_origen,
            })
            continue

        id_variedad = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)
        if not id_variedad:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            continue

        id_tiempo = obtener_id_tiempo(construir_id_tiempo(fecha), engine)
        if id_tiempo is None:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'Fecha_Raw',
                'valor': fila.get('Fecha_Raw'),
                'motivo': 'Fecha valida pero fuera de Dim_Tiempo',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        plantas_por_cama = _a_entero_no_negativo(fila.get('PlantasPorCama_Raw'))
        plantas_con_induccion = _a_entero_no_negativo(fila.get('PlantasConInduccion_Raw'))
        brotes_con_induccion = _a_entero_no_negativo(fila.get('BrotesConInduccion_Raw'))
        brotes_totales = _a_entero_no_negativo(fila.get('BrotesTotales_Raw'))
        brotes_con_flor = _a_entero_no_negativo(fila.get('BrotesConFlor_Raw'))

        if plantas_por_cama is None or plantas_por_cama <= 0:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'PlantasPorCama_Raw',
                'valor': fila.get('PlantasPorCama_Raw'),
                'motivo': 'Cantidad de plantas por cama invalida',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        if plantas_con_induccion is None or plantas_con_induccion > plantas_por_cama:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'PlantasConInduccion_Raw',
                'valor': fila.get('PlantasConInduccion_Raw'),
                'motivo': 'Plantas con induccion invalida o mayor al total por cama',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        if brotes_totales is None or brotes_totales <= 0:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'BrotesTotales_Raw',
                'valor': fila.get('BrotesTotales_Raw'),
                'motivo': 'Cantidad de brotes totales invalida',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        if brotes_con_induccion is None or brotes_con_induccion > brotes_totales:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'BrotesConInduccion_Raw',
                'valor': fila.get('BrotesConInduccion_Raw'),
                'motivo': 'Brotes con induccion invalida o mayor al total de brotes',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        if brotes_con_flor is None or brotes_con_flor > brotes_totales:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'BrotesConFlor_Raw',
                'valor': fila.get('BrotesConFlor_Raw'),
                'motivo': 'Brotes con flor invalida o mayor al total de brotes',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        dni, _ = procesar_dni(fila.get('DNI_Raw'))
        id_personal = obtener_id_personal(dni, engine)

        payload_inserts.append({
            'id_geo': id_geo,
            'id_tiempo': id_tiempo,
            'id_variedad': id_variedad,
            'id_personal': id_personal,
            'tipo_evaluacion': _texto_nulo(fila.get('Tipo_Evaluacion_Raw')),
            'codigo_consumidor': _texto_nulo(fila.get('Consumidor_Raw')),
            'plantas_por_cama': plantas_por_cama,
            'plantas_con_induccion': plantas_con_induccion,
            'brotes_con_induccion': brotes_con_induccion,
            'brotes_totales': brotes_totales,
            'brotes_con_flor': brotes_con_flor,
            'pct_plantas_induccion': _pct(plantas_con_induccion, plantas_por_cama),
            'pct_brotes_induccion': _pct(brotes_con_induccion, brotes_totales),
            'pct_brotes_flor': _pct(brotes_con_flor, brotes_totales),
            'fecha_evento': fecha.date(),
        })
        if id_origen is not None:
            ids_insertados.append(id_origen)

    if payload_inserts:
        ejecutar_en_lotes_con_engine(engine, SQL_INSERT_FACT, payload_inserts)
    resumen['insertados'] = len(payload_inserts)

    if ids_insertados:
        marcar_estado_carga_por_ids(engine, TABLA_ORIGEN, columna_id, ids_insertados, estado='PROCESADO')
    if ids_rechazados:
        marcar_estado_carga_por_ids(engine, TABLA_ORIGEN, columna_id, ids_rechazados, estado='RECHAZADO')

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

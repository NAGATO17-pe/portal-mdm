"""
fact_tasa_crecimiento_brotes.py
===============================
Carga Silver.Fact_Tasa_Crecimiento_Brotes desde Bronce.Tasa_Crecimiento_Brotes.
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


TABLA_ORIGEN = 'Bronce.Tasa_Crecimiento_Brotes'
TABLA_DESTINO = 'Silver.Fact_Tasa_Crecimiento_Brotes'

SQL_INSERT_FACT = text("""
    INSERT INTO Silver.Fact_Tasa_Crecimiento_Brotes (
        ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
        Tipo_Evaluacion, Condicion, Estado_Vegetativo,
        Tipo_Tallo, Codigo_Ensayo, Codigo_Origen,
        Campana, Observacion,
        Fecha_Poda_Aux, Dias_Desde_Poda, Medida_Crecimiento,
        Fecha_Evento, Fecha_Sistema, Estado_DQ
    ) VALUES (
        :id_geo, :id_tiempo, :id_variedad, :id_personal,
        :tipo_evaluacion, :condicion, :estado_vegetativo,
        :tipo_tallo, :codigo_ensayo, :codigo_origen,
        :campana, :observacion,
        :fecha_poda_aux, :dias_desde_poda, :medida_crecimiento,
        :fecha_evento, SYSDATETIME(), 'OK'
    )
""")


def _a_entero_nulo(valor) -> int | None:
    try:
        if valor is None:
            return None
        texto = str(valor).strip()
        if texto in ('', 'None', 'nan'):
            return None
        return int(float(texto))
    except (ValueError, TypeError):
        return None


def _a_decimal_nulo(valor) -> float | None:
    try:
        if valor is None:
            return None
        texto = str(valor).strip().replace(',', '.')
        if texto in ('', 'None', 'nan'):
            return None
        return float(texto)
    except (ValueError, TypeError):
        return None


def _texto_nulo(valor) -> str | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto if texto and texto.lower() != 'none' else None


def _obtener_columnas_tabla(engine: Engine, tabla_completa: str) -> set[str]:
    esquema, tabla = tabla_completa.split('.')
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :esquema
              AND TABLE_NAME = :tabla
        """), {'esquema': esquema, 'tabla': tabla})
        return {str(fila[0]) for fila in resultado.fetchall()}


def _motivo_cuarentena_geografia(resultado_geo: dict) -> str:
    estado = resultado_geo.get('estado')
    if estado in ('TEST_BLOCK_NO_MAPEADO', 'TEST_BLOCK_AMBIGUO'):
        return 'Test block (VI) sin mapeo unico en Dim_Geografia.'
    if estado in ('PENDIENTE_CASO_ESPECIAL', 'CASO_ESPECIAL_MODULO'):
        return 'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
    if estado in ('PENDIENTE_CAMA_GENERICA', 'CAMA_NO_RELACION'):
        return 'Cama no relacionada a la geografia operativa.'
    if estado in ('PENDIENTE_DIM_DUPLICADA', 'GEOGRAFIA_AMBIGUA'):
        return 'La clave geografica tiene mas de un registro vigente en Silver.Dim_Geografia.'
    if estado == 'CAMA_NO_VALIDA':
        return 'Cama fuera de rango operativo permitido.'
    if estado == 'CAMA_NO_CATALOGO':
        return 'Cama valida pero no registrada en el catalogo operativo.'
    return 'Geografia no encontrada en Silver.Dim_Geografia.'


def _validar_layout_migrado(engine: Engine) -> str:
    columnas_bronce = _obtener_columnas_tabla(engine, TABLA_ORIGEN)
    columnas_silver = _obtener_columnas_tabla(engine, TABLA_DESTINO)

    columnas_bronce_requeridas = {
        'ID_Tasa_Crecimiento',
        'Fecha_Raw',
        'DNI_Raw',
        'Modulo_Raw',
        'Turno_Raw',
        'Valvula_Raw',
        'Cama_Raw',
        'Condicion_Raw',
        'Estado_Vegetativo_Raw',
        'Variedad_Raw',
        'Tipo_Tallo_Raw',
        'Ensayo_Raw',
        'Medida_Raw',
        'Fecha_Poda_Aux_Raw',
        'Campana_Raw',
        'Tipo_Evaluacion_Raw',
        'Estado_Carga',
    }
    columnas_silver_requeridas = {
        'ID_Geografia',
        'ID_Tiempo',
        'ID_Variedad',
        'ID_Personal',
        'Codigo_Ensayo',
        'Codigo_Origen',
        'Fecha_Poda_Aux',
        'Dias_Desde_Poda',
        'Medida_Crecimiento',
    }

    faltantes_bronce = sorted(columnas_bronce_requeridas - columnas_bronce)
    faltantes_silver = sorted(columnas_silver_requeridas - columnas_silver)

    if faltantes_bronce or faltantes_silver:
        raise RuntimeError(
            'La migracion definitiva de Tasa_Crecimiento_Brotes no esta aplicada. '
            f'Bronce faltantes: {faltantes_bronce or "ninguno"} | '
            f'Silver faltantes: {faltantes_silver or "ninguno"}'
        )

    return 'ID_Tasa_Crecimiento'


def _leer_bronce(engine: Engine, columna_id: str) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                {columna_id} AS ID_Registro_Origen,
                Codigo_Origen_Raw,
                Semana_Raw,
                Dia_Raw,
                Fecha_Raw,
                DNI_Raw,
                Evaluador_Raw,
                Modulo_Raw,
                Turno_Raw,
                Valvula_Raw,
                Condicion_Raw,
                Estado_Vegetativo_Raw,
                Variedad_Raw,
                Cama_Raw,
                Tipo_Tallo_Raw,
                Ensayo_Raw,
                Medida_Raw,
                Fecha_Poda_Aux_Raw,
                Campana_Raw,
                Observacion_Raw,
                Tipo_Evaluacion_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_fact_tasa_crecimiento_brotes(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    columna_id = _validar_layout_migrado(engine)
    df = _leer_bronce(engine, columna_id)
    if df.empty:
        return resumen

    df, cuar_var = homologar_columna(
        df,
        'Variedad_Raw',
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

        fecha_evento, valida = procesar_fecha(fila.get('Fecha_Raw'), validar_campana=False)
        if not valida:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'Fecha_Raw',
                'valor': fila.get('Fecha_Raw'),
                'motivo': 'Fecha invalida en tasa de crecimiento',
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

        id_tiempo = obtener_id_tiempo(construir_id_tiempo(fecha_evento), engine)
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

        codigo_ensayo = _texto_nulo(fila.get('Ensayo_Raw'))
        if codigo_ensayo is None:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'Ensayo_Raw',
                'valor': fila.get('Ensayo_Raw'),
                'motivo': 'Codigo de ensayo vacio o invalido',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        medida_crecimiento = _a_decimal_nulo(fila.get('Medida_Raw'))
        if medida_crecimiento is None or medida_crecimiento < 0:
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_rechazados.append(id_origen)
            resumen['cuarentena'].append({
                'columna': 'Medida_Raw',
                'valor': fila.get('Medida_Raw'),
                'motivo': 'Medida de crecimiento invalida o negativa',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        fecha_poda_aux = None
        dias_desde_poda = None
        valor_fecha_poda = _texto_nulo(fila.get('Fecha_Poda_Aux_Raw'))
        if valor_fecha_poda is not None:
            fecha_poda_aux, valida_poda = procesar_fecha(valor_fecha_poda, validar_campana=False)
            if not valida_poda:
                resumen['rechazados'] += 1
                if id_origen is not None:
                    ids_rechazados.append(id_origen)
                resumen['cuarentena'].append({
                    'columna': 'Fecha_Poda_Aux_Raw',
                    'valor': fila.get('Fecha_Poda_Aux_Raw'),
                    'motivo': 'Fecha de poda auxiliar invalida',
                    'tipo_regla': 'DQ',
                    'id_registro_origen': id_origen,
                })
                continue

            dias_desde_poda = (fecha_evento.date() - fecha_poda_aux.date()).days
            if dias_desde_poda < 0:
                resumen['rechazados'] += 1
                if id_origen is not None:
                    ids_rechazados.append(id_origen)
                resumen['cuarentena'].append({
                    'columna': 'Fecha_Poda_Aux_Raw',
                    'valor': fila.get('Fecha_Poda_Aux_Raw'),
                    'motivo': 'Fecha de poda auxiliar posterior a la fecha de evaluacion',
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
            'condicion': _texto_nulo(fila.get('Condicion_Raw')),
            'estado_vegetativo': _texto_nulo(fila.get('Estado_Vegetativo_Raw')),
            'tipo_tallo': _texto_nulo(fila.get('Tipo_Tallo_Raw')),
            'codigo_ensayo': codigo_ensayo,
            'codigo_origen': _texto_nulo(fila.get('Codigo_Origen_Raw')),
            'campana': _texto_nulo(fila.get('Campana_Raw')),
            'observacion': _texto_nulo(fila.get('Observacion_Raw')),
            'fecha_poda_aux': None if fecha_poda_aux is None else fecha_poda_aux.date(),
            'dias_desde_poda': dias_desde_poda,
            'medida_crecimiento': medida_crecimiento,
            'fecha_evento': fecha_evento.date(),
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

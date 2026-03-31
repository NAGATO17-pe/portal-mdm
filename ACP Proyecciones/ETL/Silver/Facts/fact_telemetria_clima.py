"""
fact_telemetria_clima.py
========================
Carga Silver.Fact_Telemetria_Clima desde:
  - Bronce.Reporte_Clima
  - Bronce.Variables_Meteorologicas

Grano: Fecha + Hora + Sector_Climatico.
Transformacion critica: Humedad proporcion (0-1) -> porcentaje (0-100).
"""

import re

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from dq.cuarentena import enviar_a_cuarentena
from dq.validador import normalizar_humedad
from mdm.lookup import obtener_id_tiempo as obtener_id_tiempo_dim
from utils.fechas import obtener_id_tiempo as construir_id_tiempo, procesar_fecha
from utils.sql_lotes import ejecutar_en_lotes_con_engine, marcar_estado_carga_por_ids


TABLA_CLIMA = 'Bronce.Reporte_Clima'
TABLA_VARIABLES = 'Bronce.Variables_Meteorologicas'
TABLA_CUARENTENA = 'Bronce.Clima'
TAM_LOTE_CLIMA = 2000
PATRON_HORA_VALORES_RAW = re.compile(r'(?:^|\s\|\s)Hora_Raw=([^|]+)')


def _leer_bronce_clima(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Reporte_Clima,
                Fecha_Raw, Hora_Raw, Sector_Raw,
                TempMax_Raw, TempMin_Raw,
                Humedad_Raw, Precipitacion_Raw
            FROM {TABLA_CLIMA}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _leer_bronce_variables(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Variables_Met AS ID_Variables_Meteorologicas,
                Fecha_Raw, Sector_Raw,
                VPD_Raw, Radiacion_Raw,
                TempMax_Raw, TempMin_Raw, Humedad_Raw,
                Valores_Raw
            FROM {TABLA_VARIABLES}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _a_decimal(valor) -> float | None:
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return None


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


def _normalizar_sector_climatico(valor) -> str | None:
    if valor is None:
        return None

    texto = str(valor).strip()
    if not texto or texto.upper() in {'NONE', 'NULL', 'NAN'}:
        return None

    return texto.upper()


def _normalizar_hora_texto(valor) -> str | None:
    if valor is None:
        return None

    texto = str(valor).strip()
    if not texto or texto.upper() in {'NONE', 'NULL', 'NAN'}:
        return None

    coincidencia = re.search(r'(\d{1,2}:\d{2}(?::\d{2})?)', texto)
    if not coincidencia:
        return None

    hora = coincidencia.group(1)
    if len(hora) == 5:
        return f'{hora}:00'
    return hora


def _extraer_hora_desde_valores_raw(valores_raw) -> str | None:
    if valores_raw is None:
        return None

    coincidencia = PATRON_HORA_VALORES_RAW.search(str(valores_raw))
    if not coincidencia:
        return None

    return _normalizar_hora_texto(coincidencia.group(1))


def _construir_fecha_hora_clima(fecha_raw, hora_raw=None, valores_raw=None) -> str:
    fecha_base = str(fecha_raw).strip() if fecha_raw is not None else ''
    hora = _normalizar_hora_texto(hora_raw)
    if hora is None:
        hora = _extraer_hora_desde_valores_raw(valores_raw)

    if not fecha_base:
        return ''
    if hora is None:
        return fecha_base
    return f'{fecha_base} {hora}'


def _agregar_cuarentena(
    cuarentena: list[dict],
    columna: str,
    valor,
    motivo: str,
    id_registro_origen,
) -> None:
    cuarentena.append({
        'columna': columna,
        'valor': valor,
        'motivo': motivo,
        'id_registro_origen': id_registro_origen,
    })


SQL_INSERT_CLIMA_REPORTE = text("""
    INSERT INTO Silver.Fact_Telemetria_Clima (
        Sector_Climatico, ID_Tiempo,
        Temperatura_Max_C, Temperatura_Min_C,
        Humedad_Relativa_Pct, Precipitacion_mm,
        VPD, Radiacion_Solar,
        Fecha_Evento, Fecha_Sistema
    ) VALUES (
        :sector_climatico, :id_tiempo,
        :temp_max, :temp_min,
        :humedad, :precipitacion,
        NULL, NULL,
        :fecha_evento, SYSDATETIME()
    )
""")

SQL_INSERT_CLIMA_VARIABLES = text("""
    INSERT INTO Silver.Fact_Telemetria_Clima (
        Sector_Climatico, ID_Tiempo,
        Temperatura_Max_C, Temperatura_Min_C,
        Humedad_Relativa_Pct, Precipitacion_mm,
        VPD, Radiacion_Solar,
        Fecha_Evento, Fecha_Sistema
    ) VALUES (
        :sector_climatico, :id_tiempo,
        :temp_max, :temp_min,
        :humedad, NULL,
        :vpd, :radiacion,
        :fecha_evento, SYSDATETIME()
    )
""")


def cargar_fact_telemetria_clima(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df_clima = _leer_bronce_clima(engine)
    ids_clima_insertados = []
    ids_clima_rechazados = []
    payload_clima = []
    total_clima = len(df_clima)

    for indice, fila in enumerate(df_clima.to_dict('records'), start=1):
        id_origen = _a_entero_nulo(fila.get('ID_Reporte_Clima'))
        fecha_str = f"{fila.get('Fecha_Raw')} {fila.get('Hora_Raw', '00:00')}"
        fecha, valida = procesar_fecha(fecha_str, validar_campana=False)
        if not valida:
            _agregar_cuarentena(
                resumen['cuarentena'],
                'Fecha_Raw',
                fecha_str,
                'Fecha invalida en clima',
                id_origen,
            )
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_clima_rechazados.append(id_origen)
            continue

        id_tiempo = obtener_id_tiempo_dim(construir_id_tiempo(fecha), engine)
        if not id_tiempo:
            _agregar_cuarentena(
                resumen['cuarentena'],
                'Fecha_Raw',
                fecha_str,
                'Fecha valida pero fuera de Dim_Tiempo',
                id_origen,
            )
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_clima_rechazados.append(id_origen)
            continue

        sector_climatico = _normalizar_sector_climatico(fila.get('Sector_Raw'))
        if not sector_climatico:
            _agregar_cuarentena(
                resumen['cuarentena'],
                'Sector_Raw',
                fila.get('Sector_Raw'),
                'Sector climatico invalido o ausente',
                id_origen,
            )
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_clima_rechazados.append(id_origen)
            continue

        humedad, error_hum = normalizar_humedad(fila.get('Humedad_Raw'))
        if error_hum:
            error_hum.update({
                'columna': 'Humedad_Raw',
                'valor': fila.get('Humedad_Raw'),
                'id_registro_origen': id_origen,
            })
            resumen['cuarentena'].append(error_hum)

        payload_clima.append({
            'sector_climatico': sector_climatico,
            'id_tiempo': id_tiempo,
            'temp_max': _a_decimal(fila.get('TempMax_Raw')),
            'temp_min': _a_decimal(fila.get('TempMin_Raw')),
            'humedad': humedad,
            'precipitacion': _a_decimal(fila.get('Precipitacion_Raw')),
            'fecha_evento': fecha,
        })
        if id_origen is not None:
            ids_clima_insertados.append(id_origen)
        resumen['insertados'] += 1

    if payload_clima:
        ejecutar_en_lotes_con_engine(engine, SQL_INSERT_CLIMA_REPORTE, payload_clima, tam_lote=TAM_LOTE_CLIMA)

    marcar_estado_carga_por_ids(
        engine, TABLA_CLIMA, 'ID_Reporte_Clima', ids_clima_insertados, estado='PROCESADO'
    )
    marcar_estado_carga_por_ids(
        engine, TABLA_CLIMA, 'ID_Reporte_Clima', ids_clima_rechazados, estado='RECHAZADO'
    )

    df_vars = _leer_bronce_variables(engine)
    ids_vars_insertados = []
    ids_vars_rechazados = []
    payload_vars = []
    total_vars = len(df_vars)

    for indice, fila in enumerate(df_vars.to_dict('records'), start=1):
        id_origen = _a_entero_nulo(fila.get('ID_Variables_Meteorologicas'))
        fecha_str = _construir_fecha_hora_clima(
            fila.get('Fecha_Raw'),
            valores_raw=fila.get('Valores_Raw'),
        )
        fecha, valida = procesar_fecha(fecha_str, validar_campana=False)
        if not valida:
            _agregar_cuarentena(
                resumen['cuarentena'],
                'Fecha_Raw',
                fecha_str or fila.get('Fecha_Raw'),
                'Fecha invalida en clima',
                id_origen,
            )
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_vars_rechazados.append(id_origen)
            continue

        id_tiempo = obtener_id_tiempo_dim(construir_id_tiempo(fecha), engine)
        if not id_tiempo:
            _agregar_cuarentena(
                resumen['cuarentena'],
                'Fecha_Raw',
                fecha_str or fila.get('Fecha_Raw'),
                'Fecha valida pero fuera de Dim_Tiempo',
                id_origen,
            )
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_vars_rechazados.append(id_origen)
            continue

        sector_climatico = _normalizar_sector_climatico(fila.get('Sector_Raw'))
        if not sector_climatico:
            _agregar_cuarentena(
                resumen['cuarentena'],
                'Sector_Raw',
                fila.get('Sector_Raw'),
                'Sector climatico invalido o ausente',
                id_origen,
            )
            resumen['rechazados'] += 1
            if id_origen is not None:
                ids_vars_rechazados.append(id_origen)
            continue

        humedad, error_hum = normalizar_humedad(fila.get('Humedad_Raw'))
        if error_hum:
            error_hum.update({
                'columna': 'Humedad_Raw',
                'valor': fila.get('Humedad_Raw'),
                'id_registro_origen': id_origen,
            })
            resumen['cuarentena'].append(error_hum)

        payload_vars.append({
            'sector_climatico': sector_climatico,
            'id_tiempo': id_tiempo,
            'temp_max': _a_decimal(fila.get('TempMax_Raw')),
            'temp_min': _a_decimal(fila.get('TempMin_Raw')),
            'humedad': humedad,
            'vpd': _a_decimal(fila.get('VPD_Raw')),
            'radiacion': _a_decimal(fila.get('Radiacion_Raw')),
            'fecha_evento': fecha,
        })
        if id_origen is not None:
            ids_vars_insertados.append(id_origen)
        resumen['insertados'] += 1

    if payload_vars:
        ejecutar_en_lotes_con_engine(engine, SQL_INSERT_CLIMA_VARIABLES, payload_vars, tam_lote=TAM_LOTE_CLIMA)

    marcar_estado_carga_por_ids(
        engine, TABLA_VARIABLES, 'ID_Variables_Met', ids_vars_insertados, estado='PROCESADO'
    )
    marcar_estado_carga_por_ids(
        engine, TABLA_VARIABLES, 'ID_Variables_Met', ids_vars_rechazados, estado='RECHAZADO'
    )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_CUARENTENA, resumen['cuarentena'])

    return resumen

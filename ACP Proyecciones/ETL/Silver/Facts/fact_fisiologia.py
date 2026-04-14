"""
fact_fisiologia.py
==================
Carga Silver.Fact_Fisiologia desde Bronce.Fisiologia.

Grain: Geo + Tiempo + Variedad + Tercio
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.sql_lotes import ejecutar_en_lotes
from mdm.lookup      import resolver_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna
from silver.facts._helpers_fact_comunes import (
    a_entero_nulo as _a_int,
    finalizar_resumen_fact as _finalizar_resumen_fact,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    parsear_valores_raw as _parsear_valores_raw,
    registrar_rechazo as _registrar_rechazo,
)


TABLA_ORIGEN  = 'Bronce.Fisiologia'
TABLA_DESTINO = 'Silver.Fact_Fisiologia'

SQL_INSERT_FACT = text("""
    INSERT INTO Silver.Fact_Fisiologia (
        ID_Geografia, ID_Tiempo, ID_Variedad,
        Tercio, Brotes_Productivos, Brotes_Vegetativos,
        Hinchadas, Productivas, Total_Organos,
        Fecha_Evento, Fecha_Sistema, Estado_DQ
    ) VALUES (
        :id_geo, :id_tiempo, :id_variedad,
        :tercio, :brotes_prod, :brotes_veg,
        :hinchadas, :productivas, :total_organos,
        :fecha_evento, SYSDATETIME(), 'OK'
    )
""")

MAPA_TERCIO = {
    'BAJO': 'BAJO', 'B': 'BAJO', 'LOW': 'BAJO',
    'MEDIO': 'MEDIO', 'M': 'MEDIO', 'MID': 'MEDIO',
    'ALTO': 'ALTO', 'A': 'ALTO', 'HIGH': 'ALTO',
}


def _obtener_columna_sql(columnas_disponibles: set[str], nombre_columna: str) -> str:
    if nombre_columna in columnas_disponibles:
        return nombre_columna
    return f"CAST(NULL AS NVARCHAR(MAX)) AS {nombre_columna}"


def _obtener_valor_raw(fila: pd.Series, nombre_columna: str, valores_raw: dict[str, str] | None = None):
    valor = fila.get(nombre_columna)
    if valor is not None and str(valor).strip() not in ('', 'None', 'nan'):
        return valor

    if valores_raw is None:
        valores_raw = _parsear_valores_raw(fila.get('Valores_Raw'))
    valor_serializado = valores_raw.get(nombre_columna)
    if valor_serializado is None or str(valor_serializado).strip() in ('', 'None', 'nan'):
        return None
    return valor_serializado


def _normalizar_tercio(valor) -> str | None:
    tercio_raw = str(valor or '').strip().upper()
    return MAPA_TERCIO.get(tercio_raw, tercio_raw or None)


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        columnas_resultado = conexion.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Bronce'
              AND TABLE_NAME = 'Fisiologia'
        """)).fetchall()
        columnas_disponibles = {str(fila[0]) for fila in columnas_resultado}

        columnas_select = [
            'ID_Fisiologia',
            _obtener_columna_sql(columnas_disponibles, 'Fecha_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Fundo_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Sector_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Modulo_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Turno_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Valvula_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Variedad_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Tercio_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Hinchadas_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Productivas_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Total_Org_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Brote_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'BrotesProd_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'BrotesVeg_Raw'),
            _obtener_columna_sql(columnas_disponibles, 'Valores_Raw'),
        ]
        resultado = conexion.execute(text(f"""
            WITH LoteActual AS (
                SELECT TOP (1)
                    Fecha_Sistema,
                    Nombre_Archivo
                FROM {TABLA_ORIGEN}
                WHERE Estado_Carga = 'CARGADO'
                  AND CAST(Fecha_Sistema AS DATE) = CAST(SYSDATETIME() AS DATE)
                ORDER BY Fecha_Sistema DESC, ID_Fisiologia DESC
            )
            SELECT
                {", ".join(columnas_select)}
            FROM {TABLA_ORIGEN} f
            INNER JOIN LoteActual l
                ON f.Fecha_Sistema = l.Fecha_Sistema
               AND f.Nombre_Archivo = l.Nombre_Archivo
            WHERE f.Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_fact_fisiologia(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df)

    ids_procesados = []
    ids_rechazados = []
    payload_inserts = []
    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()

        df, cuar_var = homologar_columna(
            df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, conexion
        )
        resumen['cuarentena'].extend(cuar_var)

        for _, fila in df.iterrows():
            id_origen = int(fila['ID_Fisiologia'])
            fecha, valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='fisiologia',
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

            valores_raw = _parsear_valores_raw(fila.get('Valores_Raw'))
            modulo_raw = _obtener_valor_raw(fila, 'Modulo_Raw', valores_raw)
            turno_raw = _obtener_valor_raw(fila, 'Turno_Raw', valores_raw)
            valvula_raw = _obtener_valor_raw(fila, 'Valvula_Raw', valores_raw)
            fundo_raw = _obtener_valor_raw(fila, 'Fundo_Raw', valores_raw)
            sector_raw = _obtener_valor_raw(fila, 'Sector_Raw', valores_raw)

            modulo = None if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
            resultado_geo = resolver_geografia(
                fundo_raw,
                sector_raw,
                modulo,
                engine,
                turno=turno_raw,
                valvula=valvula_raw,
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
                        f"Fundo={fundo_raw} | Sector={sector_raw} | Modulo={modulo_raw} | "
                        f"Turno={turno_raw} | Valvula={valvula_raw}"
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

            brotes_prod = _a_int(_obtener_valor_raw(fila, 'BrotesProd_Raw', valores_raw))
            if brotes_prod is None:
                brotes_prod = _a_int(_obtener_valor_raw(fila, 'Brote_Raw', valores_raw))

            payload_inserts.append({
                'id_geo': id_geo,
                'id_tiempo': obtener_id_tiempo(fecha),
                'id_variedad': id_var,
                'tercio': _normalizar_tercio(_obtener_valor_raw(fila, 'Tercio_Raw', valores_raw)),
                'brotes_prod': brotes_prod,
                'brotes_veg': _a_int(_obtener_valor_raw(fila, 'BrotesVeg_Raw', valores_raw)),
                'hinchadas': _a_int(_obtener_valor_raw(fila, 'Hinchadas_Raw', valores_raw)),
                'productivas': _a_int(_obtener_valor_raw(fila, 'Productivas_Raw', valores_raw)),
                'total_organos': _a_int(_obtener_valor_raw(fila, 'Total_Org_Raw', valores_raw)),
                'fecha_evento': fecha,
            })
            ids_procesados.append(id_origen)

        if payload_inserts:
            ejecutar_en_lotes(conexion, SQL_INSERT_FACT, payload_inserts)
        resumen['insertados'] = len(payload_inserts)

        if ids_procesados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Fisiologia', ids_procesados)
        if ids_rechazados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Fisiologia', ids_rechazados, estado='RECHAZADO')

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_ORIGEN, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)

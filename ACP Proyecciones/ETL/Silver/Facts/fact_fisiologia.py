"""
fact_fisiologia.py
==================
Carga Silver.Fact_Fisiologia desde Bronce.Fisiologia.

Grain: Geo + Tiempo + Variedad + Tercio
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.sql_lotes import marcar_estado_carga_por_ids
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_ORIGEN  = 'Bronce.Fisiologia'
TABLA_DESTINO = 'Silver.Fact_Fisiologia'


def _a_int(valor) -> int | None:
    try:
        return int(float(str(valor)))
    except (ValueError, TypeError):
        return None


def _parsear_valores_raw(texto: str | None) -> dict[str, str]:
    if texto is None:
        return {}

    crudo = str(texto).strip()
    if not crudo:
        return {}

    resultado: dict[str, str] = {}
    for parte in crudo.split('|'):
        if '=' not in parte:
            continue
        clave, valor = parte.split('=', 1)
        clave = str(clave).strip()
        valor = str(valor).strip()
        if clave:
            resultado[clave] = valor
    return resultado


def _obtener_columna_sql(columnas_disponibles: set[str], nombre_columna: str) -> str:
    if nombre_columna in columnas_disponibles:
        return nombre_columna
    return f"CAST(NULL AS NVARCHAR(MAX)) AS {nombre_columna}"


def _obtener_valor_raw(fila: pd.Series, nombre_columna: str):
    valor = fila.get(nombre_columna)
    if valor is not None and str(valor).strip() not in ('', 'None', 'nan'):
        return valor

    valores_raw = _parsear_valores_raw(fila.get('Valores_Raw'))
    valor_serializado = valores_raw.get(nombre_columna)
    if valor_serializado is None or str(valor_serializado).strip() in ('', 'None', 'nan'):
        return None
    return valor_serializado


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
        return resumen

    df, cuar_var = homologar_columna(
        df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, engine
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_procesados = []

    with engine.begin() as conexion:
        for _, fila in df.iterrows():
            fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not valida:
                resumen['rechazados'] += 1
                continue

            modulo_raw = _obtener_valor_raw(fila, 'Modulo_Raw')
            turno_raw = _obtener_valor_raw(fila, 'Turno_Raw')
            valvula_raw = _obtener_valor_raw(fila, 'Valvula_Raw')
            fundo_raw = _obtener_valor_raw(fila, 'Fundo_Raw')
            sector_raw = _obtener_valor_raw(fila, 'Sector_Raw')

            modulo = None if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
            id_geo = obtener_id_geografia(
                fundo_raw,
                sector_raw,
                modulo,
                engine,
                turno=turno_raw,
                valvula=valvula_raw,
                cama=None,
            )
            id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo or not id_var:
                resumen['rechazados'] += 1
                continue

            # Tercio: normalizar BAJO/MEDIO/ALTO
            tercio_raw = str(_obtener_valor_raw(fila, 'Tercio_Raw') or '').strip().upper()
            tercio_map = {
                'BAJO': 'BAJO', 'B': 'BAJO', 'LOW': 'BAJO',
                'MEDIO': 'MEDIO', 'M': 'MEDIO', 'MID': 'MEDIO',
                'ALTO': 'ALTO', 'A': 'ALTO', 'HIGH': 'ALTO',
            }
            tercio = tercio_map.get(tercio_raw, tercio_raw or None)

            # Brote_Raw — puede venir como un número único (productivos)
            # o como texto combinado. Se guarda en Brotes_Productivos.
            brotes_prod = _a_int(_obtener_valor_raw(fila, 'BrotesProd_Raw'))
            if brotes_prod is None:
                brotes_prod = _a_int(_obtener_valor_raw(fila, 'Brote_Raw'))

            brotes_veg = _a_int(_obtener_valor_raw(fila, 'BrotesVeg_Raw'))

            conexion.execute(text("""
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
            """), {
                'id_geo':        id_geo,
                'id_tiempo':     obtener_id_tiempo(fecha),
                'id_variedad':   id_var,
                'tercio':        tercio,
                'brotes_prod':   brotes_prod,
                'brotes_veg':    brotes_veg,
                'hinchadas':     _a_int(_obtener_valor_raw(fila, 'Hinchadas_Raw')),
                'productivas':   _a_int(_obtener_valor_raw(fila, 'Productivas_Raw')),
                'total_organos': _a_int(_obtener_valor_raw(fila, 'Total_Org_Raw')),
                'fecha_evento':  fecha,
            })
            ids_procesados.append(int(fila['ID_Fisiologia']))
            resumen['insertados'] += 1

    marcar_estado_carga_por_ids(
        engine, TABLA_ORIGEN, 'ID_Fisiologia', ids_procesados
    )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

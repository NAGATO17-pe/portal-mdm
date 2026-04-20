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
from utils.fechas import obtener_id_tiempo
from mdm.homologador import homologar_columna
from silver.facts._base_processor import BaseFactProcessor
from silver.facts._helpers_fact_comunes import (
    a_entero_nulo as _a_int,
    finalizar_resumen_fact as _finalizar_resumen_fact,
    parsear_valores_raw as _parsear_valores_raw,
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


class ProcesadorFisiologia(BaseFactProcessor):
    def __init__(self, engine: Engine):
        super().__init__(engine, TABLA_ORIGEN, TABLA_DESTINO, columna_id='ID_Fisiologia')
        # Grain: Geo + Tiempo + Variedad + Tercio
        self.columnas_clave_unica = ['ID_Geografia', 'ID_Tiempo', 'ID_Variedad', 'Tercio']

    def _construir_payload(self, df: pd.DataFrame) -> list[dict]:
        payload = []
        for _, fila in df.iterrows():
            id_origen = int(fila['ID_Fisiologia'])

            fecha = self._validar_y_resolver_fecha(id_origen, fila.get('Fecha_Raw'), 'fisiologia')
            if fecha is None:
                continue

            valores_raw = _parsear_valores_raw(fila.get('Valores_Raw'))
            modulo_raw  = _obtener_valor_raw(fila, 'Modulo_Raw',  valores_raw)
            turno_raw   = _obtener_valor_raw(fila, 'Turno_Raw',   valores_raw)
            valvula_raw = _obtener_valor_raw(fila, 'Valvula_Raw', valores_raw)
            fundo_raw   = _obtener_valor_raw(fila, 'Fundo_Raw',   valores_raw)

            resultado_geo = self._validar_y_resolver_geografia(
                id_origen,
                fundo_raw,
                modulo_raw,
                turno=turno_raw,
                valvula=valvula_raw,
            )
            if resultado_geo is None:
                continue

            id_var = self._validar_y_resolver_variedad(id_origen, fila.get('Variedad_Canonica'), fila.get('Variedad_Raw'))
            if id_var is None:
                continue

            brotes_prod = _a_int(_obtener_valor_raw(fila, 'BrotesProd_Raw', valores_raw))
            if brotes_prod is None:
                brotes_prod = _a_int(_obtener_valor_raw(fila, 'Brote_Raw', valores_raw))

            self.ids_procesados.append(id_origen)
            payload.append({
                'ID_Geografia':     resultado_geo['id_geografia'],
                'ID_Tiempo':        obtener_id_tiempo(fecha),
                'ID_Variedad':      id_var,
                'Tercio':           _normalizar_tercio(_obtener_valor_raw(fila, 'Tercio_Raw', valores_raw)),
                'Brotes_Productivos': brotes_prod,
                'Brotes_Vegetativos': _a_int(_obtener_valor_raw(fila, 'BrotesVeg_Raw', valores_raw)),
                'Hinchadas':        _a_int(_obtener_valor_raw(fila, 'Hinchadas_Raw',  valores_raw)),
                'Productivas':      _a_int(_obtener_valor_raw(fila, 'Productivas_Raw', valores_raw)),
                'Total_Organos':    _a_int(_obtener_valor_raw(fila, 'Total_Org_Raw',  valores_raw)),
                'Fecha_Evento':     fecha,
                'Estado_DQ':        'OK',
                'id_origen_rastreo': id_origen,
            })
        return payload


def cargar_fact_fisiologia(engine: Engine) -> dict:
    proc = ProcesadorFisiologia(engine)

    df = _leer_bronce(engine)
    if df.empty:
        return _finalizar_resumen_fact(proc.resumen)
    proc.resumen['leidos'] = len(df)

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()

        df, cuar_var = homologar_columna(
            df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, conexion
        )
        # Deduplicación temprana para evitar procesar 40k+ filas innecesarias (ej. Tercios duplicados o ruido técnico)
        df = proc.pre_limpiar_duplicados_batch(df, ['Modulo_Raw', 'Fecha_Raw', 'Variedad_Raw', 'Tercio_Raw'])
        
        proc.resumen['cuarentena'].extend(cuar_var)

        payload = proc._construir_payload(df)
        proc._ejecutar_insercion_masiva_segura(contexto, payload, '#Temp_Fisiologia')

        return proc.finalizar_proceso(contexto)

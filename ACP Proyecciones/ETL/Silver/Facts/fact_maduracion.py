"""
fact_maduracion.py
==================
Carga Silver.Fact_Maduracion desde Bronce.Maduracion.

Layout real observado:
- Cada fila de Bronce representa un organo observado.
- Los atributos operativos vienen en Valores_Raw como pares clave=valor.
- El fact final guarda seguimiento por organo y fecha exacta.
"""

from __future__ import annotations

import re
import unicodedata
import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

_log = logging.getLogger("ETL_Pipeline")

from silver.facts._base_processor import BaseFactProcessor
from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.sql_lotes import ejecutar_en_lotes
from mdm.homologador import (
    cargar_catalogo_variedades,
    cargar_diccionario,
    homologar_valor,
)
from mdm.lookup import (
    obtener_id_cinta,
    obtener_id_estado_fenologico,
    obtener_id_personal,
    obtener_id_variedad,
    resolver_geografia,
)
from utils.dni import procesar_dni
from utils.fechas import obtener_id_tiempo, procesar_fecha
from utils.texto import es_test_block, normalizar_modulo
from silver.facts._helpers_fact_comunes import (
    finalizar_resumen_fact as _finalizar_resumen_fact,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    parsear_valores_raw as _parsear_valores_raw,
    registrar_rechazo as _registrar_rechazo,
)


TABLA_ORIGEN = 'Bronce.Maduracion'
TABLA_DESTINO = 'Silver.Fact_Maduracion'
TAM_LOTE_PROGRESO = 5000

# Fallback en caso de que MDM.Diccionario_Homologacion no tenga entradas para estado ciclo.
# Fuente de verdad: MDM.Diccionario_Homologacion WHERE Tabla_Origen='Bronce.Maduracion'
#                  AND Campo_Origen='DESCRIPCIONESTADOCICLO_Raw'
_MAPA_ESTADO_CICLO_FALLBACK: dict[str, str] = {
    'BOTON FLORAL': 'Boton Floral',
    'BOTON': 'Boton Floral',
    'FLOR': 'Flor',
    'PEQUENA': 'Pequena',
    'PEQUENA FRUTA': 'Pequena',
    'VERDE': 'Verde',
    'INICIO FASE 1': 'Inicio F1',
    'INICIO F1': 'Inicio F1',
    'FASE 1': 'Inicio F1',
    'INICIO FASE 2': 'Inicio F2',
    'INICIO F2': 'Inicio F2',
    'FASE 2': 'Inicio F2',
    'CREMA': 'Crema',
    'MADURA': 'Madura',
    'MADURO': 'Madura',
    'PINTON': 'Crema',
    'PINTONA': 'Crema',
    'COSECHABLE': 'Cosechable',
}

MAPA_ESTADO_CICLO = _MAPA_ESTADO_CICLO_FALLBACK

MAPA_ESTADO_POR_ID = {
    0: 'Boton Floral',
    1: 'Flor',
    2: 'Pequena',
    3: 'Verde',
    4: 'Inicio F1',
    5: 'Inicio F2',
    6: 'Crema',
    7: 'Madura',
    8: 'Cosechable',
}


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        columnas_resultado = conexion.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Bronce'
              AND TABLE_NAME = 'Maduracion'
        """)).fetchall()
        columnas_disponibles = {str(fila[0]) for fila in columnas_resultado}
        filtro_estado = "WHERE Estado_Carga = 'CARGADO'" if 'Estado_Carga' in columnas_disponibles else ""

        resultado = conexion.execute(text(f"""
            SELECT
                ID_Maduracion,
                Fecha_Raw,
                Modulo_Raw,
                Turno_Raw,
                Valvula_Raw,
                Variedad_Raw,
                Evaluador_Raw,
                Valores_Raw,
                Nombre_Archivo,
                Fecha_Sistema
            FROM {TABLA_ORIGEN}
            {filtro_estado}
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _tabla_maduracion_tiene_estado_carga(engine: Engine) -> bool:
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT 1
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Bronce'
              AND TABLE_NAME = 'Maduracion'
              AND COLUMN_NAME = 'Estado_Carga'
        """)).fetchone()
        return resultado is not None


def _obtener_valor(payload: dict[str, str], *claves: str):
    for clave in claves:
        if clave in payload and str(payload[clave]).strip() != '':
            return payload[clave]
    return None


def _extraer_entero(valor) -> int | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    texto = re.sub(r'[^0-9\-.]+', '', texto)
    if not texto:
        return None
    try:
        return int(float(texto))
    except (ValueError, TypeError):
        return None


def _extraer_texto(valor) -> str | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto if texto else None


def _normalizar_estado_crudo(valor: str | None) -> str | None:
    texto = _extraer_texto(valor)
    if not texto:
        return None
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(ch for ch in texto if not unicodedata.combining(ch))
    texto = re.sub(r'[^A-Z0-9]+', ' ', texto.upper()).strip()
    texto = re.sub(r'\s+', ' ', texto)
    return texto or None


def _resolver_estado_canonico(payload: dict[str, str], mapa_estado: dict[str, str]) -> str | None:
    descripcion = _extraer_texto(
        _obtener_valor(payload, 'DESCRIPCIONESTADOCICLO_Raw', 'DESCRIPCION_ESTADO_CICLO_Raw')
    )
    descripcion_normalizada = _normalizar_estado_crudo(descripcion)
    if descripcion_normalizada and descripcion_normalizada in mapa_estado:
        return mapa_estado[descripcion_normalizada]

    id_estado = _extraer_entero(_obtener_valor(payload, 'IDESTADOCICLO_Raw', 'ID_ESTADO_CICLO_Raw'))
    if id_estado is None:
        return None
    return MAPA_ESTADO_POR_ID.get(id_estado)


def _cargar_mapa_estado_desde_db(engine: Engine) -> dict[str, str]:
    """
    Lee alias de estado fenológico desde MDM.Diccionario_Homologacion.
    Si no hay entradas aprobadas, retorna el fallback hardcodeado.
    """
    try:
        with engine.connect() as conexion:
            resultado = conexion.execute(text("""
                SELECT Texto_Crudo, Valor_Canonico
                FROM MDM.Diccionario_Homologacion
                WHERE Tabla_Origen  = 'Bronce.Maduracion'
                  AND Campo_Origen  = 'DESCRIPCIONESTADOCICLO_Raw'
                  AND Aprobado_Por IS NOT NULL
                  AND Aprobado_Por != 'PENDIENTE'
            """)).fetchall()
        if resultado:
            return {str(fila[0]).strip().upper(): str(fila[1]).strip() for fila in resultado}
    except Exception:
        pass
    return dict(_MAPA_ESTADO_CICLO_FALLBACK)


class ProcesadorMaduracion(BaseFactProcessor):
    def __init__(self, engine: Engine):
        super().__init__(engine, TABLA_ORIGEN, TABLA_DESTINO)
        self.columnas_clave_unica = ['ID_Geografia', 'ID_Tiempo', 'ID_Variedad', 'ID_Cinta', 'ID_Organo']
        self.columna_tiebreaker_timestamp = '_fecha_registro_dt'


def cargar_fact_maduracion(engine: Engine) -> dict:
    procesador = ProcesadorMaduracion(engine)
    
    df = _leer_bronce(engine)
    if df.empty:
        return _finalizar_resumen_fact(procesador.resumen)
    procesador.resumen['leidos'] = len(df)

    diccionario_variedades = cargar_diccionario(engine, TABLA_ORIGEN)
    catalogo_variedades = cargar_catalogo_variedades(engine)
    mapa_estado_ciclo = _cargar_mapa_estado_desde_db(engine)
    cache_variedades: dict[str, tuple[str | None, str]] = {}
    tabla_soporta_estado_carga = _tabla_maduracion_tiene_estado_carga(engine)
    
    payload_inserts: list[dict] = []
    cache_cinta: dict[str | None, int | None] = {}

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()
        
        for indice, fila in enumerate(df.itertuples(index=False), start=1):

            fila_dict = fila._asdict()
            id_origen = int(fila_dict['ID_Maduracion'])
            payload = _parsear_valores_raw(fila_dict.get('Valores_Raw'))

            fecha_raw = _obtener_valor(payload, 'FECHA_Raw', 'Fecha_Raw') or fila_dict.get('Fecha_Raw')
            fecha, fecha_valida = procesar_fecha(
                fecha_raw,
                dominio='maduracion',
            )
            if not fecha_valida:
                procesador.registrar_rechazo(id_origen, 'FECHA_Raw', fecha_raw, 'Fecha invalida o fuera de campana')
                continue

            modulo_raw = _obtener_valor(payload, 'MODULO_Raw', 'Modulo_Raw') or fila_dict.get('Modulo_Raw')
            turno_raw = _obtener_valor(payload, 'TURNO_Raw', 'Turno_Raw') or fila_dict.get('Turno_Raw')
            valvula_raw = _obtener_valor(payload, 'NROVALVULA_Raw', 'VALVULA_Raw', 'Valvula_Raw') or fila_dict.get('Valvula_Raw')
            variedad_raw = _obtener_valor(payload, 'VARIEDAD_Raw', 'Variedad_Raw') or fila_dict.get('Variedad_Raw')
            evaluador_raw = _obtener_valor(payload, 'USUARIO_Raw', 'EVALUADOR_Raw', 'Evaluador_Raw') or fila_dict.get('Evaluador_Raw')
            color_raw = _extraer_texto(_obtener_valor(payload, 'COLOR_Raw', 'Color_Raw'))
            organo_raw = _obtener_valor(payload, 'ORGANO_Raw', 'Organo_Raw')

            numero_organo = _extraer_entero(organo_raw)
            if numero_organo is None or numero_organo < 1:
                procesador.registrar_rechazo(id_origen, 'ORGANO_Raw', organo_raw if organo_raw is not None else fila_dict.get('Valores_Raw'), 'ID_Organo invalido o ausente en maduracion')
                continue

            estado_canonico = _resolver_estado_canonico(payload, mapa_estado_ciclo)
            id_estado = obtener_id_estado_fenologico(estado_canonico, engine)
            if not id_estado:
                procesador.registrar_rechazo(id_origen, 'DESCRIPCIONESTADOCICLO_Raw', _obtener_valor(payload, 'DESCRIPCIONESTADOCICLO_Raw', 'IDESTADOCICLO_Raw'), 'Estado fenologico no reconocido en maduracion')
                continue

            modulo = normalizar_modulo(modulo_raw)
            clave_geo = (
                None if modulo is None else str(modulo).strip(),
                None if turno_raw is None else str(turno_raw).strip(),
                None if valvula_raw is None else str(valvula_raw).strip(),
            )
            if clave_geo not in procesador._cache_geografia:
                procesador._cache_geografia[clave_geo] = resolver_geografia(
                    None,
                    None,
                    modulo,
                    engine,
                    turno=turno_raw,
                    valvula=valvula_raw,
                    cama=None,
                )
            resultado_geo = procesador._cache_geografia[clave_geo]
            id_geografia = resultado_geo.get('id_geografia')
            if not id_geografia:
                procesador.registrar_rechazo(id_origen, 'MODULO_Raw', f"Modulo={modulo_raw}  Turno={turno_raw}  Valvula={valvula_raw}", _motivo_cuarentena_geografia(resultado_geo), tipo_regla='MDM')
                continue

            variedad_token = "" if variedad_raw is None else str(variedad_raw).strip()
            if variedad_token in cache_variedades:
                variedad_canonica, _estado_variedad = cache_variedades[variedad_token]
            else:
                variedad_canonica, estado_variedad = homologar_valor(
                    variedad_raw,
                    TABLA_ORIGEN,
                    'VARIEDAD_Raw',
                    diccionario_variedades,
                    catalogo_variedades,
                    conexion,
                )
                cache_variedades[variedad_token] = (variedad_canonica, estado_variedad)

            id_variedad = obtener_id_variedad(variedad_canonica, engine)
            if not id_variedad:
                procesador.registrar_rechazo(id_origen, 'VARIEDAD_Raw', variedad_raw, 'Variedad sin match en Dim_Variedad', tipo_regla='MDM')
                continue

            dni, _ = procesar_dni(evaluador_raw)
            clave_personal = None if dni is None else str(dni)
            if clave_personal not in procesador._cache_personal:
                procesador._cache_personal[clave_personal] = obtener_id_personal(dni, engine)
            id_personal = procesador._cache_personal[clave_personal]

            clave_cinta = None if color_raw is None else str(color_raw).strip().upper()
            if clave_cinta not in cache_cinta:
                cache_cinta[clave_cinta] = obtener_id_cinta(color_raw, engine)
            id_cinta = cache_cinta[clave_cinta]
            if not id_cinta:
                procesador.registrar_rechazo(id_origen, 'COLOR_Raw', color_raw if color_raw is not None else fila_dict.get('Valores_Raw'), 'Cinta no reconocida o ausente en maduracion')
                continue

            if fecha not in procesador._cache_tiempo:
                procesador._cache_tiempo[fecha] = obtener_id_tiempo(fecha)
            id_tiempo = procesador._cache_tiempo[fecha]
            if id_tiempo is None:
                procesador.registrar_rechazo(id_origen, 'FECHA_Raw', fecha_raw, 'Fecha sin match en Dim_Tiempo')
                continue

            fecha_registro_raw = _obtener_valor(payload, 'FECHAREGISTRO_Raw', 'Fecha_Registro_Raw') or str(fecha)
            payload_inserts.append({
                "id_origen_rastreo": id_origen,
                "ID_Personal": id_personal,
                "ID_Geografia": id_geografia,
                "ID_Tiempo": id_tiempo,
                "ID_Variedad": id_variedad,
                "ID_Estado_Fenologico": id_estado,
                "ID_Cinta": id_cinta,
                "ID_Organo": numero_organo,
                "Dias_Pasados_Del_Marcado": None,
                "Fecha_Evento": fecha,
                "Fecha_Sistema": pd.Timestamp.now(),
                "Estado_DQ": "OK",
                "_fecha_registro_dt": pd.to_datetime(fecha_registro_raw, errors='coerce'),
            })

        if payload_inserts:
            procesador._ejecutar_insercion_masiva_segura(contexto, payload_inserts, "#Temp_Fact_Maduracion")

        return procesador.finalizar_proceso(contexto)

    return _finalizar_resumen_fact(procesador.resumen)

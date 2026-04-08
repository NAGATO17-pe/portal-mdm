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

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from dq.cuarentena import enviar_a_cuarentena
from mdm.homologador import (
    cargar_catalogo_variedades,
    cargar_diccionario,
    homologar_valor,
)
from mdm.lookup import (
    obtener_id_cinta,
    obtener_id_estado_fenologico,
    obtener_id_geografia,
    obtener_id_personal,
    obtener_id_variedad,
)
from utils.dni import procesar_dni
from utils.fechas import obtener_id_tiempo, procesar_fecha
from utils.texto import es_test_block, normalizar_modulo
from silver.facts._helpers_fact_comunes import (
    parsear_valores_raw as _parsear_valores_raw,
)


TABLA_ORIGEN = 'Bronce.Maduracion'
TABLA_DESTINO = 'Silver.Fact_Maduracion'

MAPA_ESTADO_CICLO = {
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
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


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


def _resolver_estado_canonico(payload: dict[str, str]) -> str | None:
    descripcion = _extraer_texto(
        _obtener_valor(payload, 'DESCRIPCIONESTADOCICLO_Raw', 'DESCRIPCION_ESTADO_CICLO_Raw')
    )
    descripcion_normalizada = _normalizar_estado_crudo(descripcion)
    if descripcion_normalizada and descripcion_normalizada in MAPA_ESTADO_CICLO:
        return MAPA_ESTADO_CICLO[descripcion_normalizada]

    id_estado = _extraer_entero(_obtener_valor(payload, 'IDESTADOCICLO_Raw', 'ID_ESTADO_CICLO_Raw'))
    if id_estado is None:
        return None
    return MAPA_ESTADO_POR_ID.get(id_estado)


def _cargar_claves_existentes(engine: Engine) -> set[tuple[int, int, int, int, int]]:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Geografia,
                ID_Tiempo,
                ID_Variedad,
                ID_Cinta,
                ID_Organo
            FROM {TABLA_DESTINO}
        """))
        return {
            (
                int(fila.ID_Geografia),
                int(fila.ID_Tiempo),
                int(fila.ID_Variedad),
                int(fila.ID_Cinta),
                int(fila.ID_Organo),
            )
            for fila in resultado.fetchall()
        }


def cargar_fact_maduracion(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return resumen
    resumen['leidos'] = len(df)

    claves_existentes = _cargar_claves_existentes(engine)
    diccionario_variedades = cargar_diccionario(engine, TABLA_ORIGEN)
    catalogo_variedades = cargar_catalogo_variedades(engine)
    cache_variedades: dict[str, tuple[str | None, str]] = {}

    with engine.begin() as conexion:
        for _, fila in df.iterrows():
            id_origen = int(fila['ID_Maduracion'])
            payload = _parsear_valores_raw(fila.get('Valores_Raw'))

            fecha_raw = _obtener_valor(payload, 'FECHA_Raw', 'Fecha_Raw') or fila.get('Fecha_Raw')
            fecha, fecha_valida = procesar_fecha(
                fecha_raw,
                dominio='maduracion',
            )
            if not fecha_valida:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'FECHA_Raw',
                    'valor': fecha_raw,
                    'motivo': 'Fecha invalida o fuera de campana',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            modulo_raw = _obtener_valor(payload, 'MODULO_Raw', 'Modulo_Raw') or fila.get('Modulo_Raw')
            turno_raw = _obtener_valor(payload, 'TURNO_Raw', 'Turno_Raw') or fila.get('Turno_Raw')
            valvula_raw = _obtener_valor(payload, 'NROVALVULA_Raw', 'VALVULA_Raw', 'Valvula_Raw') or fila.get('Valvula_Raw')
            variedad_raw = _obtener_valor(payload, 'VARIEDAD_Raw', 'Variedad_Raw') or fila.get('Variedad_Raw')
            evaluador_raw = _obtener_valor(payload, 'USUARIO_Raw', 'EVALUADOR_Raw', 'Evaluador_Raw') or fila.get('Evaluador_Raw')
            color_raw = _extraer_texto(_obtener_valor(payload, 'COLOR_Raw', 'Color_Raw'))
            organo_raw = _obtener_valor(payload, 'ORGANO_Raw', 'Organo_Raw')

            numero_organo = _extraer_entero(organo_raw)
            if numero_organo is None or numero_organo < 1:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'ORGANO_Raw',
                    'valor': organo_raw if organo_raw is not None else fila.get('Valores_Raw'),
                    'motivo': 'ID_Organo invalido o ausente en maduracion',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            estado_canonico = _resolver_estado_canonico(payload)
            id_estado = obtener_id_estado_fenologico(estado_canonico, engine)
            if not id_estado:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'DESCRIPCIONESTADOCICLO_Raw',
                    'valor': _obtener_valor(payload, 'DESCRIPCIONESTADOCICLO_Raw', 'IDESTADOCICLO_Raw'),
                    'motivo': 'Estado fenologico no reconocido en maduracion',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            modulo = None if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
            id_geografia = obtener_id_geografia(
                None,
                None,
                modulo,
                engine,
                turno=turno_raw,
                valvula=valvula_raw,
                cama=None,
            )
            if not id_geografia:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'MODULO_Raw',
                    'valor': f"Modulo={modulo_raw} | Turno={turno_raw} | Valvula={valvula_raw}",
                    'motivo': 'Geografia no encontrada en Silver.Dim_Geografia.',
                    'severidad': 'ALTO',
                    'tipo_regla': 'MDM',
                    'id_registro_origen': id_origen,
                })
                continue

            variedad_token = '' if variedad_raw is None else str(variedad_raw).strip()
            if variedad_token in cache_variedades:
                variedad_canonica, _estado_variedad = cache_variedades[variedad_token]
            else:
                variedad_canonica, estado_variedad = homologar_valor(
                    variedad_raw,
                    TABLA_ORIGEN,
                    'VARIEDAD_Raw',
                    diccionario_variedades,
                    catalogo_variedades,
                    engine,
                )
                cache_variedades[variedad_token] = (variedad_canonica, estado_variedad)

            id_variedad = obtener_id_variedad(variedad_canonica, engine)
            if not id_variedad:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'VARIEDAD_Raw',
                    'valor': variedad_raw,
                    'motivo': 'Variedad sin match en Dim_Variedad',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            dni, _ = procesar_dni(evaluador_raw)
            id_personal = obtener_id_personal(dni, engine)

            id_cinta = obtener_id_cinta(color_raw, engine)
            if not id_cinta:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'COLOR_Raw',
                    'valor': color_raw if color_raw is not None else fila.get('Valores_Raw'),
                    'motivo': 'Cinta no reconocida o ausente en maduracion',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            id_tiempo = obtener_id_tiempo(fecha)
            if id_tiempo is None:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'FECHA_Raw',
                    'valor': fecha_raw,
                    'motivo': 'Fecha sin match en Dim_Tiempo',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            clave_unica = (
                int(id_geografia),
                int(id_tiempo),
                int(id_variedad),
                int(id_cinta),
                int(numero_organo),
            )
            if clave_unica in claves_existentes:
                continue

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Maduracion (
                    ID_Personal,
                    ID_Geografia,
                    ID_Tiempo,
                    ID_Variedad,
                    ID_Estado_Fenologico,
                    ID_Cinta,
                    ID_Organo,
                    Dias_Pasados_Del_Marcado,
                    Fecha_Evento,
                    Fecha_Sistema,
                    Estado_DQ
                ) VALUES (
                    :id_personal,
                    :id_geografia,
                    :id_tiempo,
                    :id_variedad,
                    :id_estado_fenologico,
                    :id_cinta,
                    :id_organo,
                    :dias_pasados,
                    :fecha_evento,
                    SYSDATETIME(),
                    'OK'
                )
            """), {
                'id_personal': id_personal,
                'id_geografia': id_geografia,
                'id_tiempo': id_tiempo,
                'id_variedad': id_variedad,
                'id_estado_fenologico': id_estado,
                'id_cinta': id_cinta,
                'id_organo': numero_organo,
                'dias_pasados': None,
                'fecha_evento': fecha,
            })
            claves_existentes.add(clave_unica)
            resumen['insertados'] += 1

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

"""
fact_conteo_fenologico.py
=========================
Carga Silver.Fact_Conteo_Fenologico desde Bronce.Conteo_Fruta.

Soporta dos layouts de origen:
1) Layout largo: Estado_Raw + Cantidad_Organos_Raw poblados.
2) Layout ancho: valores en Valores_Raw (Botones/Flores/Fases/etc.) y Punto_Raw.
"""

import re
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas import procesar_fecha, obtener_id_tiempo
from utils.texto import normalizar_modulo, es_test_block
from utils.dni import procesar_dni
from utils.sql_lotes import marcar_estado_carga_por_ids
from mdm.lookup import (
    obtener_id_geografia,
    obtener_id_variedad,
    obtener_id_personal,
    obtener_id_estado_fenologico,
    obtener_id_cinta,
)
from mdm.homologador import homologar_columna
from dq.cuarentena import enviar_a_cuarentena


TABLA_ORIGEN = 'Bronce.Conteo_Fruta'
TABLA_DESTINO = 'Silver.Fact_Conteo_Fenologico'

MAPA_PUNTO_COLOR = {
    1: 'Roja',
    2: 'Azul',
    3: 'Verde',
    4: 'Amarilla',
    5: 'Blanca',
    6: 'Naranja',
}

MAPA_ESTADOS_WIDE = {
    'Botones_Florales_Raw': 'Boton Floral',
    'Flores_Raw': 'Flor',
    'Bayas_Pequenas_Raw': 'Pequena',
    'Bayas_Grandes_Verdes_Raw': 'Verde',
    'Fase1_Raw': 'Inicio F1',
    'Fase2_Raw': 'Inicio F2',
    'Bayas_Cremas_Raw': 'Crema',
    'Bayas_Maduras_Raw': 'Madura',
    'Bayas_Cosechables_Raw': 'Cosechable',
}


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Conteo_Fruta,
                Fecha_Raw,
                Fundo_Raw,
                Sector_Raw,
                Modulo_Raw,
                Turno_Raw,
                Valvula_Raw,
                Variedad_Raw,
                Evaluador_Raw,
                Color_Cinta_Raw,
                Estado_Raw,
                Cantidad_Organos_Raw,
                Tipo_Evaluacion_Raw,
                Valores_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _parsear_valores_raw(texto: str | None) -> dict[str, str]:
    if texto is None:
        return {}

    crudo = str(texto).strip()
    if not crudo:
        return {}

    resultado: dict[str, str] = {}
    partes = re.split(r'\s*\|\s*', crudo)
    for parte in partes:
        if '=' not in parte:
            continue
        clave, valor = parte.split('=', 1)
        clave = str(clave).strip()
        valor = str(valor).strip()
        if clave:
            resultado[clave] = valor
    return resultado


def _normalizar_cantidad(valor) -> int:
    try:
        return int(float(str(valor)))
    except (ValueError, TypeError):
        return 0


def _resolver_color_cinta(fila: pd.Series) -> str | None:
    color = fila.get('Color_Cinta_Raw')
    if color is not None and str(color).strip():
        return str(color).strip()

    valores = _parsear_valores_raw(fila.get('Valores_Raw'))
    punto = valores.get('Punto_Raw')
    if punto is None or str(punto).strip() == '':
        return None

    try:
        punto_int = int(float(str(punto)))
    except (ValueError, TypeError):
        return None

    return MAPA_PUNTO_COLOR.get(punto_int)


def _extraer_estados_desde_fila(fila: pd.Series) -> list[tuple[str, int]]:
    estado_raw = fila.get('Estado_Raw')
    cantidad_raw = fila.get('Cantidad_Organos_Raw')

    if estado_raw is not None and str(estado_raw).strip() and cantidad_raw is not None and str(cantidad_raw).strip():
        return [(str(estado_raw).strip(), _normalizar_cantidad(cantidad_raw))]

    valores = _parsear_valores_raw(fila.get('Valores_Raw'))
    estados: list[tuple[str, int]] = []
    for clave_wide, nombre_estado in MAPA_ESTADOS_WIDE.items():
        if clave_wide not in valores:
            continue
        cantidad = _normalizar_cantidad(valores.get(clave_wide))
        estados.append((nombre_estado, cantidad))
    return estados


def cargar_fact_conteo_fenologico(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return resumen

    df, cuarentenas_var = homologar_columna(
        df,
        'Variedad_Raw',
        'Variedad_Canonica',
        TABLA_ORIGEN,
        engine,
        columna_id_origen='ID_Conteo_Fruta',
    )
    resumen['cuarentena'].extend(cuarentenas_var)

    ids_procesados: set[int] = set()

    with engine.begin() as conexion:
        for _, fila in df.iterrows():
            id_origen = int(fila['ID_Conteo_Fruta'])

            fecha, fecha_valida = procesar_fecha(fila.get('Fecha_Raw'))
            if not fecha_valida:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'Fecha_Raw',
                    'valor': fila.get('Fecha_Raw'),
                    'motivo': 'Fecha invalida o fuera de campana',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            id_tiempo = obtener_id_tiempo(fecha)

            modulo_raw = fila.get('Modulo_Raw')
            test_block = es_test_block(modulo_raw)
            modulo = None if test_block else normalizar_modulo(modulo_raw)

            id_geo = obtener_id_geografia(
                fila.get('Fundo_Raw'),
                fila.get('Sector_Raw'),
                modulo,
                engine,
                turno=fila.get('Turno_Raw'),
                valvula=fila.get('Valvula_Raw'),
                cama=None,
            )
            if not id_geo:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'Modulo_Raw',
                    'valor': f"Modulo={modulo_raw} | Turno={fila.get('Turno_Raw')} | Valvula={fila.get('Valvula_Raw')}",
                    'motivo': 'Geografia no encontrada en Dim_Geografia',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            id_variedad = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)
            if not id_variedad:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'Variedad_Raw',
                    'valor': fila.get('Variedad_Raw'),
                    'motivo': 'Variedad sin match en Dim_Variedad',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            dni, _ = procesar_dni(fila.get('Evaluador_Raw'))
            id_personal = obtener_id_personal(dni, engine)

            color_cinta = _resolver_color_cinta(fila)
            id_cinta = obtener_id_cinta(color_cinta, engine)
            if not id_cinta:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'Color_Cinta_Raw',
                    'valor': color_cinta if color_cinta is not None else fila.get('Valores_Raw'),
                    'motivo': 'Cinta no reconocida o no disponible',
                    'severidad': 'MEDIO',
                    'id_registro_origen': id_origen,
                })
                continue

            estados_cantidades = _extraer_estados_desde_fila(fila)
            if not estados_cantidades:
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna': 'Valores_Raw',
                    'valor': fila.get('Valores_Raw'),
                    'motivo': 'No se encontraron estados fenologicos/cantidades en fila',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            insertados_fila = 0
            for estado_raw, cantidad in estados_cantidades:
                id_estado = obtener_id_estado_fenologico(estado_raw, engine)
                if not id_estado:
                    resumen['cuarentena'].append({
                        'columna': 'Estado_Raw',
                        'valor': estado_raw,
                        'motivo': 'Estado fenologico no reconocido',
                        'severidad': 'ALTO',
                        'id_registro_origen': id_origen,
                    })
                    continue

                conexion.execute(text("""
                    INSERT INTO Silver.Fact_Conteo_Fenologico (
                        ID_Geografia, ID_Tiempo, ID_Variedad,
                        ID_Personal, ID_Cinta, ID_Estado_Fenologico,
                        Cantidad_Organos,
                        Fecha_Evento, Fecha_Sistema, Estado_DQ
                    ) VALUES (
                        :id_geo, :id_tiempo, :id_variedad,
                        :id_personal, :id_cinta, :id_estado,
                        :cantidad,
                        :fecha_evento, SYSDATETIME(), 'OK'
                    )
                """), {
                    'id_geo': id_geo,
                    'id_tiempo': id_tiempo,
                    'id_variedad': id_variedad,
                    'id_personal': id_personal,
                    'id_cinta': id_cinta,
                    'id_estado': id_estado,
                    'cantidad': cantidad,
                    'fecha_evento': fecha,
                })
                insertados_fila += 1

            if insertados_fila > 0:
                ids_procesados.add(id_origen)
                resumen['insertados'] += insertados_fila
            else:
                resumen['rechazados'] += 1

    if ids_procesados:
        marcar_estado_carga_por_ids(
            engine,
            TABLA_ORIGEN,
            'ID_Conteo_Fruta',
            sorted(ids_procesados),
        )

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

"""
fact_conteo_fenologico.py
=========================
Carga Silver.Fact_Conteo_Fenologico desde Bronce.Conteo_Fruta.

Soporta dos layouts de origen:
1) Layout largo: Estado_Raw + Cantidad_Organos_Raw poblados.
2) Layout ancho: valores en Valores_Raw (Botones/Flores/Fases/etc.) y Punto_Raw.
"""

import re
import unicodedata
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.fechas import procesar_fecha, obtener_id_tiempo
from utils.texto import normalizar_modulo, es_test_block
from utils.dni import procesar_dni
from utils.sql_lotes import ejecutar_en_lotes
from mdm.lookup import (
    resolver_geografia,
    obtener_id_variedad,
    obtener_id_personal,
    obtener_id_estado_fenologico,
)
from mdm.homologador import homologar_columna
from silver.facts._helpers_fact_comunes import (
    finalizar_resumen_fact as _finalizar_resumen_fact,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    parsear_valores_raw as _parsear_valores_raw,
    registrar_rechazo as _registrar_rechazo,
)


TABLA_ORIGEN = 'Bronce.Conteo_Fruta'
TABLA_DESTINO = 'Silver.Fact_Conteo_Fenologico'

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

SQL_INSERT_FACT = text("""
    INSERT INTO Silver.Fact_Conteo_Fenologico (
        ID_Geografia, ID_Tiempo, ID_Variedad,
        ID_Personal, ID_Estado_Fenologico,
        Cantidad_Organos,
        Fecha_Evento, Fecha_Sistema, Estado_DQ
    ) VALUES (
        :id_geo, :id_tiempo, :id_variedad,
        :id_personal, :id_estado,
        :cantidad,
        :fecha_evento, SYSDATETIME(), 'OK'
    )
""")


def _normalizar_tipo_evaluacion(valor) -> str:
    texto = str(valor or '').strip()
    if not texto:
        return ''
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.casefold()
    texto = re.sub(r'[^a-z0-9]+', ' ', texto)
    return re.sub(r'\s+', ' ', texto).strip()


def _es_evaluacion_compatible_con_conteo(valor) -> bool:
    """
    Acepta filas del layout real de conteo de flores, incluyendo variantes
    operativas observadas en el Excel de campo.

    Casos validados:
    - CONTEO DE FLORES
    - ENSAYO DE CONTEO
    - PODA GENERAL

    Si la fuente no trae evaluacion, conserva compatibilidad hacia atras.
    """
    normalizado = _normalizar_tipo_evaluacion(valor)
    if not normalizado:
        return True
    if 'conteo' in normalizado or 'fenolog' in normalizado:
        return True
    if normalizado == 'poda general':
        return True
    return False


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


def _normalizar_cantidad(valor) -> int:
    try:
        return int(float(str(valor)))
    except (ValueError, TypeError):
        return 0


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
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df)

    ids_procesados: set[int] = set()
    ids_rechazados: set[int] = set()
    payload_inserts = []

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()

        df, cuarentenas_var = homologar_columna(
            df,
            'Variedad_Raw',
            'Variedad_Canonica',
            TABLA_ORIGEN,
            conexion,
            columna_id_origen='ID_Conteo_Fruta',
        )
        resumen['cuarentena'].extend(cuarentenas_var)

        for _, fila in df.iterrows():
            id_origen = int(fila['ID_Conteo_Fruta'])
            tipo_evaluacion_raw = fila.get('Tipo_Evaluacion_Raw')

            if not _es_evaluacion_compatible_con_conteo(tipo_evaluacion_raw):
                resumen['rechazados'] += 1
                ids_rechazados.add(id_origen)
                resumen['cuarentena'].append({
                    'columna': 'Tipo_Evaluacion_Raw',
                    'valor': tipo_evaluacion_raw,
                    'motivo': 'Evaluacion no compatible con Fact_Conteo_Fenologico',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            fecha, fecha_valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='conteo_fenologico',
            )
            if not fecha_valida:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha invalida o fuera de campana',
                )
                continue

            id_tiempo = obtener_id_tiempo(fecha)

            modulo_raw = fila.get('Modulo_Raw')
            test_block = es_test_block(modulo_raw)
            modulo = None if test_block else normalizar_modulo(modulo_raw)

            resultado_geo = resolver_geografia(
                fila.get('Fundo_Raw'),
                fila.get('Sector_Raw'),
                modulo,
                engine,
                turno=fila.get('Turno_Raw'),
                valvula=fila.get('Valvula_Raw'),
                cama=None,
            )
            id_geo = resultado_geo.get('id_geografia')
            if not id_geo:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Modulo_Raw',
                    valor=f"Modulo={modulo_raw} | Turno={fila.get('Turno_Raw')} | Valvula={fila.get('Valvula_Raw')}",
                    motivo=_motivo_cuarentena_geografia(resultado_geo),
                    tipo_regla='MDM',
                )
                continue

            id_variedad = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)
            if not id_variedad:
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

            dni, _ = procesar_dni(fila.get('Evaluador_Raw'))
            id_personal = obtener_id_personal(dni, engine)

            estados_cantidades = _extraer_estados_desde_fila(fila)
            if not estados_cantidades:
                resumen['rechazados'] += 1
                ids_rechazados.add(id_origen)
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

                payload_inserts.append({
                    'id_geo': id_geo,
                    'id_tiempo': id_tiempo,
                    'id_variedad': id_variedad,
                    'id_personal': id_personal,
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
                ids_rechazados.add(id_origen)

        if payload_inserts:
            ejecutar_en_lotes(conexion, SQL_INSERT_FACT, payload_inserts)

        if ids_procesados:
            contexto.marcar_estado_carga(
                TABLA_ORIGEN,
                'ID_Conteo_Fruta',
                sorted(ids_procesados),
            )
        if ids_rechazados:
            contexto.marcar_estado_carga(
                TABLA_ORIGEN,
                'ID_Conteo_Fruta',
                sorted(ids_rechazados),
                estado='RECHAZADO',
            )

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_ORIGEN, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)

"""
cargador.py
===========
Lee archivos Excel de campo e inserta en Bronce como NVARCHAR raw.
Aplica validacion de layout critico cuando la operacion lo exige.
"""

import shutil
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from bronce.rutas import (
    CARPETA_PROCESADOS,
    CARPETA_RECHAZADOS,
    listar_carpetas_con_archivos,
)
from config.conexion import obtener_engine
from auditoria.log import registrar_inicio, registrar_fin


_CACHE_COLUMNAS_BRONCE: dict[str, set[str]] = {}


_FIRMAS_LAYOUT_CRITICO: dict[str, dict[str, Any]] = {
    'Bronce.Evaluacion_Vegetativa': {
        'ruta_canonica': 'evaluacion_vegetativa',
        'columnas_obligatorias': {
            'Fecha_Raw',
            'DNI_Raw',
            'Modulo_Raw',
            'Turno_Raw',
            'Valvula_Raw',
            'Cama_Raw',
            'Descripcion_Raw',
            'Evaluacion_Raw',
            'N_Plantas_Evaluadas_Raw',
            'N_Plantas_en_Floracion_Raw',
        },
        'columnas_incompatibles': {
            'Altura_Raw',
            'Tallos_basales_Raw',
            'Tallos_basales_nuevos_Raw',
            'Brotes_Generales_1_Raw',
            'Brotes_Generales_2_Raw',
            'Brotes_Generales_3_Raw',
            'Brotes_Generales_4_Raw',
            'Brotes_Productivos_Totales_Raw',
            'Brotes_Productivos_1_Raw',
            'Brotes_Productivos_2_Raw',
            'Brotes_Productivos_3_Raw',
            'Brotes_Productivos_4_Raw',
            'Diametro_brote1_Raw',
            'Diametro_brote2_Raw',
        },
        'motivo': (
            'El layout recibido no corresponde al fact actual de Evaluacion Vegetativa. '
            'Faltan columnas de plantas y aparecen metricas de brotes/altura/diametro.'
        ),
    },
    'Bronce.Fisiologia': {
        'ruta_canonica': 'fisiologia',
        'columnas_obligatorias': {
            'Fecha_Raw',
            'Fundo_Raw',
            'Modulo_Raw',
            'Variedad_Raw',
            'Tercio_Raw',
            'Hinchadas_Raw',
            'Productivas_Raw',
            'Total_Org_Raw',
            'Brote_Raw',
        },
        'columnas_incompatibles': {
            'DNI_Raw',
            'Turno_Raw',
            'Valvula_Raw',
            'Cama_Raw',
            'Descripcion_Raw',
            'Evaluacion_Raw',
            'Altura_Raw',
            'Tallos_basales_Raw',
            'Tallos_basales_nuevos_Raw',
            'Brotes_Generales_1_Raw',
            'Brotes_Generales_2_Raw',
            'Brotes_Generales_3_Raw',
            'Brotes_Generales_4_Raw',
            'Brotes_Productivos_Totales_Raw',
            'Brotes_Productivos_1_Raw',
            'Brotes_Productivos_2_Raw',
            'Brotes_Productivos_3_Raw',
            'Brotes_Productivos_4_Raw',
            'Diametro_brote1_Raw',
            'Diametro_brote2_Raw',
        },
        'motivo': (
            'El layout recibido no corresponde al fact actual de Fisiologia. '
            'Se detectaron columnas de evaluacion vegetativa o geografia operativa no compatibles.'
        ),
    },
}

_FIRMAS_RUTA_SUGERIDA: dict[str, dict[str, Any]] = {
    'evaluacion_vegetativa': {
        'tabla_destino': 'Bronce.Evaluacion_Vegetativa',
        'columnas_clave': {
            'Fecha_Raw',
            'DNI_Raw',
            'Modulo_Raw',
            'Turno_Raw',
            'Valvula_Raw',
            'Cama_Raw',
            'Descripcion_Raw',
            'Evaluacion_Raw',
            'N_Plantas_Evaluadas_Raw',
            'N_Plantas_en_Floracion_Raw',
        },
    },
    'evaluacion_pesos': {
        'tabla_destino': 'Bronce.Evaluacion_Pesos',
        'columnas_clave': {
            'Fecha_Raw',
            'DNI_Raw',
            'Modulo_Raw',
            'Turno_Raw',
            'Valvula_Raw',
            'Cama_Raw',
            'Variedad_Raw',
            'BayasPequenas_Raw',
            'PesoBayasPequenas_Raw',
            'Cosechables_Raw',
            'PesoCosechables_Raw',
        },
    },
    'evaluacion_calidad_poda': {
        'tabla_destino': 'Bronce.Evaluacion_Calidad_Poda',
        'columnas_clave': {
            'Fecha_Raw',
            'Fundo_Raw',
            'Modulo_Raw',
            'Turno_Raw',
            'Valvula_Raw',
            'Variedad_Raw',
            'Tipo_Evaluacion_Raw',
            'TallosPlanta_Raw',
            'LongitudTallo_Raw',
            'DiametroTallo_Raw',
            'RamillaPlanta_Raw',
            'ToconesPlanta_Raw',
            'CortesDefectuosos_Raw',
            'AlturaPoda_Raw',
        },
    },
}


# Mapeo de nombres de columnas comunes del Excel a nombres estándar del ETL.
# Se aplica DESPUÉS de reemplazar espacios por _ y eliminar acentos.
# Clave : nombre ya sin acentos/espacios (sin _Raw)
# Valor : nombre esperado por los scripts Silver (sin _Raw)
_ALIAS_COLUMNAS: dict[str, str] = {
    # Fechas
    'Fecha_de_evaluacion':        'Fecha',
    'Fecha_de_evaluaci_n':        'Fecha',
    'Fecha_evaluacion':           'Fecha',
    'Fecha':                      'Fecha',
    'fecha':                      'Fecha',
    # Fecha de subida
    'Fecha_de_subida':            'Fecha_Subida',
    'FechaSubida':                'Fecha_Subida',
    # Fundo
    'Fundo':                      'Fundo',
    'fundo':                      'Fundo',
    # Modulo
    'Modulo':                     'Modulo',
    'modulo':                     'Modulo',
    # Valvula
    'Valvula':                    'Valvula',
    'Cama':                       'Cama',
    'N_cama':                     'Cama',
    'N_de_cama':                  'Cama',
    # Turno
    'Turno':                      'Turno',
    'turno':                      'Turno',
    # Variedad
    'Variedad':                   'Variedad',
    'variedad':                   'Variedad',
    # Evaluacion
    'Evaluacion':                 'Evaluacion',
    'evaluacion':                 'Evaluacion',
    # DNI / Personal
    'DNI':                        'DNI',
    'dni':                        'DNI',
    'Nombres':                    'Nombres',
    'Nombre':                     'Nombres',
    'Evaluador':                  'Evaluador',
    # Sector
    'Sector':                     'Sector',
    'sector':                     'Sector',
    # Pesos bayas (Reporte_evaluacion_peso.xlsx) — post normalizacion de acentos
    'Bayas_pequenas':             'BayasPequenas',
    'Peso_bayas_pequenas':        'PesoBayasPequenas',
    'Peso_bayas_pequenas1':       'PesoBayasPequenas2',  # header Excel: "Peso bayas pequeñas.1"
    'Peso_bayas_pequenas2':       'PesoBayasPequenas2',
    'Bayas_grandes':              'BayasGrandes',
    'Peso_bayas_grandes':         'PesoBayasGrandes',
    'Bayas_fase_1':               'BayasFase1',
    'Peso_bayas_fase_1':          'PesoBayasFase1',
    'Bayas_fase_2':               'BayasFase2',
    'Peso_bayas_fase_2':          'PesoBayasFase2',
    'Cremas':                     'Cremas',
    'Peso_cremas':                'PesoCremas',
    'Maduras':                    'Maduras',
    'Peso_maduras':               'PesoMaduras',
    'Cosechables':                'Cosechables',
    'Peso_cosechables':           'PesoCosechables',
    'PesoBaya':                   'PesoBaya',
    'CantMuestra':                'CantMuestra',
}


def _alias(col_snake: str) -> str:
    """Retorna el alias estándar si existe, o la misma col_snake."""
    return _ALIAS_COLUMNAS.get(col_snake, col_snake)


def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas del Excel:
    - Quita espacios / caracteres especiales
    - Aplica alias estándares (Fecha de evaluación → Fecha_Raw)
    - Agrega sufijo _Raw si no lo tiene
    """
    columnas_nuevas = {}
    for col in df.columns:
        col_snake = (
            str(col)
            .strip()
            .replace(' ', '_')
            .replace('ó', 'o')
            .replace('á', 'a')
            .replace('é', 'e')
            .replace('í', 'i')
            .replace('ú', 'u')
            .replace('ñ', 'n')
            .replace('°', '')
            .replace('.', '')
        )
        col_snake = _alias(col_snake)
        if not col_snake.endswith('_Raw'):
            col_snake = f'{col_snake}_Raw'
        columnas_nuevas[col] = col_snake
    return df.rename(columns=columnas_nuevas)


def castear_todo_a_texto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte todas las columnas a string (vectorizado).
    None y NaN se convierten a None (NULL en SQL).
    Bronce nunca tipifica — todo es NVARCHAR.
    """
    for col in df.columns:
        mask_nulo = df[col].isna()
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
        )
        df[col] = np.where(mask_nulo, None, df[col])
    return df


def agregar_columnas_sistema(df: pd.DataFrame,
                              nombre_archivo: str) -> pd.DataFrame:
    """
    Agrega columnas de infraestructura que no vienen del Excel:
    - Fecha_Sistema  : timestamp de carga
    - Nombre_Archivo : nombre del archivo fuente
    - Estado_Carga   : estado inicial
    """
    ahora = datetime.now()
    df['Fecha_Sistema'] = ahora
    df['Nombre_Archivo'] = nombre_archivo
    df['Estado_Carga'] = 'CARGADO'
    return df


def insertar_en_bronce(df: pd.DataFrame,
                        tabla: str,
                        engine) -> int:
    """
    Inserta el DataFrame en la tabla Bronce indicada.
    Retorna el número de filas insertadas.
    """
    esquema, nombre_tabla = tabla.split('.')

    df.to_sql(
        name=nombre_tabla,
        con=engine,
        schema=esquema,
        if_exists='append',
        index=False,
        chunksize=1000,  # fast_executemany=True ya está en el engine
    )
    return len(df)


def archivar_archivo(ruta_archivo: Path, nombre_carpeta: str) -> None:
    """
    Mueve el archivo procesado a data/procesados/nombre_carpeta/
    con timestamp en el nombre para no sobrescribir.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_nuevo = f'{ruta_archivo.stem}_{timestamp}{ruta_archivo.suffix}'
    destino = CARPETA_PROCESADOS / nombre_carpeta / nombre_nuevo
    shutil.move(str(ruta_archivo), str(destino))


def archivar_archivo_rechazado(ruta_archivo: Path,
                               nombre_carpeta: str,
                               codigo_rechazo: str) -> Path:
    """
    Mueve el archivo rechazado a data/rechazados/nombre_carpeta/
    preservando trazabilidad del motivo en el nombre.
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    sufijo = str(codigo_rechazo or 'RECHAZADO').strip().replace(' ', '_')
    nombre_nuevo = f'{ruta_archivo.stem}_{sufijo}_{timestamp}{ruta_archivo.suffix}'
    destino = CARPETA_RECHAZADOS / nombre_carpeta / nombre_nuevo
    destino.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(ruta_archivo), str(destino))
    return destino


def _obtener_columnas_bronce(tabla_destino: str, engine) -> set[str]:
    """
    Devuelve columnas fisicas de la tabla Bronce destino.
    Usa cache en memoria para evitar query repetida en cada archivo.
    """
    if tabla_destino in _CACHE_COLUMNAS_BRONCE:
        return _CACHE_COLUMNAS_BRONCE[tabla_destino]

    esquema, tabla = tabla_destino.split('.')
    consulta = text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :esquema
          AND TABLE_NAME = :tabla
    """)
    with engine.connect() as conexion:
        filas = conexion.execute(
            consulta,
            {'esquema': esquema, 'tabla': tabla},
        ).fetchall()

    columnas = {str(fila[0]) for fila in filas}
    _CACHE_COLUMNAS_BRONCE[tabla_destino] = columnas
    return columnas


def _columnas_normalizadas_excel(ruta_archivo: Path, header_idx: int) -> list[str]:
    """
    Lee solo encabezados del Excel y devuelve nombres normalizados (con _Raw).
    """
    df = pd.read_excel(
        str(ruta_archivo),
        header=header_idx,
        dtype=str,
        nrows=0,
        engine='openpyxl',
    )
    df = normalizar_columnas(df)
    return [str(col) for col in df.columns]


def _detectar_header_idx(ruta_archivo: Path, tabla_destino: str, engine) -> int:
    """
    Detecta si encabezado real esta en fila 0 o fila 1 comparando
    columnas del Excel vs columnas reales de la tabla Bronce.
    """
    columnas_bronce = _obtener_columnas_bronce(tabla_destino, engine)
    columnas_raw = {col for col in columnas_bronce if str(col).endswith('_Raw')}

    if not columnas_raw:
        return 0

    candidatos = {}
    for idx in (0, 1):
        try:
            cols = _columnas_normalizadas_excel(ruta_archivo, idx)
        except Exception:
            continue
        candidatos[idx] = cols

    if not candidatos:
        return 0

    mejor_idx = 0
    mejor_score = (-1, -1, -1)

    for idx, cols in candidatos.items():
        set_cols = set(cols)
        match = len(set_cols & columnas_raw)
        unnamed = sum(1 for col in cols if str(col).lower().startswith('unnamed'))
        desconocidas = len(set_cols - columnas_raw)
        score = (match, -unnamed, -desconocidas)
        if score > mejor_score:
            mejor_idx = idx
            mejor_score = score

    return mejor_idx


def _serializar_valores_extra(df: pd.DataFrame, columnas_extra: list[str]) -> pd.Series:
    """
    Serializa columnas no mapeadas en formato "col=valor | col2=valor2".
    """
    if not columnas_extra:
        return pd.Series([None] * len(df), index=df.index)

    def _serializar_fila(fila: pd.Series) -> str | None:
        partes = []
        for columna in columnas_extra:
            valor = fila.get(columna)
            if valor is None:
                continue
            texto = str(valor).strip()
            if not texto or texto.lower() == 'none':
                continue
            partes.append(f'{columna}={texto}')
        return ' | '.join(partes) if partes else None

    return df.apply(_serializar_fila, axis=1)


def _alinear_dataframe_a_tabla(
    df: pd.DataFrame,
    tabla_destino: str,
    engine,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Alinea columnas del DataFrame con columnas fisicas de la tabla SQL.
    - Aplica mapeos de compatibilidad entre layouts de Excel.
    - Descarta columnas no existentes en SQL.
    - Si existe Valores_Raw, serializa columnas descartadas para no perder dato.
    """
    columnas_tabla = _obtener_columnas_bronce(tabla_destino, engine)
    if not columnas_tabla:
        return df, []

    # Compatibilidad entre layouts de reportes y DDL legado.
    if 'Evaluador_Raw' in columnas_tabla and 'Evaluador_Raw' not in df.columns and 'Nombres_Raw' in df.columns:
        df['Evaluador_Raw'] = df['Nombres_Raw']
    if 'Variedad_Raw' in columnas_tabla and 'Variedad_Raw' not in df.columns and 'Descripcion_Raw' in df.columns:
        df['Variedad_Raw'] = df['Descripcion_Raw']
    if 'Semanas_Poda_Raw' in columnas_tabla and 'Semanas_Poda_Raw' not in df.columns and 'N_de_cama_Raw' in df.columns:
        df['Semanas_Poda_Raw'] = df['N_de_cama_Raw']

    columnas_extra = [col for col in df.columns if col not in columnas_tabla]

    if columnas_extra and 'Valores_Raw' in columnas_tabla:
        extras_serializados = _serializar_valores_extra(df, columnas_extra)
        if 'Valores_Raw' in df.columns:
            base = df['Valores_Raw'].astype(str).replace({'None': ''}).fillna('')
            extra = extras_serializados.astype(str).replace({'None': ''}).fillna('')
            combinado = (base.str.strip() + ' | ' + extra.str.strip()).str.strip(' |')
            df['Valores_Raw'] = combinado.replace('', None)
        else:
            df['Valores_Raw'] = extras_serializados

    columnas_insertables = [col for col in df.columns if col in columnas_tabla]
    if not columnas_insertables:
        return df.iloc[:, 0:0], columnas_extra

    return df[columnas_insertables], columnas_extra


def _detectar_ruta_sugerida(
    columnas_detectadas: set[str],
    nombre_carpeta_actual: str,
) -> tuple[str | None, float]:
    mejor_ruta = None
    mejor_score = 0.0

    for ruta, firma in _FIRMAS_RUTA_SUGERIDA.items():
        if ruta == nombre_carpeta_actual:
            continue

        columnas_clave = set(firma.get('columnas_clave', set()))
        if not columnas_clave:
            continue

        coincidencias = len(columnas_detectadas & columnas_clave)
        score = coincidencias / len(columnas_clave)
        if score >= 0.8 and coincidencias >= 6 and score > mejor_score:
            mejor_ruta = ruta
            mejor_score = score

    return mejor_ruta, mejor_score


def _score_ruta_actual(
    columnas_detectadas: set[str],
    nombre_carpeta_actual: str,
) -> float:
    firma = _FIRMAS_RUTA_SUGERIDA.get(nombre_carpeta_actual)
    if not firma:
        return 0.0

    columnas_clave = set(firma.get('columnas_clave', set()))
    if not columnas_clave:
        return 0.0

    coincidencias = len(columnas_detectadas & columnas_clave)
    return coincidencias / len(columnas_clave)


def _validar_layout_critico(
    nombre_carpeta: str,
    tabla_destino: str,
    columnas_detectadas: set[str],
) -> dict | None:
    firma = _FIRMAS_LAYOUT_CRITICO.get(tabla_destino)
    if not firma:
        return None

    columnas_obligatorias = set(firma.get('columnas_obligatorias', set()))
    faltantes = sorted(columnas_obligatorias - columnas_detectadas)
    columnas_incompatibles = sorted(columnas_detectadas & set(firma.get('columnas_incompatibles', set())))

    if not faltantes:
        return None

    if not columnas_incompatibles:
        return None

    ruta_sugerida, _ = _detectar_ruta_sugerida(columnas_detectadas, nombre_carpeta)
    columnas_clave = sorted(
        columnas_detectadas & (columnas_obligatorias | set(firma.get('columnas_incompatibles', set())))
    )
    detalle_ruta = (
        f'Ruta sugerida: {ruta_sugerida}'
        if ruta_sugerida
        else 'Ruta sugerida: sin ruta compatible conocida en el ETL actual'
    )

    return {
        'codigo': 'LAYOUT_INCOMPATIBLE',
        'tabla': tabla_destino,
        'ruta_recibida': nombre_carpeta,
        'faltantes': faltantes,
        'columnas_detectadas_clave': columnas_clave,
        'ruta_sugerida': ruta_sugerida,
        'mensaje': (
            f'LAYOUT_INCOMPATIBLE | archivo incompatible con {nombre_carpeta} -> {tabla_destino}. '
            f'Faltantes: {", ".join(faltantes)} | '
            f'Detectadas clave: {", ".join(columnas_clave) if columnas_clave else "ninguna"} | '
            f'Motivo: {firma["motivo"]} | {detalle_ruta}'
        ),
    }


def _validar_enrutamiento_global(
    nombre_carpeta: str,
    tabla_destino: str,
    columnas_detectadas: set[str],
) -> dict | None:
    ruta_sugerida, score_sugerida = _detectar_ruta_sugerida(columnas_detectadas, nombre_carpeta)
    if not ruta_sugerida:
        return None

    score_actual = _score_ruta_actual(columnas_detectadas, nombre_carpeta)
    if score_actual >= 0.5:
        return None

    columnas_clave_sugeridas = sorted(
        columnas_detectadas & set(_FIRMAS_RUTA_SUGERIDA[ruta_sugerida].get('columnas_clave', set()))
    )
    return {
        'codigo': 'RUTA_CONTENIDO_INCOMPATIBLE',
        'tabla': tabla_destino,
        'ruta_recibida': nombre_carpeta,
        'ruta_sugerida': ruta_sugerida,
        'mensaje': (
            f'RUTA_CONTENIDO_INCOMPATIBLE | archivo recibido en {nombre_carpeta} -> {tabla_destino}, '
            f'pero su contenido coincide con alta confianza con la ruta {ruta_sugerida}. '
            f'Score actual: {score_actual:.2f} | Score sugerido: {score_sugerida:.2f} | '
            f'Columnas clave detectadas: {", ".join(columnas_clave_sugeridas) if columnas_clave_sugeridas else "ninguna"}'
        ),
    }


def cargar_archivo(nombre_carpeta: str,
                   ruta_archivo: Path,
                   tabla_destino: str,
                   engine) -> dict:
    """
    Carga un archivo Excel a su tabla Bronce destino.
    Retorna resumen del resultado.
    """
    resultado = {
        'archivo':   ruta_archivo.name,
        'tabla':     tabla_destino,
        'filas':     0,
        'estado':    'ERROR',
        'mensaje':   '',
    }

    try:
        # Leer Excel — todo como string desde el inicio
        header_idx = _detectar_header_idx(ruta_archivo, tabla_destino, engine)
        df = pd.read_excel(str(ruta_archivo), header=header_idx, dtype=str, engine='openpyxl')

        if df.empty:
            resultado['mensaje'] = 'Archivo vacío — sin filas para cargar'
            resultado['estado'] = 'VACIO'
            return resultado

        # Normalizar columnas → agregar _Raw
        df = normalizar_columnas(df)

        validacion_layout = _validar_layout_critico(
            nombre_carpeta,
            tabla_destino,
            {str(col) for col in df.columns},
        )
        if not validacion_layout:
            validacion_layout = _validar_enrutamiento_global(
                nombre_carpeta,
                tabla_destino,
                {str(col) for col in df.columns},
            )
        if validacion_layout:
            ruta_rechazada = archivar_archivo_rechazado(
                ruta_archivo,
                nombre_carpeta,
                validacion_layout['codigo'],
            )
            resultado['estado'] = 'ERROR'
            resultado['critico'] = True
            resultado['codigo'] = validacion_layout['codigo']
            resultado['ruta_sugerida'] = validacion_layout['ruta_sugerida']
            resultado['mensaje'] = (
                f'{validacion_layout["mensaje"]} | '
                f'Archivo movido a rechazados: {ruta_rechazada}'
            )
            return resultado

        # Castear todo a texto (NVARCHAR)
        df = castear_todo_a_texto(df)

        # Agregar columnas de sistema
        df = agregar_columnas_sistema(df, ruta_archivo.name)

        # Alinear columnas a la tabla SQL destino (Bronce tolerante a layout).
        df, columnas_descartadas = _alinear_dataframe_a_tabla(df, tabla_destino, engine)
        if df.shape[1] == 0:
            resultado['estado'] = 'ERROR'
            resultado['mensaje'] = (
                'No hay columnas insertables en la tabla destino. '
                f'Columnas no mapeadas: {len(columnas_descartadas)}'
            )
            return resultado

        # Insertar en Bronce
        filas_insertadas = insertar_en_bronce(df, tabla_destino, engine)

        # Archivar archivo procesado
        archivar_archivo(ruta_archivo, nombre_carpeta)

        resultado['filas']   = filas_insertadas
        resultado['estado']  = 'OK'
        resultado['mensaje'] = f'{filas_insertadas} filas insertadas en {tabla_destino}'
        if columnas_descartadas:
            resultado['mensaje'] += f' | columnas extras: {len(columnas_descartadas)}'

    except Exception as error:
        resultado['mensaje'] = str(error)
        resultado['estado']  = 'ERROR'

    return resultado


def ejecutar_carga_bronce() -> list[dict]:
    """
    Punto de entrada del módulo Bronce.
    Busca todos los archivos pendientes y los carga a sus tablas destino.
    Retorna lista de resultados por archivo.
    """
    engine = obtener_engine()
    pendientes = listar_carpetas_con_archivos()
    resultados = []

    if not pendientes:
        print('Bronce: sin archivos pendientes.')
        return resultados

    print(f'Bronce: {len(pendientes)} archivo(s) encontrado(s).')

    for nombre_carpeta, ruta_archivo, tabla_destino in pendientes:
        print(f'  Cargando {ruta_archivo.name} -> {tabla_destino}...', end=' ')

        id_log = registrar_inicio(tabla_destino, ruta_archivo.name)
        resultado = cargar_archivo(
            nombre_carpeta, ruta_archivo, tabla_destino, engine
        )
        registrar_fin(id_log, resultado)

        icono = '[OK]' if resultado['estado'] == 'OK' else '[ERROR]'
        print(f'{icono} {resultado["mensaje"]}')

        resultados.append(resultado)

    return resultados

"""
cargador.py
===========
Lee archivos Excel de campo e inserta en Bronce como NVARCHAR raw.
Sin transformaciones. Sin validaciones. Solo guardar el dato crudo.
"""

import shutil
import numpy as np
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from bronce.rutas import (
    CARPETA_PROCESADOS,
    listar_carpetas_con_archivos,
    obtener_archivo_mas_reciente,
)
from config.conexion import obtener_engine
from auditoria.log import registrar_inicio, registrar_fin


_CACHE_COLUMNAS_BRONCE: dict[str, set[str]] = {}


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

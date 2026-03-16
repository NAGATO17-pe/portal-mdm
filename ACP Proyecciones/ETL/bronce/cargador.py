"""
cargador.py
===========
Lee archivos Excel de campo e inserta en Bronce como NVARCHAR raw.
Sin transformaciones. Sin validaciones. Solo guardar el dato crudo.
"""

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from bronce.rutas import (
    CARPETA_PROCESADOS,
    listar_carpetas_con_archivos,
    obtener_archivo_mas_reciente,
)
from config.conexion import obtener_engine
from auditoria.log import registrar_inicio, registrar_fin


def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas del Excel:
    - Quita espacios al inicio y final
    - Reemplaza espacios internos por underscore
    - Agrega sufijo _Raw si no lo tiene
    """
    columnas_nuevas = {}
    for col in df.columns:
        col_limpia = str(col).strip().replace(' ', '_')
        if not col_limpia.endswith('_Raw'):
            col_limpia = f'{col_limpia}_Raw'
        columnas_nuevas[col] = col_limpia
    return df.rename(columns=columnas_nuevas)


def castear_todo_a_texto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte todas las columnas a string.
    None y NaN se convierten a None (NULL en SQL).
    Bronce nunca tipifica — todo es NVARCHAR.
    """
    for col in df.columns:
        df[col] = df[col].apply(
            lambda valor: None if pd.isna(valor) else str(valor).strip()
        )
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
        method='multi',
        chunksize=500,
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
        df = pd.read_excel(str(ruta_archivo), dtype=str, engine='openpyxl')

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

        # Insertar en Bronce
        filas_insertadas = insertar_en_bronce(df, tabla_destino, engine)

        # Archivar archivo procesado
        archivar_archivo(ruta_archivo, nombre_carpeta)

        resultado['filas']   = filas_insertadas
        resultado['estado']  = 'OK'
        resultado['mensaje'] = f'{filas_insertadas} filas insertadas en {tabla_destino}'

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
        print(f'  Cargando {ruta_archivo.name} → {tabla_destino}...', end=' ')

        id_log = registrar_inicio(tabla_destino, ruta_archivo.name)
        resultado = cargar_archivo(
            nombre_carpeta, ruta_archivo, tabla_destino, engine
        )
        registrar_fin(id_log, resultado)

        icono = '✅' if resultado['estado'] == 'OK' else '❌'
        print(f'{icono} {resultado["mensaje"]}')

        resultados.append(resultado)

    return resultados

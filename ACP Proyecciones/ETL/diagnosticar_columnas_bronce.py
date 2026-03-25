"""
diagnosticar_columnas_bronce.py
===============================
Compara columnas del Excel crudo contra columnas fisicas de tablas Bronce.

Uso:
  py diagnosticar_columnas_bronce.py
  py diagnosticar_columnas_bronce.py --tipo evaluacion_pesos
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from bronce.cargador import (
    _alinear_dataframe_a_tabla,
    _detectar_header_idx,
    _obtener_columnas_bronce,
    normalizar_columnas,
)
from bronce.rutas import RUTAS, listar_carpetas_con_archivos
from config.conexion import obtener_engine, verificar_conexion


def _obtener_pendientes(tipo: str | None) -> list[tuple[str, Path, str]]:
    pendientes = listar_carpetas_con_archivos()
    if tipo:
        pendientes = [fila for fila in pendientes if fila[0] == tipo]
    return pendientes


def _leer_columnas_excel(ruta_archivo: Path, header_idx: int) -> tuple[list[str], list[str]]:
    df_original = pd.read_excel(
        str(ruta_archivo),
        header=header_idx,
        dtype=str,
        nrows=0,
        engine='openpyxl',
    )
    columnas_originales = [str(col) for col in df_original.columns]
    columnas_normalizadas = list(normalizar_columnas(df_original).columns)
    return columnas_originales, columnas_normalizadas


def _formatear_lista(valores: list[str]) -> str:
    if not valores:
        return '  - ninguna'
    return '\n'.join(f'  - {valor}' for valor in valores)


def diagnosticar_columnas(tipo: str | None = None) -> int:
    if not verificar_conexion():
        print('Sin conexion. Diagnostico detenido.')
        return 1

    pendientes = _obtener_pendientes(tipo)
    if not pendientes:
        mensaje = 'No hay archivos pendientes para diagnosticar.'
        if tipo:
            mensaje = f'No hay archivos pendientes para el tipo: {tipo}'
        print(mensaje)
        return 0

    engine = obtener_engine()

    for nombre_carpeta, ruta_archivo, tabla_destino in pendientes:
        print()
        print('=' * 88)
        print(f'DIAGNOSTICO BRONCE: {nombre_carpeta}')
        print('=' * 88)
        print(f'Archivo        : {ruta_archivo.name}')
        print(f'Tabla destino  : {tabla_destino}')

        header_idx = _detectar_header_idx(ruta_archivo, tabla_destino, engine)
        print(f'Header detectado: fila {header_idx}')

        columnas_excel, columnas_normalizadas = _leer_columnas_excel(ruta_archivo, header_idx)
        columnas_tabla = sorted(_obtener_columnas_bronce(tabla_destino, engine))
        columnas_raw_sql = sorted([col for col in columnas_tabla if col.endswith('_Raw')])

        df_muestra = pd.read_excel(
            str(ruta_archivo),
            header=header_idx,
            dtype=str,
            nrows=5,
            engine='openpyxl',
        )
        df_muestra = normalizar_columnas(df_muestra)
        df_alineado, columnas_extra = _alinear_dataframe_a_tabla(df_muestra, tabla_destino, engine)

        columnas_insertables = list(df_alineado.columns)
        columnas_faltantes = sorted([col for col in columnas_raw_sql if col not in columnas_normalizadas])
        columnas_match = sorted([col for col in columnas_normalizadas if col in columnas_raw_sql])

        print()
        print(f'Columnas Excel crudas       : {len(columnas_excel)}')
        print(_formatear_lista(columnas_excel))

        print()
        print(f'Columnas Excel normalizadas : {len(columnas_normalizadas)}')
        print(_formatear_lista(columnas_normalizadas))

        print()
        print(f'Columnas SQL tabla          : {len(columnas_tabla)}')
        print(_formatear_lista(columnas_tabla))

        print()
        print(f'Columnas _Raw en SQL        : {len(columnas_raw_sql)}')
        print(_formatear_lista(columnas_raw_sql))

        print()
        print(f'Columnas con match directo  : {len(columnas_match)}')
        print(_formatear_lista(columnas_match))

        print()
        print(f'Columnas insertables        : {len(columnas_insertables)}')
        print(_formatear_lista(columnas_insertables))

        print()
        print(f'Columnas extra del Excel    : {len(columnas_extra)}')
        print(_formatear_lista(sorted(columnas_extra)))

        print()
        print(f'Columnas _Raw faltantes     : {len(columnas_faltantes)}')
        print(_formatear_lista(columnas_faltantes))

    print()
    return 0


def _construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Diagnostica columnas de Excel vs tablas Bronce.'
    )
    parser.add_argument(
        '--tipo',
        choices=sorted(RUTAS.keys()),
        help='Tipo/carpeta canonica a diagnosticar',
    )
    return parser


if __name__ == '__main__':
    argumentos = _construir_parser().parse_args()
    raise SystemExit(diagnosticar_columnas(argumentos.tipo))

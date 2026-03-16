"""
lookup.py
=========
Lookup de IDs de dimensiones desde Silver y MDM.
Los Facts necesitan los IDs de las dims antes de insertarse.

Patrón:
  - Carga la dim completa en memoria al inicio del pipeline
  - Resuelve IDs en memoria — sin consultas por fila
  - Si no encuentra match → asigna surrogate o envía a cuarentena
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text


# ── Cache de dimensiones en memoria ───────────────────────────
_cache: dict[str, pd.DataFrame] = {}


def _cargar_dim(engine: Engine, tabla: str,
                col_id: str, col_clave: str) -> pd.DataFrame:
    """
    Carga una dimensión en cache si no está ya cargada.
    """
    if tabla not in _cache:
        with engine.connect() as conexion:
            resultado = conexion.execute(
                text(f'SELECT {col_id}, {col_clave} FROM {tabla}')
            )
            _cache[tabla] = pd.DataFrame(
                resultado.fetchall(), columns=[col_id, col_clave]
            )
    return _cache[tabla]


def limpiar_cache() -> None:
    """
    Limpia el cache — llamar al inicio de cada ejecución del pipeline.
    """
    _cache.clear()


# ── Lookups por dimensión ─────────────────────────────────────

def obtener_id_tiempo(fecha_yyyymmdd: int | None,
                       engine: Engine) -> int | None:
    """
    Retorna ID_Tiempo para una fecha en formato YYYYMMDD.
    """
    if fecha_yyyymmdd is None:
        return None

    dim = _cargar_dim(engine, 'Silver.Dim_Tiempo', 'ID_Tiempo', 'ID_Tiempo')
    coincidencia = dim[dim['ID_Tiempo'] == fecha_yyyymmdd]

    return int(coincidencia.iloc[0]['ID_Tiempo']) if not coincidencia.empty else None


def obtener_id_variedad(nombre_canonico: str | None,
                         engine: Engine) -> int | None:
    """
    Retorna ID_Variedad dado el nombre canónico.
    """
    if not nombre_canonico:
        return None

    dim = _cargar_dim(engine, 'Silver.Dim_Variedad', 'ID_Variedad', 'Variedad')
    coincidencia = dim[
        dim['Variedad'].str.lower() == nombre_canonico.lower()
    ]

    return int(coincidencia.iloc[0]['ID_Variedad']) if not coincidencia.empty else None


def obtener_id_personal(dni: str | None,
                          engine: Engine) -> int:
    """
    Retorna ID_Personal dado el DNI.
    Si el DNI es None o no existe → retorna -1 (Sin Evaluador).
    """
    if not dni:
        return -1

    dim = _cargar_dim(engine, 'Silver.Dim_Personal', 'ID_Personal', 'DNI')
    coincidencia = dim[dim['DNI'] == dni]

    return int(coincidencia.iloc[0]['ID_Personal']) if not coincidencia.empty else -1


def obtener_id_geografia(fundo: str | None,
                          sector: str | None,
                          modulo: str | None,
                          engine: Engine) -> int | None:
    """
    Retorna ID_Geografia dado fundo + sector + módulo.
    Solo retorna registros vigentes (Es_Vigente = 1).
    """
    if not fundo:
        return None

    clave = 'Silver.Dim_Geografia'
    if clave not in _cache:
        with engine.connect() as conexion:
            resultado = conexion.execute(text("""
                SELECT ID_Geografia, Fundo, Sector, Modulo
                FROM Silver.Dim_Geografia
                WHERE Es_Vigente = 1
            """))
            _cache[clave] = pd.DataFrame(
                resultado.fetchall(),
                columns=['ID_Geografia', 'Fundo', 'Sector', 'Modulo']
            )

    dim = _cache[clave]
    mascara = dim['Fundo'].str.lower() == fundo.lower()

    if sector:
        mascara &= dim['Sector'].str.lower() == sector.lower()
    if modulo:
        mascara &= dim['Modulo'].str.lower() == modulo.lower()

    coincidencia = dim[mascara]
    return int(coincidencia.iloc[0]['ID_Geografia']) if not coincidencia.empty else None


def obtener_id_estado_fenologico(estado: str | None,
                                   engine: Engine) -> int | None:
    """
    Retorna ID_Estado_Fenologico dado el nombre del estado.
    """
    if not estado:
        return None

    dim = _cargar_dim(
        engine,
        'Silver.Dim_Estado_Fenologico',
        'ID_Estado_Fenologico',
        'Estado'
    )
    coincidencia = dim[
        dim['Estado'].str.lower() == estado.lower()
    ]

    return int(coincidencia.iloc[0]['ID_Estado_Fenologico']) \
        if not coincidencia.empty else None


def obtener_id_actividad(actividad: str | None,
                          engine: Engine) -> int | None:
    """
    Retorna ID_Actividad dado el nombre de la actividad.
    """
    if not actividad:
        return None

    dim = _cargar_dim(
        engine,
        'Silver.Dim_Actividad_Operativa',
        'ID_Actividad',
        'Actividad'
    )
    coincidencia = dim[
        dim['Actividad'].str.lower() == actividad.lower()
    ]

    return int(coincidencia.iloc[0]['ID_Actividad']) \
        if not coincidencia.empty else None


def obtener_id_cinta(color: str | None,
                      engine: Engine) -> int | None:
    """
    Retorna ID_Cinta dado el color.
    """
    if not color:
        return None

    dim = _cargar_dim(engine, 'Silver.Dim_Cinta', 'ID_Cinta', 'Color_Cinta')
    coincidencia = dim[
        dim['Color_Cinta'].str.lower() == color.lower()
    ]

    return int(coincidencia.iloc[0]['ID_Cinta']) \
        if not coincidencia.empty else None

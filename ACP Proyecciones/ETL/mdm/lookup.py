"""
lookup.py
=========
Lookup de IDs de dimensiones desde Silver y MDM.
Cache en memoria — sin consultas por fila.
Surrogate -1 garantizado para Personal sin DNI.
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text


_cache: dict[str, pd.DataFrame] = {}
_cache_mapas: dict[str, dict] = {}


def _cargar_dim(engine: Engine, tabla: str,
                col_id: str, col_clave: str) -> pd.DataFrame:
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
    _cache.clear()
    _cache_mapas.clear()


def _normalizar_texto(valor) -> str | None:
    if valor is None:
        return None
    if isinstance(valor, float) and pd.isna(valor):
        return None

    texto = str(valor).strip()
    if texto in ('', 'None', 'nan'):
        return None
    return texto.lower()


def _obtener_mapa_dim(engine: Engine,
                      tabla: str,
                      col_id: str,
                      col_clave: str) -> dict[str, int]:
    clave_cache = f'mapa::{tabla}::{col_id}::{col_clave}'
    if clave_cache in _cache_mapas:
        return _cache_mapas[clave_cache]

    dim = _cargar_dim(engine, tabla, col_id, col_clave)
    mapa: dict[str, int] = {}
    for _, fila in dim.iterrows():
        clave = _normalizar_texto(fila.get(col_clave))
        if clave is None or clave in mapa:
            continue
        try:
            mapa[clave] = int(fila[col_id])
        except (ValueError, TypeError):
            continue

    _cache_mapas[clave_cache] = mapa
    return mapa


def obtener_id_tiempo(fecha_yyyymmdd: int | None,
                       engine: Engine) -> int | None:
    if fecha_yyyymmdd is None:
        return None
    try:
        clave = str(int(fecha_yyyymmdd))
    except (ValueError, TypeError):
        return None
    mapa = _obtener_mapa_dim(engine, 'Silver.Dim_Tiempo', 'ID_Tiempo', 'ID_Tiempo')
    return mapa.get(clave)


def obtener_id_variedad(nombre_canonico: str | None,
                         engine: Engine) -> int | None:
    clave = _normalizar_texto(nombre_canonico)
    if clave is None:
        return None
    mapa = _obtener_mapa_dim(engine, 'Silver.Dim_Variedad', 'ID_Variedad', 'Nombre_Variedad')
    return mapa.get(clave)


def obtener_id_personal(dni: str | None,
                          engine: Engine) -> int:
    clave = _normalizar_texto(dni)
    if clave is None:
        return -1
    mapa = _obtener_mapa_dim(engine, 'Silver.Dim_Personal', 'ID_Personal', 'DNI')
    return mapa.get(clave, -1)


def obtener_id_geografia(fundo: str | None,
                          sector: str | None,
                          modulo,
                          engine: Engine) -> int | None:
    """
    Busca ID_Geografia por fundo + sector + modulo.
    Solo retorna registros vigentes (Es_Vigente = 1).

    FIX: si fundo es None pero sector no es None, busca solo por sector.
    Esto cubre Fact_Telemetria_Clima donde el Excel solo trae Sector.
    """
    if not fundo and not sector and modulo is None:
        return None

    clave_cache = 'Silver.Dim_Geografia'
    if clave_cache not in _cache:
        with engine.connect() as conexion:
            resultado = conexion.execute(text("""
                SELECT ID_Geografia, Fundo, Sector, Modulo
                FROM Silver.Dim_Geografia
                WHERE Es_Vigente = 1
            """))
            _cache[clave_cache] = pd.DataFrame(
                resultado.fetchall(),
                columns=['ID_Geografia', 'Fundo', 'Sector', 'Modulo']
            )

    dim = _cache[clave_cache]

    if fundo:
        mascara = dim['Fundo'].str.lower() == fundo.lower()
        if sector:
            mascara &= dim['Sector'].str.lower() == sector.lower()
    elif sector:
        # Solo sector — caso telemetría clima
        mascara = dim['Sector'].str.lower() == sector.lower()
    else:
        mascara = pd.Series(True, index=dim.index)

    if modulo is not None:
        mascara &= dim['Modulo'].astype(str).str.lower() == str(modulo).lower()

    coincidencia = dim[mascara]
    return int(coincidencia.iloc[0]['ID_Geografia']) if not coincidencia.empty else None


def obtener_id_estado_fenologico(estado: str | None,
                                   engine: Engine) -> int | None:
    clave = _normalizar_texto(estado)
    if clave is None:
        return None
    mapa = _obtener_mapa_dim(
        engine,
        'Silver.Dim_Estado_Fenologico',
        'ID_Estado_Fenologico',
        'Nombre_Estado'
    )
    return mapa.get(clave)


def obtener_id_actividad(actividad: str | None,
                          engine: Engine) -> int | None:
    clave = _normalizar_texto(actividad)
    if clave is None:
        return None
    mapa = _obtener_mapa_dim(
        engine,
        'Silver.Dim_Actividad_Operativa',
        'ID_Actividad',
        'Nombre_Actividad'
    )
    return mapa.get(clave)


def obtener_id_cinta(color: str | None,
                      engine: Engine) -> int | None:
    clave = _normalizar_texto(color)
    if clave is None:
        return None
    mapa = _obtener_mapa_dim(engine, 'Silver.Dim_Cinta', 'ID_Cinta', 'Color_Cinta')
    return mapa.get(clave)


def _geo_token(valor) -> str | None:
    if valor is None:
        return None
    if isinstance(valor, float) and pd.isna(valor):
        return None

    texto = str(valor).strip()
    if texto in ('', 'None', 'nan'):
        return None

    try:
        return str(int(float(texto)))
    except (ValueError, TypeError):
        return texto.lower()


def obtener_id_geografia(fundo: str | None,
                          sector: str | None,
                          modulo,
                          engine: Engine,
                          turno=None,
                          valvula=None,
                          cama=None) -> int | None:
    """
    Busca ID_Geografia con la mayor precision posible.
    Compatibilidad:
    - llamadas antiguas siguen funcionando con fundo/sector/modulo
    - si llegan turno/valvula/cama, se usan para desambiguar
    """
    if not fundo and not sector and modulo is None and turno is None and valvula is None and cama is None:
        return None

    fundo_token = _geo_token(fundo)
    sector_token = _geo_token(sector)
    modulo_token = _geo_token(modulo)
    turno_token = _geo_token(turno)
    valvula_token = _geo_token(valvula)
    cama_token = _geo_token(cama)

    clave_cache = 'Silver.Dim_Geografia'
    if clave_cache not in _cache:
        with engine.connect() as conexion:
            resultado = conexion.execute(text("""
                SELECT
                    ID_Geografia,
                    Fundo,
                    Sector,
                    Modulo,
                    Turno,
                    Valvula,
                    Cama
                FROM Silver.Dim_Geografia
                WHERE Es_Vigente = 1
            """))
            dim = pd.DataFrame(
                resultado.fetchall(),
                columns=['ID_Geografia', 'Fundo', 'Sector', 'Modulo', 'Turno', 'Valvula', 'Cama']
            )
        dim['Fundo_token'] = dim['Fundo'].map(_geo_token)
        dim['Sector_token'] = dim['Sector'].map(_geo_token)
        dim['Modulo_token'] = dim['Modulo'].map(_geo_token)
        dim['Turno_token'] = dim['Turno'].map(_geo_token)
        dim['Valvula_token'] = dim['Valvula'].map(_geo_token)
        dim['Cama_token'] = dim['Cama'].map(_geo_token)
        _cache[clave_cache] = dim

    clave_busqueda = (
        fundo_token, sector_token, modulo_token,
        turno_token, valvula_token, cama_token,
    )
    mapa_geo = _cache_mapas.setdefault('geo_lookup', {})
    if clave_busqueda in mapa_geo:
        return mapa_geo[clave_busqueda]

    dim = _cache[clave_cache]
    mascara = pd.Series(True, index=dim.index)

    if fundo_token is not None:
        mascara &= dim['Fundo_token'] == fundo_token

    if sector_token is not None:
        mascara &= dim['Sector_token'] == sector_token

    if modulo_token is not None:
        mascara &= dim['Modulo_token'] == modulo_token

    if turno_token is not None:
        mascara &= dim['Turno_token'] == turno_token

    if valvula_token is not None:
        mascara &= dim['Valvula_token'] == valvula_token

    if cama_token is not None:
        mascara &= dim['Cama_token'] == cama_token

    coincidencia = dim[mascara]
    valor = int(coincidencia.iloc[0]['ID_Geografia']) if not coincidencia.empty else None
    mapa_geo[clave_busqueda] = valor
    return valor

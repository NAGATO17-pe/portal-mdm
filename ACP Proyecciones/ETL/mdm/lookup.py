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
import re


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

    clave_cache = 'mapa::Silver.Dim_Tiempo::ID_Tiempo'
    if clave_cache not in _cache_mapas:
        with engine.connect() as conexion:
            resultado = conexion.execute(text("""
                SELECT ID_Tiempo
                FROM Silver.Dim_Tiempo
            """))
            mapa = {}
            for fila in resultado.fetchall():
                try:
                    id_tiempo = int(fila[0])
                    mapa[str(id_tiempo)] = id_tiempo
                except (ValueError, TypeError):
                    continue
            _cache_mapas[clave_cache] = mapa

    mapa = _cache_mapas[clave_cache]
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

    if re.fullmatch(r'[+-]?\d+', texto):
        return str(int(texto))

    if re.fullmatch(r'[+-]?\d+\.0+', texto):
        return str(int(texto.split('.', 1)[0]))

    return texto.lower()

def _cargar_dim_geografia(engine: Engine) -> pd.DataFrame:
    clave_cache = 'Silver.Dim_Geografia'
    if clave_cache in _cache:
        return _cache[clave_cache]

    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT
                ID_Geografia,
                Fundo,
                Sector,
                Modulo,
                Turno,
                Valvula,
                Cama,
                Es_Test_Block
            FROM Silver.Dim_Geografia
            WHERE Es_Vigente = 1
        """))
        dim = pd.DataFrame(
            resultado.fetchall(),
            columns=[
                'ID_Geografia', 'Fundo', 'Sector', 'Modulo',
                'Turno', 'Valvula', 'Cama', 'Es_Test_Block'
            ]
        )

    dim['Fundo_token'] = dim['Fundo'].map(_geo_token)
    dim['Sector_token'] = dim['Sector'].map(_geo_token)
    dim['Modulo_token'] = dim['Modulo'].map(_geo_token)
    dim['Turno_token'] = dim['Turno'].map(_geo_token)
    dim['Valvula_token'] = dim['Valvula'].map(_geo_token)
    dim['Cama_token'] = dim['Cama'].map(_geo_token)
    _cache[clave_cache] = dim
    return dim


def _es_modulo_especial(modulo_token: str | None) -> bool:
    if modulo_token is None:
        return False
    return re.fullmatch(r'[+-]?\d+', modulo_token) is None


def _resolver_id_unico(coincidencias: pd.DataFrame) -> int | None:
    if len(coincidencias) != 1:
        return None
    return int(coincidencias.iloc[0]['ID_Geografia'])


def _resolver_geografia_sp(fundo: str | None,
                           sector: str | None,
                           modulo,
                           engine: Engine,
                           turno=None,
                           valvula=None,
                           cama=None) -> dict | None:
    """
    Resolver oficial por Stored Procedure.
    Nota: fundo/sector se ignoran en el SP actual porque el grano operativo
    para resolver es Modulo+Turno+Valvula (+Cama opcional).
    """
    clave_busqueda = (
        _geo_token(fundo),
        _geo_token(sector),
        _geo_token(modulo),
        _geo_token(turno),
        _geo_token(valvula),
        _geo_token(cama),
        'SP',
    )
    mapa_geo = _cache_mapas.setdefault('geo_resolucion_sp', {})
    if clave_busqueda in mapa_geo:
        return mapa_geo[clave_busqueda]

    try:
        with engine.connect() as conexion:
            fila = conexion.execute(text("""
                EXEC Silver.sp_Resolver_Geografia_Cama
                    @Modulo_Raw = :modulo_raw,
                    @Turno_Raw = :turno_raw,
                    @Valvula_Raw = :valvula_raw,
                    @Cama_Raw = :cama_raw
            """), {
                'modulo_raw': None if modulo is None else str(modulo),
                'turno_raw': None if turno is None else str(turno),
                'valvula_raw': None if valvula is None else str(valvula),
                'cama_raw': None if cama is None else str(cama),
            }).fetchone()
    except Exception:
        return None

    if not fila:
        return None

    resultado = {
        'id_geografia': fila.ID_Geografia,
        'id_cama_catalogo': fila.ID_Cama_Catalogo,
        'estado': fila.Estado_Resolucion,
        'detalle': fila.Detalle,
    }
    mapa_geo[clave_busqueda] = resultado
    return resultado


def _resolver_geografia_legacy(fundo: str | None,
                               sector: str | None,
                               modulo,
                               engine: Engine,
                               turno=None,
                               valvula=None,
                               cama=None) -> dict:
    if not fundo and not sector and modulo is None and turno is None and valvula is None and cama is None:
        return {
            'id_geografia': None,
            'estado': 'SIN_DATOS_GEOGRAFIA',
            'detalle': 'No se recibieron componentes geograficos para resolver ID_Geografia.',
        }

    fundo_token = _geo_token(fundo)
    sector_token = _geo_token(sector)
    modulo_token = _geo_token(modulo)
    turno_token = _geo_token(turno)
    valvula_token = _geo_token(valvula)
    cama_token = _geo_token(cama)

    clave_busqueda = (
        fundo_token, sector_token, modulo_token,
        turno_token, valvula_token, cama_token,
    )
    mapa_geo = _cache_mapas.setdefault('geo_resolucion', {})
    if clave_busqueda in mapa_geo:
        return mapa_geo[clave_busqueda]

    if _es_modulo_especial(modulo_token):
        resultado = {
            'id_geografia': None,
            'estado': 'PENDIENTE_CASO_ESPECIAL',
            'detalle': 'Modulo especial requiere catalogacion y regla MDM antes de resolver ID_Geografia.',
        }
        mapa_geo[clave_busqueda] = resultado
        return resultado

    dim = _cargar_dim_geografia(engine)
    mascara_base = pd.Series(True, index=dim.index)

    if fundo_token is not None:
        mascara_base &= dim['Fundo_token'] == fundo_token

    if sector_token is not None:
        mascara_base &= dim['Sector_token'] == sector_token

    if modulo_token is not None:
        mascara_base &= dim['Modulo_token'] == modulo_token

    if turno_token is not None:
        mascara_base &= dim['Turno_token'] == turno_token

    if valvula_token is not None:
        mascara_base &= dim['Valvula_token'] == valvula_token

    requiere_revision_cama = (
        cama_token == '0'
        or (cama_token is None and (turno_token is not None or valvula_token is not None))
    )

    if cama_token not in (None, '0'):
        coincidencias = dim[mascara_base & (dim['Cama_token'] == cama_token)]
        id_geografia = _resolver_id_unico(coincidencias)
        if id_geografia is not None:
            resultado = {
                'id_geografia': id_geografia,
                'estado': 'RESUELTA_DIM_GEOGRAFIA',
                'detalle': 'Coincidencia exacta en Silver.Dim_Geografia.',
            }
        elif len(coincidencias) > 1:
            resultado = {
                'id_geografia': None,
                'estado': 'PENDIENTE_DIM_DUPLICADA',
                'detalle': 'La combinacion geografica tiene mas de un registro vigente en Silver.Dim_Geografia.',
            }
        else:
            resultado = {
                'id_geografia': None,
                'estado': 'PENDIENTE_GEOGRAFIA_NO_EXISTE',
                'detalle': 'La combinacion geografica no existe en Silver.Dim_Geografia.',
            }
        mapa_geo[clave_busqueda] = resultado
        return resultado

    coincidencias_genericas = dim[
        mascara_base & dim['Cama_token'].isin([None, '0'])
    ]
    id_generico = _resolver_id_unico(coincidencias_genericas)
    if id_generico is not None:
        resultado = {
            'id_geografia': id_generico,
            'estado': 'RESUELTA_DIM_GEOGRAFIA',
            'detalle': 'Coincidencia exacta a nivel generico en Silver.Dim_Geografia.',
        }
    elif len(coincidencias_genericas) > 1:
        resultado = {
            'id_geografia': None,
            'estado': 'PENDIENTE_DIM_DUPLICADA',
            'detalle': 'La clave generica de geografia tiene mas de un registro vigente en Silver.Dim_Geografia.',
        }
    elif requiere_revision_cama:
        resultado = {
            'id_geografia': None,
            'estado': 'PENDIENTE_CAMA_GENERICA',
            'detalle': 'La geografia a nivel valvula o cama generica no existe en Silver.Dim_Geografia.',
        }
    else:
        resultado = {
            'id_geografia': None,
            'estado': 'PENDIENTE_GEOGRAFIA_NO_EXISTE',
            'detalle': 'La combinacion geografica no existe en Silver.Dim_Geografia.',
        }

    mapa_geo[clave_busqueda] = resultado
    return resultado


def resolver_geografia(fundo: str | None,
                       sector: str | None,
                       modulo,
                       engine: Engine,
                       turno=None,
                       valvula=None,
                       cama=None) -> dict:
    """
    Resolver principal de geografia.
    1) Intenta resolver por SP (fuente oficial de reglas).
    2) Si el SP no existe o falla, usa fallback legacy en Python.
    """
    turno_token = _geo_token(turno)
    valvula_token = _geo_token(valvula)
    cama_token = _geo_token(cama)
    granularidad_operativa = any(token is not None for token in (turno_token, valvula_token, cama_token))

    resultado_sp = _resolver_geografia_sp(
        fundo,
        sector,
        modulo,
        engine,
        turno=turno,
        valvula=valvula,
        cama=cama,
    )
    if resultado_sp is not None:
        # Si el SP resolvio ID, siempre se respeta como fuente oficial.
        if resultado_sp.get('id_geografia') is not None:
            return resultado_sp
        # Si hay granularidad operativa (turno/valvula/cama), no se hace fallback
        # para no saltarse la validacion operativa del resolvedor oficial.
        if granularidad_operativa:
            return resultado_sp
        # Para facts que solo tienen modulo (sin turno/valvula/cama), el SP puede
        # devolver no resuelto por diseno; en ese caso aplicamos fallback legacy.
        return _resolver_geografia_legacy(
            fundo,
            sector,
            modulo,
            engine,
            turno=turno,
            valvula=valvula,
            cama=cama,
        )

    return _resolver_geografia_legacy(
        fundo,
        sector,
        modulo,
        engine,
        turno=turno,
        valvula=valvula,
        cama=cama,
    )


def obtener_id_geografia(fundo: str | None,
                         sector: str | None,
                         modulo,
                         engine: Engine,
                         turno=None,
                         valvula=None,
                         cama=None) -> int | None:
    resultado = resolver_geografia(
        fundo,
        sector,
        modulo,
        engine,
        turno=turno,
        valvula=valvula,
        cama=cama,
    )
    return resultado.get('id_geografia')

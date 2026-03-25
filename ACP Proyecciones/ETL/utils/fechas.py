"""
fechas.py
=========
Utilidades para parseo y normalización de fechas.
Los Excel de campo llegan con formatos inconsistentes.
"""

from datetime import datetime, date
import re
from typing import Optional


FORMATOS_ACEPTADOS = [
    '%d/%m/%Y',
    '%d/%m/%y',
    '%Y-%m-%d',
    '%d-%m-%Y',
    '%d.%m.%Y',
    '%Y%m%d',
    '%d/%m/%Y %H:%M:%S',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S',
]


def parsear_serie_fechas(serie: 'pd.Series') -> 'pd.Series':
    """
    Parsea una Serie COMPLETA de fechas en un solo paso vectorizado.
    Mucho más rápido que aplicar parsear_fecha fila a fila.
    Usa inferencia automática de formato; los valores no parseables quedan NaT.
    """
    import pandas as pd
    # Primer intento rápido con inferencia de formato
    serie_texto = serie.astype('string')
    mascara_iso = serie_texto.str.fullmatch(r'\d{4}-\d{2}-\d{2}', na=False)
    mascara_iso_dt = serie_texto.str.fullmatch(r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}', na=False)

    resultado = pd.Series(pd.NaT, index=serie.index, dtype='datetime64[ns]')
    if mascara_iso.any():
        resultado.loc[mascara_iso] = pd.to_datetime(
            serie_texto[mascara_iso],
            errors='coerce',
            format='%Y-%m-%d'
        )
    if mascara_iso_dt.any():
        serie_iso_dt = serie_texto[mascara_iso_dt].str.replace('T', ' ', regex=False)
        resultado.loc[mascara_iso_dt] = pd.to_datetime(
            serie_iso_dt,
            errors='coerce',
            format='%Y-%m-%d %H:%M:%S'
        )

    mascara_restante = resultado.isna() & serie.notna()
    if mascara_restante.any():
        resultado.loc[mascara_restante] = pd.to_datetime(
            serie[mascara_restante],
            errors='coerce',
            dayfirst=True
        )
    # Rellenar los NaT con intento de número de serie Excel
    mascara_nat = resultado.isna() & serie.notna()
    if mascara_nat.any():
        import pandas as _pd
        from datetime import datetime as _dt, timedelta
        BASE = _dt(1899, 12, 30)
        def _desde_serial(v):
            try:
                serial = int(float(str(v).strip()))
                if 30000 < serial < 60000:
                    return BASE + timedelta(days=serial)
            except (ValueError, TypeError):
                pass
            return None
        resultado_serial = _pd.to_datetime(
            serie[mascara_nat].map(_desde_serial),
            errors='coerce'
        )
        resultado = resultado.where(~mascara_nat, other=resultado_serial)
    return resultado


def parsear_fecha(valor: str | None) -> Optional[datetime]:
    """
    Intenta parsear un string de fecha. Intento rápido primero,
    luego recorre todos los formatos aceptados si falla.
    Retorna datetime si parsea correctamente, None si falla.
    """
    if valor is None:
        return None

    valor = str(valor).strip()

    if not valor:
        return None

    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', valor):
        try:
            return datetime.strptime(valor, '%Y-%m-%d')
        except ValueError:
            pass

    if re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', valor):
        try:
            return datetime.strptime(valor, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass

    if re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', valor):
        try:
            return datetime.strptime(valor, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            pass

    # Intento rápido con inferencia automática
    try:
        import pandas as _pd
        resultado = _pd.to_datetime(valor, dayfirst=True)
        if resultado is not _pd.NaT:
            return resultado.to_pydatetime()
    except Exception:
        pass

    # Excel a veces entrega float (número de serie de fecha)
    try:
        numero = float(valor)
        # Número de serie de Excel → fecha
        return datetime.fromordinal(
            datetime(1899, 12, 30).toordinal() + int(numero)
        )
    except ValueError:
        pass

    for formato in FORMATOS_ACEPTADOS:
        try:
            return datetime.strptime(valor, formato)
        except ValueError:
            continue

    return None


def obtener_semana_iso(fecha: datetime | date | None) -> Optional[int]:
    """
    Retorna el número de semana ISO de una fecha.
    """
    if fecha is None:
        return None
    return fecha.isocalendar()[1]


def obtener_id_tiempo(fecha: datetime | date | None) -> Optional[int]:
    """
    Convierte una fecha al formato ID_Tiempo = YYYYMMDD.
    Referencia directa a Silver.Dim_Tiempo.
    """
    if fecha is None:
        return None
    return int(fecha.strftime('%Y%m%d'))


def es_fecha_valida_campana(fecha: datetime | date | None,
                             inicio: str = '2025-03-01',
                             fin:    str = '2026-06-30') -> bool:
    """
    Verifica que la fecha esté dentro del rango de la campaña activa.
    """
    if fecha is None:
        return False

    fecha_inicio = datetime.strptime(inicio, '%Y-%m-%d').date()
    fecha_fin    = datetime.strptime(fin,    '%Y-%m-%d').date()

    fecha_date = fecha.date() if isinstance(fecha, datetime) else fecha

    return fecha_inicio <= fecha_date <= fecha_fin


def procesar_fecha(valor: str | None) -> tuple[Optional[datetime], bool]:
    """
    Parsea y valida la fecha en un solo paso.
    Retorna tupla (fecha_parseada, es_valida_para_campana).
    """
    fecha  = parsear_fecha(valor)
    valida = es_fecha_valida_campana(fecha)
    return fecha, valida

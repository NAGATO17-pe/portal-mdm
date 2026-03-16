"""
fechas.py
=========
Utilidades para parseo y normalización de fechas.
Los Excel de campo llegan con formatos inconsistentes.
"""

from datetime import datetime, date
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


def parsear_fecha(valor: str | None) -> Optional[datetime]:
    """
    Intenta parsear un string de fecha probando todos los formatos aceptados.
    Retorna datetime si parsea correctamente, None si falla.
    """
    if valor is None:
        return None

    valor = str(valor).strip()

    if not valor:
        return None

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
                             inicio: str = '2025-06-01',
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

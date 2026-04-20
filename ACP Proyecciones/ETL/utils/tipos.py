"""Conversores de tipos robustos — fuente única de verdad para el ETL."""
from __future__ import annotations


def a_entero(valor) -> int | None:
    try:
        if valor is None:
            return None
        t = str(valor).strip()
        if t in ("", "None", "nan", "NaN"):
            return None
        return int(float(t.replace(",", ".")))
    except (ValueError, TypeError):
        return None


def a_entero_no_negativo(valor) -> int | None:
    n = a_entero(valor)
    return n if (n is not None and n >= 0) else None


def a_decimal(valor) -> float | None:
    try:
        if valor is None:
            return None
        t = str(valor).strip()
        if t in ("", "None", "nan", "NaN"):
            return None
        return float(t.replace(",", "."))
    except (ValueError, TypeError):
        return None


def texto_nulo(valor) -> str | None:
    if valor is None:
        return None
    t = str(valor).strip()
    return t if t and t.lower() not in ("none", "nan") else None


def obtener_valor_raw(
    fila,
    nombre_columna: str,
    valores_raw: dict | None = None,
):
    """Busca valor en columna directa o, si falta, en el dict valores_raw."""
    valor = fila.get(nombre_columna)
    if valor is not None and str(valor).strip() not in ("", "None", "nan"):
        return valor
    if valores_raw is None:
        return None
    v = valores_raw.get(nombre_columna)
    return v if v is not None and str(v).strip() not in ("", "None", "nan") else None

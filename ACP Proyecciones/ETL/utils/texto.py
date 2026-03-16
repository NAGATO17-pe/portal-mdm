"""
texto.py
========
Utilidades para normalización de texto libre.
Variedades, nombres de personal y geografía llegan
con mayúsculas inconsistentes, tildes y espacios extra.
"""

import re
import unicodedata


def normalizar_espacio(valor: str | None) -> str | None:
    """
    Elimina espacios al inicio, final y dobles internos.
    """
    if valor is None:
        return None
    return re.sub(r'\s+', ' ', str(valor).strip())


def titulo(valor: str | None) -> str | None:
    """
    Aplica Title Case — primera letra de cada palabra en mayúscula.
    Usa .title() de Python que maneja tildes correctamente.
    Ejemplo: 'BILOXI' → 'Biloxi', 'sekoya pop' → 'Sekoya Pop'
    """
    if valor is None:
        return None
    return normalizar_espacio(valor).title()


def mayusculas(valor: str | None) -> str | None:
    """
    Convierte a mayúsculas — para códigos y siglas.
    """
    if valor is None:
        return None
    return normalizar_espacio(valor).upper()


def quitar_tildes(valor: str | None) -> str | None:
    """
    Elimina tildes para comparaciones insensibles a acentos.
    Útil para lookup en MDM antes de Levenshtein.
    """
    if valor is None:
        return None
    return ''.join(
        caracter for caracter in unicodedata.normalize('NFD', valor)
        if unicodedata.category(caracter) != 'Mn'
    )


def normalizar_variedad(valor: str | None) -> str | None:
    """
    Normalización estándar para nombres de variedad:
    - Quita espacios extra
    - Aplica Title Case
    Ejemplo: '  BILOXY  ' → 'Biloxy'
    """
    if valor is None:
        return None
    return titulo(valor)


def normalizar_nombre_persona(valor: str | None) -> str | None:
    """
    Normalización para nombres de personal:
    - Quita espacios extra
    - Aplica Title Case
    Ejemplo: 'JUAN CARLOS LOPEZ' → 'Juan Carlos Lopez'
    """
    if valor is None:
        return None
    return titulo(valor)


def normalizar_modulo(valor: str | None) -> str | None:
    """
    Normalización para módulos y geografía.
    Detecta Test Block y retorna None para manejo especial.
    """
    if valor is None:
        return None

    valor_limpio = normalizar_espacio(valor).upper()

    if 'TEST' in valor_limpio or 'BLOCK' in valor_limpio:
        return None  # Señal para Es_Test_Block = 1

    return valor_limpio


def es_test_block(valor: str | None) -> bool:
    """
    Detecta si el valor de módulo corresponde a un Test Block.
    """
    if valor is None:
        return False
    return 'TEST' in str(valor).upper() or 'BLOCK' in str(valor).upper()


def limpiar_numerico_texto(valor: str | None) -> str | None:
    """
    Limpia valores numéricos que Excel convirtió a texto.
    Ejemplo: '25.0' → '25', '  12.00  ' → '12'
    """
    if valor is None:
        return None
    valor = normalizar_espacio(str(valor))
    try:
        return str(int(float(valor)))
    except (ValueError, TypeError):
        return valor

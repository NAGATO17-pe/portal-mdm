import pandas as pd

from silver.facts.fact_fisiologia import _normalizar_tercio, _obtener_valor_raw


def test_normalizar_tercio_acepta_alias_operativos():
    assert _normalizar_tercio('low') == 'BAJO'
    assert _normalizar_tercio('M') == 'MEDIO'
    assert _normalizar_tercio('ALTO') == 'ALTO'


def test_obtener_valor_raw_usa_serializado_solo_si_falta_columna_directa():
    fila = pd.Series({
        'Modulo_Raw': None,
        'Valores_Raw': 'Modulo_Raw=03 | Turno_Raw=06',
    })

    assert _obtener_valor_raw(fila, 'Modulo_Raw') == '03'


def test_obtener_valor_raw_prioriza_valor_directo():
    fila = pd.Series({
        'Modulo_Raw': '11',
        'Valores_Raw': 'Modulo_Raw=03 | Turno_Raw=06',
    })

    assert _obtener_valor_raw(fila, 'Modulo_Raw') == '11'

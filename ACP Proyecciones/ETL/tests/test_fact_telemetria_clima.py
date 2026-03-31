from silver.facts.fact_telemetria_clima import (
    _construir_fecha_hora_clima,
    _extraer_hora_desde_valores_raw,
)


def test_extraer_hora_desde_valores_raw_recupera_hora_serializada():
    valores_raw = 'Ano_Raw=2026 | Semana_Raw=13 | Hora_Raw=08:30:00 | Otra_Metrica=1.25'

    hora = _extraer_hora_desde_valores_raw(valores_raw)

    assert hora == '08:30:00'


def test_construir_fecha_hora_clima_usa_hora_desde_valores_raw():
    valores_raw = 'Ano_Raw=2026 | Semana_Raw=13 | Hora_Raw=08:30 | Otra_Metrica=1.25'

    fecha_hora = _construir_fecha_hora_clima('2026-03-29', valores_raw=valores_raw)

    assert fecha_hora == '2026-03-29 08:30:00'


def test_construir_fecha_hora_clima_sin_hora_retorna_solo_fecha():
    fecha_hora = _construir_fecha_hora_clima('2026-03-29', valores_raw='Semana_Raw=13')

    assert fecha_hora == '2026-03-29'

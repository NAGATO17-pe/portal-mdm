from silver.facts.fact_telemetria_clima import (
    _construir_fecha_hora_clima,
    _extraer_hora_desde_valores_raw,
    _resolver_duplicados_clima,
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


def test_resolver_duplicados_clima_colapsa_duplicado_exacto():
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}
    ids_rechazados = []
    registros = [
        {
            'id_origen': 1,
            'sector_climatico': 'F07',
            'fecha_evento': '2025-11-25 14:30:00',
            'temp_max': 28.8,
            'temp_min': 28.4,
            'humedad': 58.0,
            'vpd': 1.64,
            'radiacion': 583.0,
        },
        {
            'id_origen': 2,
            'sector_climatico': 'F07',
            'fecha_evento': '2025-11-25 14:30:00',
            'temp_max': 28.8,
            'temp_min': 28.4,
            'humedad': 58.0,
            'vpd': 1.64,
            'radiacion': 583.0,
        },
    ]

    registros_validos, ids_insertados = _resolver_duplicados_clima(
        registros,
        campos_metricas=('temp_max', 'temp_min', 'humedad', 'vpd', 'radiacion'),
        resumen=resumen,
        ids_rechazados=ids_rechazados,
        descripcion_origen='Bronce.Variables_Meteorologicas',
    )

    assert len(registros_validos) == 1
    assert ids_insertados == [1]
    assert ids_rechazados == []
    assert resumen['rechazados'] == 0
    assert resumen['cuarentena'] == []


def test_resolver_duplicados_clima_envia_conflictivo_a_cuarentena():
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}
    ids_rechazados = []
    registros = [
        {
            'id_origen': 10,
            'sector_climatico': 'F07',
            'fecha_evento': '2025-11-25 14:30:00',
            'temp_max': 28.8,
            'temp_min': 28.4,
            'humedad': 58.0,
            'vpd': 1.64,
            'radiacion': 583.0,
        },
        {
            'id_origen': 11,
            'sector_climatico': 'F07',
            'fecha_evento': '2025-11-25 14:30:00',
            'temp_max': 28.1,
            'temp_min': 27.9,
            'humedad': 59.0,
            'vpd': 1.55,
            'radiacion': 585.0,
        },
    ]

    registros_validos, ids_insertados = _resolver_duplicados_clima(
        registros,
        campos_metricas=('temp_max', 'temp_min', 'humedad', 'vpd', 'radiacion'),
        resumen=resumen,
        ids_rechazados=ids_rechazados,
        descripcion_origen='Bronce.Variables_Meteorologicas',
    )

    assert registros_validos == []
    assert ids_insertados == []
    assert ids_rechazados == [10, 11]
    assert resumen['rechazados'] == 2
    assert len(resumen['cuarentena']) == 2

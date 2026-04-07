from silver.facts.fact_tasa_crecimiento_brotes import _registrar_rechazo, _texto_nulo


def test_texto_nulo_descarta_nan_y_none_textual():
    assert _texto_nulo('nan') is None
    assert _texto_nulo('None') is None
    assert _texto_nulo(' ENSAYO-01 ') == 'ENSAYO-01'


def test_registrar_rechazo_actualiza_resumen_e_ids():
    resumen = {'rechazados': 0, 'cuarentena': []}
    ids_rechazados = set()

    _registrar_rechazo(
        resumen,
        ids_rechazados,
        55,
        columna='Fecha_Raw',
        valor='fecha_invalida',
        motivo='Fecha invalida en tasa de crecimiento',
        tipo_regla='DQ',
    )

    assert resumen['rechazados'] == 1
    assert ids_rechazados == {55}
    assert resumen['cuarentena'][0]['motivo'] == 'Fecha invalida en tasa de crecimiento'

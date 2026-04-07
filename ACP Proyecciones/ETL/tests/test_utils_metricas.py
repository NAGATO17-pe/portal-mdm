from utils.metricas import (
    construir_reporte_dq_operativo,
    formatear_resumen_fact,
    normalizar_resultado_fact,
)


def test_normalizar_resultado_fact_calcula_metricas_operativas():
    resultado = normalizar_resultado_fact({
        'leidos': 10,
        'insertados': 7,
        'rechazados': 3,
        'cuarentena': [
            {'motivo': 'Fecha invalida'},
            {'motivo': 'Fecha invalida'},
            {'motivo': 'Variedad sin match'},
        ],
    })

    assert resultado['leidos'] == 10
    assert resultado['cuarentena_total'] == 3
    assert resultado['tasa_rechazo_pct'] == 30.0
    assert resultado['motivos_principales'][0] == {
        'motivo': 'Fecha invalida',
        'cantidad': 2,
    }


def test_normalizar_resultado_fact_infiere_leidos_si_no_vienen():
    resultado = normalizar_resultado_fact({
        'insertados': 4,
        'rechazados': 1,
        'cuarentena': [],
    })

    assert resultado['leidos'] == 5
    assert resultado['tasa_rechazo_pct'] == 20.0


def test_formatear_resumen_fact_incluye_motivos():
    lineas = formatear_resumen_fact({
        'leidos': 8,
        'insertados': 6,
        'rechazados': 2,
        'cuarentena_total': 2,
        'tasa_rechazo_pct': 25.0,
        'motivos_principales': [
            {'motivo': 'Geografia no encontrada', 'cantidad': 2},
        ],
    })

    assert '8 leidos' in lineas[0]
    assert '25.0% rechazo' in lineas[0]
    assert lineas[1] == '          motivo: Geografia no encontrada (2)'


def test_construir_reporte_dq_operativo_retorna_payload_estable():
    reporte = construir_reporte_dq_operativo('Fact_Telemetria_Clima', {
        'insertados': 10,
        'rechazados': 2,
        'cuarentena': [
            {'motivo': 'Humedad no numerica'},
            {'motivo': 'Humedad no numerica'},
        ],
    })

    assert reporte['fact'] == 'Fact_Telemetria_Clima'
    assert reporte['cuarentena_total'] == 2
    assert reporte['motivos_principales'][0]['motivo'] == 'Humedad no numerica'

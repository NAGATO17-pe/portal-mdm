from datetime import datetime

from dq.validador import validar_fecha, validar_peso_baya
from utils.fechas import (
    FECHA_CAMPANA_FIN,
    FECHA_CAMPANA_INICIO,
    describir_rango_campana,
    obtener_politica_fecha,
    procesar_fecha,
    resolver_dominio_fecha,
)


def test_procesar_fecha_fuera_de_campana_se_marca_invalida():
    fecha, valida = procesar_fecha('2022-01-15')

    assert fecha == datetime(2022, 1, 15)
    assert valida is False


def test_procesar_fecha_sin_validacion_de_campana_permite_historico():
    fecha, valida = procesar_fecha('2022-01-15', validar_campana=False)

    assert fecha == datetime(2022, 1, 15)
    assert valida is True


def test_procesar_fecha_invalida_sigue_fallando_sin_validacion_de_campana():
    fecha, valida = procesar_fecha('fecha_inexistente', validar_campana=False)

    assert fecha is None
    assert valida is False


def test_procesar_fecha_clima_permite_historico_por_dominio():
    fecha, valida = procesar_fecha('2022-01-15', dominio='clima')

    assert fecha == datetime(2022, 1, 15)
    assert valida is True


def test_procesar_fecha_iso_con_hora_sin_cero_preserva_dia_y_mes():
    fecha, valida = procesar_fecha('2022-11-09 1:00:00', dominio='clima')

    assert fecha == datetime(2022, 11, 9, 1, 0, 0)
    assert valida is True


def test_procesar_fecha_corta_interpreta_anio_primero_cuando_corresponde():
    fecha, valida = procesar_fecha('22/11/09 1:00:00', dominio='clima')

    assert fecha == datetime(2022, 11, 9, 1, 0, 0)
    assert valida is True


def test_procesar_fecha_corta_mantiene_dia_primero_cuando_el_anio_va_al_final():
    fecha, valida = procesar_fecha('09/11/22 1:00:00', dominio='clima')

    assert fecha == datetime(2022, 11, 9, 1, 0, 0)
    assert valida is True


def test_procesar_fecha_fact_operativa_aplica_politica_de_campana():
    fecha, valida = procesar_fecha('2022-01-15', dominio='evaluacion_pesos')

    assert fecha == datetime(2022, 1, 15)
    assert valida is False


def test_obtener_politica_fecha_por_dominio():
    politica = obtener_politica_fecha('clima')

    assert politica['validar_campana'] is False
    assert politica['inicio'] is None
    assert politica['fin'] is None


def test_describir_rango_campana_usa_politica_del_dominio():
    assert describir_rango_campana(dominio='evaluacion_pesos') == '2025-03-01 -> 2026-06-30'


def test_describir_rango_campana_usa_parametros_configurados(monkeypatch):
    monkeypatch.setattr(
        'utils.fechas.obtener',
        lambda parametro, default='': {
            'CAMPANA_FECHA_INICIO': '2026-01-15',
            'CAMPANA_FECHA_FIN': '2026-12-31',
        }.get(parametro, default),
    )

    assert describir_rango_campana(dominio='evaluacion_pesos') == '2026-01-15 -> 2026-12-31'


def test_resolver_dominio_fecha_reutiliza_mapeo_central():
    assert resolver_dominio_fecha('conteo_fruta') == 'conteo_fenologico'


def test_validar_peso_baya_respeta_minimo_biologico():
    peso, error = validar_peso_baya('0.4')

    assert peso is None
    assert error is not None


def test_validar_peso_baya_acepta_minimo_exacto():
    peso, error = validar_peso_baya('0.5')

    assert peso == 0.5
    assert error is None


def test_validar_fecha_reporta_rango_desde_fuente_unica():
    _, error = validar_fecha('2022-01-15', dominio='evaluacion_pesos')

    assert error is not None
    assert error['motivo'] == 'Fecha fuera del rango de campana (2025-03-01 -> 2026-06-30)'


def test_validar_fecha_reporta_rango_desde_config(monkeypatch):
    monkeypatch.setattr(
        'utils.fechas.obtener',
        lambda parametro, default='': {
            'CAMPANA_FECHA_INICIO': '2026-01-15',
            'CAMPANA_FECHA_FIN': '2026-12-31',
        }.get(parametro, default),
    )

    _, error = validar_fecha('2025-01-15', dominio='evaluacion_pesos')

    assert error is not None
    assert error['motivo'] == 'Fecha fuera del rango de campana (2026-01-15 -> 2026-12-31)'


def test_limites_de_campana_vigentes():
    fecha_inicio, valida_inicio = procesar_fecha(FECHA_CAMPANA_INICIO)
    fecha_fin, valida_fin = procesar_fecha(FECHA_CAMPANA_FIN)

    assert fecha_inicio == datetime(2025, 3, 1)
    assert valida_inicio is True
    assert fecha_fin == datetime(2026, 6, 30)
    assert valida_fin is True

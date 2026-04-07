from silver.facts.fact_conteo_fenologico import (
    _es_evaluacion_compatible_con_conteo,
    _normalizar_tipo_evaluacion,
)


def test_normalizar_tipo_evaluacion_quita_acentos_y_separadores():
    assert _normalizar_tipo_evaluacion('Conteo de Flores / Fenológico') == 'conteo de flores fenologico'


def test_evaluacion_vacia_se_considera_compatible():
    assert _es_evaluacion_compatible_con_conteo(None) is True
    assert _es_evaluacion_compatible_con_conteo('') is True


def test_evaluacion_de_conteo_es_compatible():
    assert _es_evaluacion_compatible_con_conteo('Conteo de Flores') is True
    assert _es_evaluacion_compatible_con_conteo('Seguimiento Fenológico') is True
    assert _es_evaluacion_compatible_con_conteo('Ensayo de Conteo') is True
    assert _es_evaluacion_compatible_con_conteo('Poda General') is True


def test_evaluacion_de_otro_dominio_se_rechaza():
    assert _es_evaluacion_compatible_con_conteo('Maduracion') is False
    assert _es_evaluacion_compatible_con_conteo('Evaluacion de Pesos') is False

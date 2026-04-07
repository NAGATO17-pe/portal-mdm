from runner.ejecutor import _PasoActivo, _extraer_inicio_paso, _linea_es_error_de_paso


def test_extraer_inicio_paso_detecta_hito_pipeline():
    paso = _extraer_inicio_paso("[12/22] Cargando Fact_Telemetria_Clima...")

    assert paso == (12, "Cargando Fact_Telemetria_Clima")


def test_extraer_inicio_paso_detecta_reproceso_parcial():
    paso = _extraer_inicio_paso("[03/5] Reprocesando Fact_Telemetria_Clima...")

    assert paso == (3, "Reprocesando Fact_Telemetria_Clima")


def test_linea_es_error_de_paso_marca_error_para_fact_actual():
    es_error, mensaje = _linea_es_error_de_paso(
        "  ERROR en Fact_Telemetria_Clima: decimal invalido",
        _PasoActivo(id_paso=1, nombre_paso="Cargando Fact_Telemetria_Clima", orden=12),
    )

    assert es_error is True
    assert "decimal invalido" in mensaje


def test_linea_es_error_de_paso_ignora_error_de_otra_fact():
    es_error, mensaje = _linea_es_error_de_paso(
        "  ERROR en Fact_Evaluacion_Pesos: fallo controlado",
        _PasoActivo(id_paso=1, nombre_paso="Cargando Fact_Telemetria_Clima", orden=12),
    )

    assert es_error is False
    assert mensaje is None

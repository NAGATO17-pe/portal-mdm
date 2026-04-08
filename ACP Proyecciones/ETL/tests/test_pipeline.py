from __future__ import annotations

from datetime import datetime

import pytest

import pipeline


_CARGADORES_FACTS_COMPLETAS = [
    "cargar_fact_cosecha_sap",
    "cargar_fact_conteo_fenologico",
    "cargar_fact_maduracion",
    "cargar_fact_peladas",
    "cargar_fact_telemetria_clima",
    "cargar_fact_evaluacion_pesos",
    "cargar_fact_tareo",
    "cargar_fact_fisiologia",
    "cargar_fact_evaluacion_vegetativa",
    "cargar_fact_induccion_floral",
    "cargar_fact_tasa_crecimiento_brotes",
    "cargar_fact_sanidad_activo",
    "cargar_fact_ciclo_poda",
]


def _resultado_fact_ok(*_args, **_kwargs) -> dict:
    return {
        "insertados": 1,
        "leidos": 1,
        "rechazados": 0,
        "tasa_rechazo_pct": 0.0,
        "motivos_principales": [],
        "cuarentena": [],
    }


def _parchear_entorno_pipeline(monkeypatch) -> None:
    monkeypatch.setattr(pipeline, "_encabezado", lambda: datetime(2026, 4, 8, 8, 0, 0))
    monkeypatch.setattr(pipeline, "_paso", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "_resumen_fact", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "_resumen_final", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "verificar_conexion", lambda: True)
    monkeypatch.setattr(pipeline, "obtener_engine", lambda: object())
    monkeypatch.setattr(pipeline, "limpiar_lookup", lambda: None)
    monkeypatch.setattr(pipeline, "limpiar_params", lambda: None)
    monkeypatch.setattr(pipeline, "registrar_inicio", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(pipeline, "registrar_fin", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "_obtener_contexto_sql",
        lambda _engine: {"servidor": "TEST_SERVER", "base_datos": "TEST_DB"},
    )
    monkeypatch.setattr(pipeline, "ejecutar_carga_bronce", lambda: [])
    monkeypatch.setattr(
        pipeline,
        "cargar_dim_personal",
        lambda _engine: {"insertados": 0, "actualizados": 0},
    )
    monkeypatch.setattr(
        pipeline,
        "cargar_dim_geografia",
        lambda _engine: {
            "vigentes": 1,
            "operativos": 1,
            "test_block": 0,
            "sin_cama_explicita": 0,
            "duplicados": 0,
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_ejecutar_sp_validar_camas",
        lambda _engine, **_kwargs: {
            "Cama_Fuera_Regla": 0,
            "Geografias_Saturadas": 0,
            "Estado_Calidad_Cama": "OK_OPERATIVO",
        },
    )


def test_ejecutar_omite_gold_y_falla_si_alguna_fact_revienta(monkeypatch):
    _parchear_entorno_pipeline(monkeypatch)

    for nombre in _CARGADORES_FACTS_COMPLETAS:
        monkeypatch.setattr(pipeline, nombre, _resultado_fact_ok)

    def _fact_fallida(_engine):
        raise RuntimeError("fallo controlado")

    monkeypatch.setattr(pipeline, "cargar_fact_cosecha_sap", _fact_fallida)

    llamadas_gold: list[object] = []

    def _gold_no_deberia_correr(*_args, **_kwargs):
        llamadas_gold.append("gold")
        return {"Gold.Mart_Cosecha": 1}

    monkeypatch.setattr(pipeline, "refrescar_todos_los_marts", _gold_no_deberia_correr)

    with pytest.raises(pipeline.ErrorEjecucionPipeline) as error:
        pipeline.ejecutar()

    assert llamadas_gold == []
    assert "Fact_Cosecha_SAP: fallo controlado" in str(error.value)


def test_reproceso_omite_gold_y_falla_si_fact_dirigida_revienta(monkeypatch):
    _parchear_entorno_pipeline(monkeypatch)
    monkeypatch.setattr(
        pipeline,
        "_preparar_fact_reproceso",
        lambda *_args, **_kwargs: {
            "filas_bronce_reabiertas": 0,
            "filas_destino_eliminadas": 0,
        },
    )

    meta_original = pipeline.CATALOGO_FACTS["Fact_Telemetria_Clima"]

    def _fact_fallida(_engine):
        raise RuntimeError("fallo reproceso")

    monkeypatch.setitem(
        pipeline.CATALOGO_FACTS,
        "Fact_Telemetria_Clima",
        {**meta_original, "funcion": _fact_fallida},
    )

    llamadas_gold: list[object] = []

    def _gold_no_deberia_correr(*_args, **_kwargs):
        llamadas_gold.append("gold")
        return {"Gold.Mart_Clima": 1}

    monkeypatch.setattr(pipeline, "refrescar_marts_seleccionados", _gold_no_deberia_correr)

    with pytest.raises(pipeline.ErrorEjecucionPipeline) as error:
        pipeline.ejecutar_reproceso_facts(
            ["Fact_Telemetria_Clima"],
            incluir_dependencias=False,
            refrescar_gold=True,
            forzar_relectura_bronce=True,
        )

    assert llamadas_gold == []
    assert "Fact_Telemetria_Clima: fallo reproceso" in str(error.value)

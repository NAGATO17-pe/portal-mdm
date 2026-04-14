from utils.ejecucion import (
    DEPENDENCIA_DIM_GEOGRAFIA,
    DEPENDENCIA_DIM_PERSONAL,
    DEPENDENCIA_SP_CAMA_SYNC,
    DEPENDENCIA_SP_CAMA_VALIDACION,
    CONFIG_FACTS,
    construir_catalogo_facts,
    obtener_catalogo_facts,
    obtener_tablas_bronce_por_dependencias,
    normalizar_facts_solicitadas,
    resolver_plan_reproceso,
)


def test_normalizar_facts_solicitadas_acepta_case_y_comas():
    facts = normalizar_facts_solicitadas([
        "fact_telemetria_clima",
        "Fact_Evaluacion_Pesos, fact_tareo",
        "Fact_Tareo",
    ])
    assert facts == [
        "Fact_Telemetria_Clima",
        "Fact_Evaluacion_Pesos",
        "Fact_Tareo",
    ]


def test_resolver_plan_reproceso_ordena_dependencias_y_marts():
    plan = resolver_plan_reproceso(
        ["Fact_Evaluacion_Pesos", "Fact_Telemetria_Clima"],
        incluir_dependencias=True,
        refrescar_gold=True,
    )
    assert plan["facts"] == ["Fact_Telemetria_Clima", "Fact_Evaluacion_Pesos"]
    assert plan["dependencias"] == [
        DEPENDENCIA_DIM_PERSONAL,
        DEPENDENCIA_DIM_GEOGRAFIA,
        DEPENDENCIA_SP_CAMA_SYNC,
        DEPENDENCIA_SP_CAMA_VALIDACION,
    ]
    assert plan["marts"] == ["Gold.Mart_Clima", "Gold.Mart_Pesos_Calibres"]


def test_resolver_plan_reproceso_rechaza_fact_desconocida():
    try:
        resolver_plan_reproceso(["Fact_Inexistente"])
    except ValueError as error:
        assert "Facts no reconocidas" in str(error)
    else:
        raise AssertionError("Se esperaba ValueError para una fact desconocida.")


def test_catalogo_facts_expone_estrategia_y_fuentes():
    catalogo = obtener_catalogo_facts()
    clima = next(item for item in catalogo if item["nombre_fact"] == "Fact_Telemetria_Clima")

    assert clima["tabla_destino"] == "Silver.Fact_Telemetria_Clima"
    assert clima["estrategia_rerun"] == "rebuild_fact"
    assert "Bronce.Reporte_Clima" in clima["fuentes_bronce"]


def test_construir_catalogo_facts_adjunta_funcion_y_respeta_orden():
    funciones = {nombre: object() for nombre in CONFIG_FACTS}

    catalogo = construir_catalogo_facts(funciones)

    assert list(catalogo.keys()) == list(CONFIG_FACTS.keys())
    assert catalogo["Fact_Telemetria_Clima"]["funcion"] is funciones["Fact_Telemetria_Clima"]


def test_obtener_tablas_bronce_por_dependencias_deduplica_y_respeta_orden_catalogo():
    tablas = obtener_tablas_bronce_por_dependencias([
        DEPENDENCIA_SP_CAMA_SYNC,
        DEPENDENCIA_SP_CAMA_VALIDACION,
    ])

    assert tablas == [
        "Bronce.Evaluacion_Pesos",
        "Bronce.Evaluacion_Vegetativa",
    ]


def test_resolver_plan_reproceso_falla_si_falta_estrategia_rerun():
    estrategia_original = CONFIG_FACTS["Fact_Tareo"].get("estrategia_rerun")
    CONFIG_FACTS["Fact_Tareo"]["estrategia_rerun"] = "NO_DECLARADA"
    try:
        resolver_plan_reproceso(["Fact_Tareo"])
    except ValueError as error:
        assert "estrategia_rerun declarada" in str(error)
    else:
        raise AssertionError("Se esperaba ValueError cuando falta estrategia_rerun.")
    finally:
        CONFIG_FACTS["Fact_Tareo"]["estrategia_rerun"] = estrategia_original

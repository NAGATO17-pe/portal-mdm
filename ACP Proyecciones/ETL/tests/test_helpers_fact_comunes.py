from __future__ import annotations

from silver.facts import _helpers_fact_comunes as helpers


def test_a_entero_nulo_convierte_texto_numerico():
    assert helpers.a_entero_nulo("12.0") == 12


def test_a_entero_no_negativo_descarta_negativos():
    assert helpers.a_entero_no_negativo("-1") is None


def test_texto_nulo_descarta_tokens_vacios():
    assert helpers.texto_nulo("nan") is None
    assert helpers.texto_nulo(" texto ") == "texto"


def test_parsear_valores_raw_omite_segmentos_invalidos():
    assert helpers.parsear_valores_raw(" A = 1 | invalido | B= 2 ") == {
        "A": "1",
        "B": "2",
    }


def test_motivo_cuarentena_geografia_para_estado_conocido():
    assert helpers.motivo_cuarentena_geografia({"estado": "NO_IDENTIFICADO"}) == (
        "Geografia no encontrada en Silver.Dim_Geografia."
    )


def test_motivo_cuarentena_geografia_para_fallback():
    assert helpers.motivo_cuarentena_geografia({"estado": "OTRO"}) == (
        "Geografia no encontrada en Silver.Dim_Geografia."
    )


def test_validar_layout_migrado_detecta_columnas_faltantes(monkeypatch):
    monkeypatch.setattr(
        helpers,
        "obtener_columnas_tabla",
        lambda _engine, tabla: {"A"} if tabla == "Bronce.Tabla" else {"ID"},
    )
    try:
        helpers.validar_layout_migrado(
            object(),
            tabla_origen="Bronce.Tabla",
            tabla_destino="Silver.Tabla",
            columna_id="ID",
            columnas_bronce_requeridas={"A", "B"},
            columnas_silver_requeridas={"ID"},
            nombre_layout="layout test",
        )
    except RuntimeError as error:
        assert "Bronce faltantes: ['B']" in str(error)
    else:
        raise AssertionError("Se esperaba RuntimeError por columnas faltantes")


def test_validar_layout_migrado_acepta_layout_valido(monkeypatch):
    monkeypatch.setattr(
        helpers,
        "obtener_columnas_tabla",
        lambda _engine, tabla: {"A", "B"} if tabla == "Bronce.Tabla" else {"ID"},
    )
    columna_id = helpers.validar_layout_migrado(
        object(),
        tabla_origen="Bronce.Tabla",
        tabla_destino="Silver.Tabla",
        columna_id="ID",
        columnas_bronce_requeridas={"A", "B"},
        columnas_silver_requeridas={"ID"},
        nombre_layout="layout test",
    )
    assert columna_id == "ID"

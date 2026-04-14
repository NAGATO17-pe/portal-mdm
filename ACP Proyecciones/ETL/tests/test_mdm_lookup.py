from __future__ import annotations

import pandas as pd

import mdm.lookup as lookup


def test_obtener_id_geografia_delega_a_resolver_geografia(monkeypatch):
    llamadas: list[dict] = []
    engine = object()

    def _resolver_stub(fundo, sector, modulo, engine, **kwargs):
        llamadas.append(
            {
                "fundo": fundo,
                "sector": sector,
                "modulo": modulo,
                "engine": engine,
                **kwargs,
            }
        )
        return {"id_geografia": 321}

    monkeypatch.setattr(lookup, "resolver_geografia", _resolver_stub)

    resultado = lookup.obtener_id_geografia(
        fundo="fundo 1",
        sector="sector a",
        modulo=9,
        engine=engine,
        turno=2,
        valvula="A1",
        cama="10",
    )

    assert resultado == 321
    assert len(llamadas) == 1
    assert llamadas[0]["fundo"] == "fundo 1"
    assert llamadas[0]["sector"] == "sector a"
    assert llamadas[0]["modulo"] == 9
    assert llamadas[0]["engine"] is engine
    assert llamadas[0]["turno"] == 2
    assert llamadas[0]["valvula"] == "A1"
    assert llamadas[0]["cama"] == "10"


def test_resolver_geografia_legacy_considera_submodulo_en_modulo_decimal(monkeypatch):
    monkeypatch.setattr(lookup, "_resolver_geografia_sp", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        lookup,
        "_cargar_dim_geografia",
        lambda _engine: pd.DataFrame([
            {
                "ID_Geografia": 100,
                "Fundo_token": None,
                "Sector_token": None,
                "Modulo_token": "9",
                "SubModulo_token": "1",
                "Turno_token": None,
                "Valvula_token": None,
                "Cama_token": None,
            },
            {
                "ID_Geografia": 200,
                "Fundo_token": None,
                "Sector_token": None,
                "Modulo_token": "9",
                "SubModulo_token": "2",
                "Turno_token": None,
                "Valvula_token": None,
                "Cama_token": None,
            },
        ]),
    )

    resultado = lookup.resolver_geografia(None, None, "9.1", object())

    assert resultado["id_geografia"] == 100
    assert resultado["estado"] == "RESUELTA_DIM_GEOGRAFIA"

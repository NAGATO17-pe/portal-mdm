from __future__ import annotations

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

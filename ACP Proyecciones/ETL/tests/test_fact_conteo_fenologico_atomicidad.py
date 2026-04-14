from __future__ import annotations

import pandas as pd

from silver.facts import fact_conteo_fenologico as modulo


def test_fact_conteo_fenologico_usa_misma_transaccion_para_side_effects(monkeypatch):
    class _Conexion:
        def __init__(self):
            self.insert_payloads = []

        def execute(self, sentencia, payload=None):
            texto = str(sentencia)
            if 'INSERT INTO Silver.Fact_Conteo_Fenologico' in texto:
                if isinstance(payload, list):
                    self.insert_payloads.extend(payload)
                elif payload is not None:
                    self.insert_payloads.append(payload)
            return None

    class _Gestor:
        def __init__(self, conexion):
            self.conexion = conexion

        def __enter__(self):
            return self.conexion

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Engine:
        def __init__(self):
            self.conexion = _Conexion()

        def begin(self):
            return _Gestor(self.conexion)

    engine = _Engine()
    llamadas = {
        'homologacion': None,
        'mark': [],
        'cuarentena': None,
    }

    monkeypatch.setattr(
        modulo,
        '_leer_bronce',
        lambda _engine: pd.DataFrame([
            {
                'ID_Conteo_Fruta': 1,
                'Fecha_Raw': '2026-04-10',
                'Fundo_Raw': 'Fundo 1',
                'Sector_Raw': 'Sector A',
                'Modulo_Raw': '1',
                'Turno_Raw': '1',
                'Valvula_Raw': 'A1',
                'Variedad_Raw': 'Biloxi',
                'Evaluador_Raw': '12345678',
                'Color_Cinta_Raw': 'Roja',
                'Estado_Raw': 'Flor',
                'Cantidad_Organos_Raw': '12',
                'Tipo_Evaluacion_Raw': 'Conteo de Flores',
                'Valores_Raw': None,
            },
            {
                'ID_Conteo_Fruta': 2,
                'Fecha_Raw': 'fecha_invalida',
                'Fundo_Raw': 'Fundo 1',
                'Sector_Raw': 'Sector A',
                'Modulo_Raw': '1',
                'Turno_Raw': '1',
                'Valvula_Raw': 'A1',
                'Variedad_Raw': 'Biloxi',
                'Evaluador_Raw': '12345678',
                'Color_Cinta_Raw': 'Roja',
                'Estado_Raw': 'Flor',
                'Cantidad_Organos_Raw': '12',
                'Tipo_Evaluacion_Raw': 'Conteo de Flores',
                'Valores_Raw': None,
            },
        ]),
    )

    def _homologar_columna(df, columna_raw, columna_destino, tabla_origen, recurso_db, **_kwargs):
        llamadas['homologacion'] = recurso_db
        return df.assign(Variedad_Canonica='Biloxi'), []

    def _procesar_fecha(valor, **_kwargs):
        if valor == 'fecha_invalida':
            return None, False
        return pd.Timestamp(valor), True

    monkeypatch.setattr(modulo, 'homologar_columna', _homologar_columna)
    monkeypatch.setattr(modulo, 'procesar_fecha', _procesar_fecha)
    monkeypatch.setattr(modulo, 'obtener_id_tiempo', lambda *_args, **_kwargs: 20260410)
    monkeypatch.setattr(modulo, 'resolver_geografia', lambda *_args, **_kwargs: {'id_geografia': 111})
    monkeypatch.setattr(modulo, 'obtener_id_variedad', lambda *_args, **_kwargs: 222)
    monkeypatch.setattr(modulo, 'procesar_dni', lambda *_args, **_kwargs: ('12345678', True))
    monkeypatch.setattr(modulo, 'obtener_id_personal', lambda *_args, **_kwargs: 333)
    monkeypatch.setattr(modulo, 'obtener_id_estado_fenologico', lambda *_args, **_kwargs: 444)
    monkeypatch.setattr(
        modulo.ContextoTransaccionalETL,
        'marcar_estado_carga',
        lambda self, *args, **kwargs: llamadas['mark'].append(self.conexion) or 1,
    )
    monkeypatch.setattr(
        modulo.ContextoTransaccionalETL,
        'enviar_cuarentena',
        lambda self, *args, **kwargs: llamadas.__setitem__('cuarentena', self.conexion) or 0,
    )

    resumen = modulo.cargar_fact_conteo_fenologico(engine)

    assert resumen['insertados'] == 1
    assert resumen['rechazados'] == 1
    assert len(engine.conexion.insert_payloads) == 1
    assert llamadas['homologacion'] is engine.conexion
    assert llamadas['mark'] == [engine.conexion, engine.conexion]
    assert llamadas['cuarentena'] is engine.conexion

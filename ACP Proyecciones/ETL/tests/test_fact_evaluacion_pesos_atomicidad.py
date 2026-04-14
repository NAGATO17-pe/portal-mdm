from __future__ import annotations

import pandas as pd

from silver.facts import fact_evaluacion_pesos as modulo


def test_fact_evaluacion_pesos_usa_misma_transaccion_para_side_effects(monkeypatch):
    class _Conexion:
        def __init__(self):
            self.insert_payloads = []

        def execute(self, sentencia, payload=None):
            texto = str(sentencia)
            if 'INSERT INTO Silver.Fact_Evaluacion_Pesos' in texto:
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
        'mark': None,
        'cuarentena': None,
    }

    monkeypatch.setattr(
        modulo,
        '_leer_bronce',
        lambda _engine: pd.DataFrame([
            {
                'ID_Evaluacion_Pesos': 10,
                'Fecha_Raw': '2026-04-10',
                'Modulo_Raw': '1',
                'Valvula_Raw': 'A1',
                'Turno_Raw': '1',
                'Cama_Raw': '10',
                'Variedad_Raw': 'Biloxi',
                'DNI_Raw': '12345678',
                'CantMuestra_Raw': '10',
                'PesoBaya_Raw': '20',
                'BayasPequenas_Raw': '0',
                'PesoBayasPequenas_Raw': '0',
                'BayasGrandes_Raw': '0',
                'BayasFase1_Raw': '0',
                'PesoBayasFase1_Raw': '0',
                'BayasFase2_Raw': '0',
                'PesoBayasFase2_Raw': '0',
                'Cremas_Raw': '0',
                'PesoCremas_Raw': '0',
                'Maduras_Raw': '0',
                'PesoMaduras_Raw': '0',
                'Cosechables_Raw': '0',
                'PesoCosechables_Raw': '0',
            }
            ,
            {
                'ID_Evaluacion_Pesos': 11,
                'Fecha_Raw': 'fecha_invalida',
                'Modulo_Raw': '1',
                'Valvula_Raw': 'A1',
                'Turno_Raw': '1',
                'Cama_Raw': '10',
                'Variedad_Raw': 'Biloxi',
                'DNI_Raw': '12345678',
                'CantMuestra_Raw': '10',
                'PesoBaya_Raw': '20',
                'BayasPequenas_Raw': '0',
                'PesoBayasPequenas_Raw': '0',
                'BayasGrandes_Raw': '0',
                'BayasFase1_Raw': '0',
                'PesoBayasFase1_Raw': '0',
                'BayasFase2_Raw': '0',
                'PesoBayasFase2_Raw': '0',
                'Cremas_Raw': '0',
                'PesoCremas_Raw': '0',
                'Maduras_Raw': '0',
                'PesoMaduras_Raw': '0',
                'Cosechables_Raw': '0',
                'PesoCosechables_Raw': '0',
            }
        ]),
    )
    def _homologar_columna(df, columna_raw, columna_destino, tabla_origen, recurso_db, **_kwargs):
        llamadas['homologacion'] = recurso_db
        return df.assign(Variedad_Canonica='Biloxi'), []

    monkeypatch.setattr(modulo, 'homologar_columna', _homologar_columna)
    def _procesar_fecha(valor, **_kwargs):
        if valor == 'fecha_invalida':
            return None, False
        return '2026-04-10', True

    monkeypatch.setattr(modulo, 'procesar_fecha', _procesar_fecha)
    monkeypatch.setattr(modulo, 'construir_id_tiempo', lambda _fecha: 20260410)
    monkeypatch.setattr(modulo, 'obtener_id_tiempo', lambda *_args, **_kwargs: 20260410)
    monkeypatch.setattr(modulo, 'resolver_geografia', lambda *_args, **_kwargs: {'id_geografia': 111})
    monkeypatch.setattr(modulo, 'obtener_id_variedad', lambda *_args, **_kwargs: 222)
    monkeypatch.setattr(modulo, 'procesar_dni', lambda *_args, **_kwargs: ('12345678', True))
    monkeypatch.setattr(modulo, 'obtener_id_personal', lambda *_args, **_kwargs: 333)
    monkeypatch.setattr(modulo, 'validar_peso_baya', lambda peso: (peso, None))
    monkeypatch.setattr(
        modulo.ContextoTransaccionalETL,
        'marcar_estado_carga',
        lambda self, *args, **kwargs: llamadas.__setitem__('mark', self.conexion) or 1,
    )
    monkeypatch.setattr(
        modulo.ContextoTransaccionalETL,
        'enviar_cuarentena',
        lambda self, *args, **kwargs: llamadas.__setitem__('cuarentena', self.conexion) or 0,
    )

    resumen = modulo.cargar_fact_evaluacion_pesos(engine)

    assert resumen['insertados'] == 1
    assert resumen['rechazados'] == 1
    assert engine.conexion.insert_payloads
    assert llamadas['homologacion'] is engine.conexion
    assert llamadas['mark'] is engine.conexion
    assert llamadas['cuarentena'] is engine.conexion

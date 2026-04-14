from __future__ import annotations

import pandas as pd

from silver.facts import fact_evaluacion_vegetativa as modulo


def test_fact_evaluacion_vegetativa_usa_misma_transaccion_y_separa_rechazados(monkeypatch):
    class _Conexion:
        def __init__(self):
            self.insert_payloads = []

        def execute(self, sentencia, payload=None):
            texto = str(sentencia)
            if 'INSERT INTO Silver.Fact_Evaluacion_Vegetativa' in texto:
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

    monkeypatch.setattr(modulo, '_validar_layout_migrado', lambda _engine: 'ID_Evaluacion_Vegetativa')
    monkeypatch.setattr(
        modulo,
        '_leer_bronce',
        lambda _engine, _columna_id: pd.DataFrame([
            {
                'ID_Registro_Origen': 1,
                'Fecha_Raw': '2026-04-10',
                'DNI_Raw': '12345678',
                'Modulo_Raw': '1',
                'Turno_Raw': '1',
                'Valvula_Raw': 'A1',
                'Cama_Raw': '10',
                'Descripcion_Raw': 'Biloxi',
                'Evaluacion_Raw': 'Semanal',
                'N_Plantas_Evaluadas_Raw': '10',
                'N_Plantas_en_Floracion_Raw': '15',
            },
            {
                'ID_Registro_Origen': 2,
                'Fecha_Raw': 'fecha_invalida',
                'DNI_Raw': '12345678',
                'Modulo_Raw': '1',
                'Turno_Raw': '1',
                'Valvula_Raw': 'A1',
                'Cama_Raw': '10',
                'Descripcion_Raw': 'Biloxi',
                'Evaluacion_Raw': 'Semanal',
                'N_Plantas_Evaluadas_Raw': '10',
                'N_Plantas_en_Floracion_Raw': '5',
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
    monkeypatch.setattr(modulo, 'resolver_geografia', lambda *_args, **_kwargs: {'id_geografia': 111})
    monkeypatch.setattr(modulo, 'obtener_id_variedad', lambda *_args, **_kwargs: 222)
    monkeypatch.setattr(modulo, 'construir_id_tiempo', lambda _fecha: 20260410)
    monkeypatch.setattr(modulo, 'obtener_id_tiempo', lambda *_args, **_kwargs: 20260410)
    monkeypatch.setattr(modulo, 'procesar_dni', lambda *_args, **_kwargs: ('12345678', True))
    monkeypatch.setattr(modulo, 'obtener_id_personal', lambda *_args, **_kwargs: 333)
    def _marcar_estado_carga(self, tabla_origen, columna_id, ids, estado='PROCESADO', **_kwargs):
        llamadas['mark'].append((self.conexion, tabla_origen, columna_id, list(ids), estado))
        return 1

    monkeypatch.setattr(
        modulo.ContextoTransaccionalETL,
        'marcar_estado_carga',
        _marcar_estado_carga,
    )
    monkeypatch.setattr(
        modulo.ContextoTransaccionalETL,
        'enviar_cuarentena',
        lambda self, *args, **kwargs: llamadas.__setitem__('cuarentena', self.conexion) or 0,
    )

    resumen = modulo.cargar_fact_evaluacion_vegetativa(engine)

    assert resumen['insertados'] == 0
    assert resumen['rechazados'] == 2
    assert len(engine.conexion.insert_payloads) == 0
    assert llamadas['homologacion'] is engine.conexion
    assert llamadas['mark'] == [
        (engine.conexion, modulo.TABLA_ORIGEN, 'ID_Evaluacion_Vegetativa', [1, 2], 'RECHAZADO'),
    ]
    assert llamadas['cuarentena'] is engine.conexion

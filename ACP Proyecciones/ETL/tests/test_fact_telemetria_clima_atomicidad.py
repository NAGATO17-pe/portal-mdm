from __future__ import annotations

import pandas as pd

from silver.facts import fact_telemetria_clima as modulo


def test_fact_telemetria_clima_usa_misma_transaccion_para_side_effects(monkeypatch):
    class _Conexion:
        def __init__(self):
            self.insert_payloads = []

        def execute(self, sentencia, payload=None):
            texto = str(sentencia)
            if 'INSERT INTO Silver.Fact_Telemetria_Clima' in texto:
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
        'mark': [],
        'cuarentena': None,
    }

    monkeypatch.setattr(
        modulo,
        '_leer_bronce_clima',
        lambda _engine: pd.DataFrame([
            {
                'ID_Reporte_Clima': 1,
                'Fecha_Raw': '2026-04-10',
                'Hora_Raw': '08:00',
                'Sector_Raw': 'F07',
                'TempMax_Raw': '28.5',
                'TempMin_Raw': '18.2',
                'Humedad_Raw': '55',
                'Precipitacion_Raw': '0',
            },
            {
                'ID_Reporte_Clima': 2,
                'Fecha_Raw': 'fecha_invalida',
                'Hora_Raw': '08:00',
                'Sector_Raw': 'F07',
                'TempMax_Raw': '28.5',
                'TempMin_Raw': '18.2',
                'Humedad_Raw': '55',
                'Precipitacion_Raw': '0',
            },
        ]),
    )
    monkeypatch.setattr(
        modulo,
        '_leer_bronce_variables',
        lambda _engine: pd.DataFrame([
            {
                'ID_Variables_Meteorologicas': 10,
                'Fecha_Raw': '2026-04-10',
                'Sector_Raw': 'F07',
                'VPD_Raw': '1.2',
                'Radiacion_Raw': '500',
                'TempMax_Raw': '28.5',
                'TempMin_Raw': '18.2',
                'Humedad_Raw': '55',
                'Valores_Raw': 'Hora_Raw=09:00:00',
            }
        ]),
    )

    def _procesar_fecha(valor, **_kwargs):
        if 'fecha_invalida' in str(valor):
            return None, False
        return pd.Timestamp(str(valor)), True

    monkeypatch.setattr(modulo, 'procesar_fecha', _procesar_fecha)
    monkeypatch.setattr(modulo, 'construir_id_tiempo', lambda _fecha: 20260410)
    monkeypatch.setattr(modulo, 'obtener_id_tiempo_dim', lambda *_args, **_kwargs: 20260410)
    monkeypatch.setattr(modulo, 'normalizar_humedad', lambda valor: (55.0, None))
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

    resumen = modulo.cargar_fact_telemetria_clima(engine)

    assert resumen['insertados'] == 2
    assert resumen['rechazados'] == 1
    assert len(engine.conexion.insert_payloads) == 2
    assert llamadas['mark'] == [
        engine.conexion,
        engine.conexion,
        engine.conexion,
    ]
    assert llamadas['cuarentena'] is engine.conexion

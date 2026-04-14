from __future__ import annotations

import pandas as pd

from silver.facts import fact_cosecha_sap as modulo


def test_fact_cosecha_sap_usa_misma_transaccion_para_ambas_fuentes(monkeypatch):
    class _Conexion:
        def __init__(self):
            self.insert_payloads = []

        def execute(self, sentencia, payload=None):
            texto = str(sentencia)
            if 'INSERT INTO Silver.Fact_Cosecha_SAP' in texto:
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
        'homologacion': [],
        'mark': [],
        'cuarentena': None,
    }

    monkeypatch.setattr(
        modulo,
        '_leer_bronce_cosecha',
        lambda _engine: pd.DataFrame([
            {
                'ID_Reporte_Cosecha': 1,
                'Fecha_Raw': '2026-04-10',
                'Fundo_Raw': 'Fundo 1',
                'Modulo_Raw': '1',
                'Variedad_Raw': 'Biloxi',
                'KgNeto_Raw': '100',
                'Jabas_Raw': '20',
                'Lote_Raw': 'L1',
                'Responsable_Raw': 'juan perez',
            }
        ]),
    )
    monkeypatch.setattr(
        modulo,
        '_leer_bronce_sap',
        lambda _engine: pd.DataFrame([
            {
                'ID_Data_SAP': 10,
                'Fecha_Raw': '2026-04-10',
                'Fundo_Raw': 'Fundo 1',
                'Modulo_Raw': '1',
                'Variedad_Raw': 'Biloxi',
                'Peso_Bruto_Raw': '120',
                'Peso_Neto_Raw': '100',
                'Cantidad_Jabas_Raw': '20',
                'Lote_Raw': 'L2',
                'Almacen_Raw': 'ALM',
                'Doc_Remision_Raw': 'DOC1',
                'Codigo_Cliente_Raw': 'CLI1',
                'Responsable_Raw': 'maria perez',
                'Descripcion_Material_Raw': 'Material',
                'Material_Codigo_Raw': 'MAT1',
                'Fecha_Recepcion_Raw': '2026-04-11',
            }
        ]),
    )

    def _homologar_columna(df, columna_raw, columna_destino, tabla_origen, recurso_db, **_kwargs):
        llamadas['homologacion'].append(recurso_db)
        cuarentena = []
        if tabla_origen == modulo.TABLA_COSECHA:
            cuarentena = [
                {
                    'Campo_Origen': 'Variedad_Raw',
                    'Valor_Recibido': 'Biloxi typo',
                    'Motivo': 'Homologacion pendiente',
                    'Tipo_Regla': 'MDM',
                }
            ]
        return df.assign(Variedad_Canonica='Biloxi'), cuarentena

    monkeypatch.setattr(modulo, 'homologar_columna', _homologar_columna)
    monkeypatch.setattr(modulo, 'procesar_fecha', lambda valor, **_kwargs: (pd.Timestamp(str(valor)), True))
    monkeypatch.setattr(modulo, 'resolver_geografia', lambda *_args, **_kwargs: {'id_geografia': 111})
    monkeypatch.setattr(modulo, 'obtener_id_variedad', lambda *_args, **_kwargs: 222)
    monkeypatch.setattr(modulo, 'obtener_id_tiempo', lambda *_args, **_kwargs: 20260410)
    monkeypatch.setattr(modulo, '_obtener_id_condicion_default', lambda: 77)
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

    resumen = modulo.cargar_fact_cosecha_sap(engine)

    assert resumen['insertados'] == 2
    assert len(engine.conexion.insert_payloads) == 2
    assert all(payload['id_condicion'] == 77 for payload in engine.conexion.insert_payloads)
    assert llamadas['homologacion'] == [engine.conexion, engine.conexion]
    assert llamadas['mark'] == [engine.conexion, engine.conexion]
    assert llamadas['cuarentena'] is engine.conexion

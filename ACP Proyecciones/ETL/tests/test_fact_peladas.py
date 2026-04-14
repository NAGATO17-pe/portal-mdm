import pandas as pd

from silver.facts import fact_peladas


def test_obtener_valor_raw_usa_valores_raw_si_falta_columna_directa():
    fila = pd.Series({
        'Punto_Raw': None,
        'Valores_Raw': 'Punto_Raw=4 | Turno_Raw=3',
    })

    assert fact_peladas._obtener_valor_raw(fila, 'Punto_Raw') == '4'


def test_cargar_fact_peladas_usa_turno_y_valvula_desde_layout_real(monkeypatch):
    df = pd.DataFrame([{
        'ID_Peladas': 1,
        'Fecha_Raw': '2025-04-30',
        'Fundo_Raw': None,
        'Modulo_Raw': '5',
        'Turno_Raw': None,
        'Valvula_Raw': None,
        'Variedad_Raw': 'BILOXI',
        'DNI_Raw': None,
        'Evaluador_Raw': 'CARMEN DINA CHOCAN LLACSAHUANGA',
        'Punto_Raw': None,
        'Muestras_Raw': None,
        'BotonesFlorales_Raw': None,
        'Flores_Raw': None,
        'BayasPequenas_Raw': None,
        'BayasGrandes_Raw': None,
        'Fase1_Raw': None,
        'Fase2_Raw': None,
        'BayasCremas_Raw': None,
        'BayasMaduras_Raw': None,
        'BayasCosechables_Raw': None,
        'PlantasProductivas_Raw': None,
        'PlantasNoProductivas_Raw': None,
        'Valores_Raw': (
            'Turno_Raw=3 | Valvula_Raw=1 | Punto_Raw=4 | Muestras_Raw=8 | '
            'DNI_Raw=76007517 | BotonesFlorales_Raw=1127 | Flores_Raw=974'
        ),
        'Variedad_Canonica': 'BILOXI',
    }])

    class _ConexionDummy:
        def __init__(self):
            self.ejecuciones = []

        def execute(self, sentencia, parametros=None):
            self.ejecuciones.append((str(sentencia), parametros))
            return None

    class _ContextoDummy:
        instancias = []

        def __init__(self, _engine):
            self.conexion = _ConexionDummy()
            self.marcas = []
            self.cuarentenas = []
            _ContextoDummy.instancias.append(self)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def _conexion_activa(self):
            return self.conexion

        def marcar_estado_carga(self, tabla, columna_id, ids, estado='PROCESADO', **_kwargs):
            self.marcas.append((tabla, columna_id, list(ids), estado))
            return len(ids)

        def enviar_cuarentena(self, tabla, filas):
            self.cuarentenas.append((tabla, list(filas)))
            return len(filas)

    capturado_geo = {}

    monkeypatch.setattr(fact_peladas, '_leer_bronce', lambda _engine: df.copy())
    monkeypatch.setattr(
        fact_peladas,
        'homologar_columna',
        lambda df_in, *_args, **_kwargs: (df_in, []),
    )
    monkeypatch.setattr(fact_peladas, 'ContextoTransaccionalETL', _ContextoDummy)
    monkeypatch.setattr(
        fact_peladas,
        'resolver_geografia',
        lambda fundo, sector, modulo, engine, turno=None, valvula=None, cama=None: (
            capturado_geo.update({
                'fundo': fundo,
                'sector': sector,
                'modulo': modulo,
                'turno': turno,
                'valvula': valvula,
                'cama': cama,
            }) or {'id_geografia': 101, 'estado': 'RESUELTA_BASE_SIN_CAMA', 'detalle': 'ok'}
        ),
    )
    monkeypatch.setattr(fact_peladas, 'obtener_id_variedad', lambda *_args, **_kwargs: 202)
    monkeypatch.setattr(fact_peladas, 'obtener_id_personal', lambda *_args, **_kwargs: 303)
    monkeypatch.setattr(fact_peladas, 'obtener_id_tiempo', lambda *_args, **_kwargs: 20250430)

    resultado = fact_peladas.cargar_fact_peladas(object())

    assert resultado['insertados'] == 1
    assert capturado_geo == {
        'fundo': None,
        'sector': None,
        'modulo': '5',
        'turno': '3',
        'valvula': '1',
        'cama': None,
    }


def test_cargar_fact_peladas_persiste_motivo_detallado_en_rechazos(monkeypatch):
    df = pd.DataFrame([
        {
            'ID_Peladas': 1,
            'Fecha_Raw': '2025-04-30',
            'Fundo_Raw': None,
            'Modulo_Raw': '9.1',
            'Turno_Raw': '16',
            'Valvula_Raw': '50',
            'Variedad_Raw': 'BILOXI',
            'DNI_Raw': '76007517',
            'Evaluador_Raw': 'X',
            'Punto_Raw': '1',
            'Muestras_Raw': '8',
            'BotonesFlorales_Raw': '1',
            'Flores_Raw': '1',
            'BayasPequenas_Raw': '1',
            'BayasGrandes_Raw': '1',
            'Fase1_Raw': '1',
            'Fase2_Raw': '1',
            'BayasCremas_Raw': '1',
            'BayasMaduras_Raw': '1',
            'BayasCosechables_Raw': '1',
            'PlantasProductivas_Raw': '1',
            'PlantasNoProductivas_Raw': '0',
            'Valores_Raw': None,
            'Variedad_Canonica': 'BILOXI',
        },
        {
            'ID_Peladas': 2,
            'Fecha_Raw': '2025-04-30',
            'Fundo_Raw': None,
            'Modulo_Raw': '5',
            'Turno_Raw': '3',
            'Valvula_Raw': '1',
            'Variedad_Raw': 'RARISIMA',
            'DNI_Raw': '76007517',
            'Evaluador_Raw': 'X',
            'Punto_Raw': '1',
            'Muestras_Raw': '8',
            'BotonesFlorales_Raw': '1',
            'Flores_Raw': '1',
            'BayasPequenas_Raw': '1',
            'BayasGrandes_Raw': '1',
            'Fase1_Raw': '1',
            'Fase2_Raw': '1',
            'BayasCremas_Raw': '1',
            'BayasMaduras_Raw': '1',
            'BayasCosechables_Raw': '1',
            'PlantasProductivas_Raw': '1',
            'PlantasNoProductivas_Raw': '0',
            'Valores_Raw': None,
            'Variedad_Canonica': 'RARISIMA',
        },
    ])

    class _ConexionDummy:
        def execute(self, sentencia, parametros=None):
            return None

    class _ContextoDummy:
        instancias = []

        def __init__(self, _engine):
            self.conexion = _ConexionDummy()
            self.marcas = []
            self.cuarentenas = []
            _ContextoDummy.instancias.append(self)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def _conexion_activa(self):
            return self.conexion

        def marcar_estado_carga(self, tabla, columna_id, ids, estado='PROCESADO', **_kwargs):
            self.marcas.append((tabla, columna_id, list(ids), estado))
            return len(ids)

        def enviar_cuarentena(self, tabla, filas):
            self.cuarentenas.append((tabla, list(filas)))
            return len(filas)

    def _resolver_geo(_fundo, _sector, modulo, _engine, turno=None, valvula=None, cama=None):
        if str(modulo) == '9.1':
            return {
                'id_geografia': None,
                'estado': 'GEOGRAFIA_NO_ENCONTRADA',
                'detalle': 'No existe geografia vigente para modulo/submodulo/turno/valvula.',
            }
        return {'id_geografia': 101, 'estado': 'RESUELTA_BASE_SIN_CAMA', 'detalle': 'ok'}

    monkeypatch.setattr(fact_peladas, '_leer_bronce', lambda _engine: df.copy())
    monkeypatch.setattr(
        fact_peladas,
        'homologar_columna',
        lambda df_in, *_args, **_kwargs: (df_in, []),
    )
    monkeypatch.setattr(fact_peladas, 'ContextoTransaccionalETL', _ContextoDummy)
    monkeypatch.setattr(fact_peladas, 'resolver_geografia', _resolver_geo)
    monkeypatch.setattr(
        fact_peladas,
        'obtener_id_variedad',
        lambda variedad, *_args, **_kwargs: None if variedad == 'RARISIMA' else 202,
    )
    monkeypatch.setattr(fact_peladas, 'obtener_id_personal', lambda *_args, **_kwargs: 303)
    monkeypatch.setattr(fact_peladas, 'obtener_id_tiempo', lambda *_args, **_kwargs: 20250430)

    resultado = fact_peladas.cargar_fact_peladas(object())

    assert resultado['insertados'] == 0
    assert resultado['rechazados'] == 2
    assert len(resultado['cuarentena']) == 2
    assert resultado['cuarentena'][0]['motivo'] == 'No existe geografia vigente para modulo/submodulo/turno/valvula.'
    assert resultado['cuarentena'][1]['motivo'] == 'Variedad sin match en Dim_Variedad'

from pathlib import Path

import pandas as pd

from bronce import cargador


def test_leer_excel_peladas_prioriza_hoja_bd_lt(monkeypatch):
    class _Libro:
        sheet_names = ['BD', 'BD_LT']

    llamadas = []

    monkeypatch.setattr(cargador.pd, 'ExcelFile', lambda *_args, **_kwargs: _Libro())

    def _fake_leer(*_args, **kwargs):
        llamadas.append((kwargs['sheet_name'], kwargs['header_idx']))
        return pd.DataFrame({'Fecha_Raw': ['2025-04-30']})

    monkeypatch.setattr(cargador, '_leer_excel_especial', _fake_leer)

    df = cargador._leer_excel_peladas_bd(Path('Peladas_V2.xlsx'))

    assert not df.empty
    assert llamadas == [('BD_LT', 0)]


def test_proyectar_dataframe_peladas_mapea_columnas_y_serializa_extras(monkeypatch):
    df_fuente = pd.DataFrame({
        'Ano_Raw': ['2025'],
        'Semana_Raw': ['Sem 18'],
        'Fecha_Raw': ['2025-04-30'],
        'Fecha_Subida_Raw': ['2025-04-30 16:03:27'],
        'DNI_Raw': ['76007517'],
        'Nombres_Raw': ['CARMEN DINA CHOCAN LLACSAHUANGA'],
        'Modulo_Raw': ['5'],
        'Turno_Raw': ['3'],
        'Valvula_Raw': ['1'],
        'Tipo_Evaluacion_Raw': ['PELADAS'],
        'Punto_Raw': ['4'],
        'Variedad_Raw': ['BILOXI'],
        'BotonesFlorales_Raw': ['1127'],
        'Flores_Raw': ['974'],
        'BayasPequenas_Raw': ['0'],
        'BayasGrandes_Raw': ['0'],
        'Fase1_Raw': ['0'],
        'Fase2_Raw': ['0'],
        'BayasCremas_Raw': ['0'],
        'BayasMaduras_Raw': ['0'],
        'BayasCosechables_Raw': ['0'],
        'PlantasProductivas_Raw': ['0'],
        'PlantasNoProductivas_Raw': ['0'],
        'Muestras_Raw': ['8'],
        'TotalPlantas_Raw': ['0'],
        'TOTAL_Raw': ['2101'],
    })

    monkeypatch.setattr(cargador, '_leer_excel_peladas_bd', lambda _ruta: df_fuente.copy())

    salida = cargador._proyectar_dataframe_peladas_bronce(Path('Peladas_V2.xlsx'))

    assert salida.loc[0, 'Fecha_Raw'] == '2025-04-30'
    assert salida.loc[0, 'Modulo_Raw'] == '5'
    assert salida.loc[0, 'Turno_Raw'] == '3'
    assert salida.loc[0, 'Valvula_Raw'] == '1'
    assert salida.loc[0, 'Muestras_Raw'] == '8'
    assert salida.loc[0, 'BotonesFlorales_Raw'] == '1127'
    assert salida.loc[0, 'Evaluador_Raw'] == 'CARMEN DINA CHOCAN LLACSAHUANGA'
    assert 'Ano_Raw=2025' in salida.loc[0, 'Valores_Raw']
    assert 'TOTAL_Raw=2101' in salida.loc[0, 'Valores_Raw']

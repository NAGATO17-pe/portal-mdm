from conftest import assert_columnas_existen, assert_tabla_existe


def test_existe_fact_conteo_fenologico(engine):
    assert_tabla_existe(engine, 'Silver', 'Fact_Conteo_Fenologico')
    assert_columnas_existen(
        engine,
        'Silver',
        'Fact_Conteo_Fenologico',
        ['ID_Geografia', 'ID_Tiempo', 'ID_Personal', 'ID_Variedad', 'ID_Estado_Fenologico'],
    )


def test_existe_fact_evaluacion_pesos(engine):
    assert_tabla_existe(engine, 'Silver', 'Fact_Evaluacion_Pesos')
    assert_columnas_existen(
        engine,
        'Silver',
        'Fact_Evaluacion_Pesos',
        ['ID_Geografia', 'ID_Tiempo', 'ID_Personal', 'ID_Variedad', 'Peso_Promedio_Baya_g'],
    )


def test_existe_fact_evaluacion_vegetativa(engine):
    assert_tabla_existe(engine, 'Silver', 'Fact_Evaluacion_Vegetativa')
    assert_columnas_existen(
        engine,
        'Silver',
        'Fact_Evaluacion_Vegetativa',
        ['ID_Geografia', 'ID_Tiempo', 'ID_Personal', 'ID_Variedad'],
    )


def test_existe_fact_ciclo_poda(engine):
    assert_tabla_existe(engine, 'Silver', 'Fact_Ciclo_Poda')
    assert_columnas_existen(
        engine,
        'Silver',
        'Fact_Ciclo_Poda',
        ['ID_Geografia', 'ID_Tiempo', 'ID_Variedad'],
    )


def test_existe_fact_maduracion(engine):
    assert_tabla_existe(engine, 'Silver', 'Fact_Maduracion')
    assert_columnas_existen(
        engine,
        'Silver',
        'Fact_Maduracion',
        [
            'ID_Geografia',
            'ID_Tiempo',
            'ID_Personal',
            'ID_Variedad',
            'ID_Estado_Fenologico',
            'ID_Cinta',
            'ID_Organo',
        ],
    )



def test_existe_fact_telemetria_clima(engine):
    assert_tabla_existe(engine, 'Silver', 'Fact_Telemetria_Clima')
    assert_columnas_existen(
        engine,
        'Silver',
        'Fact_Telemetria_Clima',
        ['Sector_Climatico', 'ID_Tiempo', 'Temperatura_Max_C', 'Temperatura_Min_C', 'Humedad_Relativa_Pct'],
    )

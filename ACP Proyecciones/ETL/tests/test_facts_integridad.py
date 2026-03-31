from conftest import scalar, skip_si_fact_vacio


def _assert_sin_huerfanas(engine, tabla_fact: str, columna_fk: str, tabla_dim: str, columna_dim: str) -> None:
    skip_si_fact_vacio(engine, tabla_fact)
    huerfanas = scalar(
        engine,
        f"""
        SELECT COUNT(*)
        FROM {tabla_fact} f
        LEFT JOIN {tabla_dim} d
          ON d.{columna_dim} = f.{columna_fk}
        WHERE f.{columna_fk} IS NOT NULL
          AND d.{columna_dim} IS NULL
        """,
    )
    assert int(huerfanas or 0) == 0, f'Hay huerfanas en {tabla_fact}.{columna_fk} -> {tabla_dim}.{columna_dim}'


def test_fk_fact_pesos(engine):
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Pesos', 'ID_Geografia', 'Silver.Dim_Geografia', 'ID_Geografia')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Pesos', 'ID_Tiempo', 'Silver.Dim_Tiempo', 'ID_Tiempo')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Pesos', 'ID_Variedad', 'Silver.Dim_Variedad', 'ID_Variedad')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Pesos', 'ID_Personal', 'Silver.Dim_Personal', 'ID_Personal')


def test_fk_fact_vegetativa(engine):
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Vegetativa', 'ID_Geografia', 'Silver.Dim_Geografia', 'ID_Geografia')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Vegetativa', 'ID_Tiempo', 'Silver.Dim_Tiempo', 'ID_Tiempo')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Vegetativa', 'ID_Variedad', 'Silver.Dim_Variedad', 'ID_Variedad')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Evaluacion_Vegetativa', 'ID_Personal', 'Silver.Dim_Personal', 'ID_Personal')


def test_fk_fact_conteo(engine):
    _assert_sin_huerfanas(engine, 'Silver.Fact_Conteo_Fenologico', 'ID_Geografia', 'Silver.Dim_Geografia', 'ID_Geografia')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Conteo_Fenologico', 'ID_Tiempo', 'Silver.Dim_Tiempo', 'ID_Tiempo')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Conteo_Fenologico', 'ID_Variedad', 'Silver.Dim_Variedad', 'ID_Variedad')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Conteo_Fenologico', 'ID_Personal', 'Silver.Dim_Personal', 'ID_Personal')


def test_fk_fact_ciclo_poda(engine):
    _assert_sin_huerfanas(engine, 'Silver.Fact_Ciclo_Poda', 'ID_Geografia', 'Silver.Dim_Geografia', 'ID_Geografia')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Ciclo_Poda', 'ID_Tiempo', 'Silver.Dim_Tiempo', 'ID_Tiempo')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Ciclo_Poda', 'ID_Variedad', 'Silver.Dim_Variedad', 'ID_Variedad')


def test_fk_fact_maduracion(engine):
    _assert_sin_huerfanas(engine, 'Silver.Fact_Maduracion', 'ID_Geografia', 'Silver.Dim_Geografia', 'ID_Geografia')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Maduracion', 'ID_Tiempo', 'Silver.Dim_Tiempo', 'ID_Tiempo')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Maduracion', 'ID_Variedad', 'Silver.Dim_Variedad', 'ID_Variedad')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Maduracion', 'ID_Personal', 'Silver.Dim_Personal', 'ID_Personal')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Maduracion', 'ID_Cinta', 'Silver.Dim_Cinta', 'ID_Cinta')
    _assert_sin_huerfanas(engine, 'Silver.Fact_Maduracion', 'ID_Estado_Fenologico', 'Silver.Dim_Estado_Fenologico', 'ID_Estado_Fenologico')



def test_fk_fact_clima_tiempo(engine):
    _assert_sin_huerfanas(engine, 'Silver.Fact_Telemetria_Clima', 'ID_Tiempo', 'Silver.Dim_Tiempo', 'ID_Tiempo')

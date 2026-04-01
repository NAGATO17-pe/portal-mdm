from conftest import scalar, skip_si_fact_vacio

def test_fact_pesos_rango_biologico(engine):
    skip_si_fact_vacio(engine, 'Silver.Fact_Evaluacion_Pesos')
    invalidas = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM Silver.Fact_Evaluacion_Pesos
        WHERE Peso_Promedio_Baya_g IS NULL
           OR Peso_Promedio_Baya_g < 0.5
           OR Peso_Promedio_Baya_g > 8.0
        """,
    )
    assert int(invalidas or 0) == 0

def test_fact_vegetativa_floracion_no_supera_total(engine):
    skip_si_fact_vacio(engine, 'Silver.Fact_Evaluacion_Vegetativa')
    invalidas = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM Silver.Fact_Evaluacion_Vegetativa
        WHERE Cantidad_Plantas_en_Floracion > Cantidad_Plantas_Evaluadas
        """,
    )
    assert int(invalidas or 0) == 0


def test_fact_induccion_totales_consistentes(engine):
    skip_si_fact_vacio(engine, 'Silver.Fact_Induccion_Floral')
    invalidas = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM Silver.Fact_Induccion_Floral
        WHERE Cantidad_Plantas_Por_Cama <= 0
           OR Cantidad_Plantas_Con_Induccion > Cantidad_Plantas_Por_Cama
           OR Cantidad_Brotes_Totales <= 0
           OR Cantidad_Brotes_Con_Induccion > Cantidad_Brotes_Totales
           OR Cantidad_Brotes_Con_Flor > Cantidad_Brotes_Totales
        """,
    )
    assert int(invalidas or 0) == 0


def test_fact_tasa_crecimiento_valores_validos(engine):
    skip_si_fact_vacio(engine, 'Silver.Fact_Tasa_Crecimiento_Brotes')
    invalidas = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM Silver.Fact_Tasa_Crecimiento_Brotes
        WHERE Medida_Crecimiento IS NULL
           OR Medida_Crecimiento < 0
           OR Codigo_Ensayo IS NULL
           OR LTRIM(RTRIM(Codigo_Ensayo)) = ''
           OR (Dias_Desde_Poda IS NOT NULL AND Dias_Desde_Poda < 0)
        """,
    )
    assert int(invalidas or 0) == 0


def test_fact_maduracion_organo_valido(engine):
    skip_si_fact_vacio(engine, 'Silver.Fact_Maduracion')
    invalidas = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM Silver.Fact_Maduracion
        WHERE ID_Organo IS NULL OR ID_Organo <= 0
        """,
    )
    assert int(invalidas or 0) == 0

def test_fact_maduracion_dias_no_negativos(engine):
    skip_si_fact_vacio(engine, 'Silver.Fact_Maduracion')
    invalidas = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM Silver.Fact_Maduracion
        WHERE Dias_Pasados_Del_Marcado IS NOT NULL
          AND Dias_Pasados_Del_Marcado < 0
        """,
    )
    assert int(invalidas or 0) == 0

def test_fact_clima_sector_climatico_valido(engine):
    skip_si_fact_vacio(engine, 'Silver.Fact_Telemetria_Clima')
    invalidas = scalar(
        engine,
        """
        SELECT COUNT(*)
        FROM Silver.Fact_Telemetria_Clima
        WHERE Sector_Climatico IS NULL
           OR LTRIM(RTRIM(Sector_Climatico)) = ''
        """,
    )
    assert int(invalidas or 0) == 0

from bronce import cargador


def test_validar_enrutamiento_global_omite_ruta_sin_firma_actual():
    columnas = {
        'Fecha_Raw',
        'DNI_Raw',
        'Modulo_Raw',
        'Turno_Raw',
        'Valvula_Raw',
        'Punto_Raw',
        'Variedad_Raw',
        'BotonesFlorales_Raw',
        'Flores_Raw',
        'BayasPequenas_Raw',
        'BayasGrandes_Raw',
        'Fase1_Raw',
        'Fase2_Raw',
        'BayasCremas_Raw',
        'BayasMaduras_Raw',
        'BayasCosechables_Raw',
        'PlantasProductivas_Raw',
        'PlantasNoProductivas_Raw',
        'Muestras_Raw',
    }

    resultado = cargador._validar_enrutamiento_global(
        'conteo_fruta',
        'Bronce.Conteo_Fruta',
        columnas,
    )

    assert resultado is None


def test_validar_enrutamiento_global_mantiene_bloqueo_para_ruta_con_firma():
    columnas = set(cargador._FIRMAS_RUTA_SUGERIDA['peladas']['columnas_clave'])

    resultado = cargador._validar_enrutamiento_global(
        'induccion_floral',
        'Bronce.Induccion_Floral',
        columnas,
    )

    assert resultado is not None
    assert resultado['codigo'] == 'RUTA_CONTENIDO_INCOMPATIBLE'
    assert resultado['ruta_sugerida'] == 'peladas'

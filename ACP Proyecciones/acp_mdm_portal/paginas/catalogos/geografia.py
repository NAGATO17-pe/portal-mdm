import pandas as pd
import streamlit as st
from utils.componentes import badge_html, estado_vacio_html, seccion_tabla_con_guardar
from utils.formato import header_pagina
from utils.api_client import get_api

def render() -> None:
    from utils.catalogos import render_catalogo

    render_catalogo(
        icono="📍",
        titulo="Geografía y Módulos",
        subtitulo="Estructura jerárquica: Fundo → Lote → Módulo.",
        endpoint="geografia",
        clave_tabla="geografia",
        renombres={
            "fundo": "Fundo",
            "sector": "Sector",
            "modulo": "Módulo",
            "turno": "Turno",
            "valvula": "Válvula",
            "cama": "Cama",
            "es_test_block": "Test Block",
            "codigo_sap_campo": "SAP",
            "es_vigente": "Vigente",
        },
        metricas=[("📍 Total Ubicaciones Mapeadas", lambda df: len(df))],
        columnas_metricas=2,
    )

import pandas as pd
import streamlit as st
from utils.componentes import badge_html, estado_vacio_html, seccion_tabla_con_guardar
from utils.formato import header_pagina
from utils.api_client import get_api

def render() -> None:
    from utils.catalogos import render_catalogo

    render_catalogo(
        icono="🍇",
        titulo="Variedades (MDM)",
        subtitulo="Catálogo maestro de variedades de cultivo.",
        endpoint="variedades",
        clave_tabla="variedades",
        renombres={
            "nombre_canonico": "Nombre Canónico",
            "breeder": "Breeder",
            "es_activa": "Activa",
        },
        metricas=[
            ("🍇 Total", lambda df: len(df)),
            ("🟢 Activas", lambda df: int(df["Activa"].sum()) if "Activa" in df.columns else len(df)),
        ],
    )

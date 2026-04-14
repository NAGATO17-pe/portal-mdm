import pandas as pd
import streamlit as st
from utils.componentes import badge_html, estado_vacio_html, seccion_tabla_con_guardar
from utils.formato import header_pagina
from utils.api_client import get_api

def render() -> None:
    from utils.catalogos import render_catalogo

    render_catalogo(
        icono="👤",
        titulo="Personal Agricola",
        subtitulo="Nómina y supervisores.",
        endpoint="personal",
        clave_tabla="personal",
        renombres={
            "dni": "DNI",
            "nombre_completo": "Nombre Completo",
            "rol": "Rol",
            "sexo": "Sexo",
            "id_planilla": "ID Planilla",
            "pct_asertividad": "% Asertividad",
            "dias_ausentismo": "Días Ausentismo",
        },
        metricas=[("👤 Total Evaluadores/Personal", lambda df: len(df))],
        columnas_metricas=2,
    )

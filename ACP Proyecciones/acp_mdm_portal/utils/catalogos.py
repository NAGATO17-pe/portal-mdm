from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import streamlit as st

from utils.api_client import get_api
from utils.componentes import seccion_tabla_con_guardar
from utils.formato import header_pagina


@st.cache_data(ttl=60, show_spinner=False)
def cargar_catalogo(endpoint: str) -> pd.DataFrame:
    resultado = get_api(f"/catalogos/{endpoint}?pagina=1&tamano=1000")
    if resultado.ok and isinstance(resultado.data, dict):
        datos = resultado.data.get("datos", [])
        if datos:
            return pd.DataFrame(datos)
    return pd.DataFrame()


def render_catalogo(
    *,
    icono: str,
    titulo: str,
    subtitulo: str,
    endpoint: str,
    clave_tabla: str,
    renombres: dict[str, str],
    metricas: list[tuple[str, Callable[[pd.DataFrame], object]]],
    columnas_metricas: int | None = None,
) -> None:
    header_pagina(icono, titulo, subtitulo)
    df = cargar_catalogo(endpoint)

    if df.empty:
        st.warning("No se pudo cargar el catálogo o está vacío. Revisa la conectividad al backend.")
        return

    df = df.rename(columns=renombres)

    total_columnas = columnas_metricas or len(metricas)
    columnas = st.columns(total_columnas)
    for columna, (label, fn_valor) in zip(columnas, metricas):
        columna.metric(label, fn_valor(df))

    st.markdown("<hr style='margin:16px 0;'>", unsafe_allow_html=True)
    seccion_tabla_con_guardar(
        df,
        key=clave_tabla,
        titulo="📥 Catálogo Consolidado",
        page_size=20,
        mostrar_boton_guardar=False,
    )

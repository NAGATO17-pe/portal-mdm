import pandas as pd
import streamlit as st
from utils.componentes import badge_html, estado_vacio_html, seccion_tabla_con_guardar
from utils.formato import header_pagina
from utils.api_client import get_api

@st.cache_data(ttl=60, show_spinner=False)
def cargar_variedades() -> pd.DataFrame:
    res = get_api("/catalogos/variedades?pagina=1&tamano=1000")
    if res and res.status_code == 200:
        datos = res.json().get("datos", [])
        if datos:
            return pd.DataFrame(datos)
    return pd.DataFrame()

def render() -> None:
    header_pagina("🍇", "Variedades (MDM)", "Catálogo maestro de variedades de cultivo.")
    df = cargar_variedades()
    
    if df.empty:
        st.warning("No se pudo cargar el catálogo o está vacío. Revisa la conectividad al backend.")
        return

    # Mapeo reverso para nombres limpios
    df = df.rename(columns={
        "nombre_canonico": "Nombre Canónico", 
        "breeder": "Breeder", 
        "es_activa": "Activa"
    })

    total = len(df)
    activas = int(df["Activa"].sum()) if "Activa" in df.columns else total

    c1, c2 = st.columns(2)
    c1.metric("🍇 Total", total)
    c2.metric("🟢 Activas", activas)

    st.markdown("<hr style='margin:16px 0;'>", unsafe_allow_html=True)
    seccion_tabla_con_guardar(df, key="variedades", titulo="📥 Catálogo Consolidado", page_size=20, mostrar_boton_guardar=False)

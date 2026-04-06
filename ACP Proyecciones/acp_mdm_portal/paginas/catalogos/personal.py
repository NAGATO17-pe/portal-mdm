import pandas as pd
import streamlit as st
from utils.componentes import badge_html, estado_vacio_html, seccion_tabla_con_guardar
from utils.formato import header_pagina
from utils.api_client import get_api

@st.cache_data(ttl=60, show_spinner=False)
def cargar_personal() -> pd.DataFrame:
    res = get_api("/catalogos/personal?pagina=1&tamano=1000")
    if res and res.status_code == 200:
        datos = res.json().get("datos", [])
        if datos:
            return pd.DataFrame(datos)
    return pd.DataFrame()

def render() -> None:
    header_pagina("👤", "Personal Agricola", "Nómina y supervisores.")
    df = cargar_personal()
    
    if df.empty:
        st.warning("No se pudo cargar el catálogo o está vacío. Revisa la conectividad al backend.")
        return

    # Renombramos
    df = df.rename(columns={
        "dni": "DNI", "nombre_completo": "Nombre Completo", 
        "rol": "Rol", "sexo": "Sexo", "id_planilla": "ID Planilla",
        "pct_asertividad": "% Asertividad", "dias_ausentismo": "Días Ausentismo"
    })

    total = len(df)
    c1, c2 = st.columns(2)
    c1.metric("👤 Total Evaluadores/Personal", total)

    st.markdown("<hr style='margin:16px 0;'>", unsafe_allow_html=True)
    seccion_tabla_con_guardar(df, key="personal", titulo="📥 Catálogo Consolidado", page_size=20, mostrar_boton_guardar=False)

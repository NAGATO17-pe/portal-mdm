import pandas as pd
import streamlit as st
from utils.componentes import badge_html, estado_vacio_html, seccion_tabla_con_guardar
from utils.formato import header_pagina
from utils.api_client import get_api

@st.cache_data(ttl=60, show_spinner=False)
def cargar_geografia() -> pd.DataFrame:
    res = get_api("/catalogos/geografia?pagina=1&tamano=1000")
    if res and res.status_code == 200:
        datos = res.json().get("datos", [])
        if datos:
            return pd.DataFrame(datos)
    return pd.DataFrame()

def render() -> None:
    header_pagina("📍", "Geografía y Módulos", "Estructura jerárquica: Fundo → Lote → Módulo.")
    df = cargar_geografia()
    
    if df.empty:
        st.warning("No se pudo cargar el catálogo o está vacío. Revisa la conectividad al backend.")
        return

    # Mapeo a títulos bonitos
    df = df.rename(columns={
        "fundo": "Fundo", "sector": "Sector", "modulo": "Módulo", 
        "turno": "Turno", "valvula": "Válvula", "cama": "Cama",
        "es_test_block": "Test Block", "codigo_sap_campo": "SAP", "es_vigente": "Vigente"
    })

    total = len(df)
    c1, c2 = st.columns(2)
    c1.metric("📍 Total Ubicaciones Mapeadas", total)

    st.markdown("<hr style='margin:16px 0;'>", unsafe_allow_html=True)
    seccion_tabla_con_guardar(df, key="geografia", titulo="📥 Catálogo Consolidado", page_size=20, mostrar_boton_guardar=False)

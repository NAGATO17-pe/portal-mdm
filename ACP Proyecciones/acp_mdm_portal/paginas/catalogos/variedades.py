import streamlit as st
import pandas as pd
from utils.formato import header_pagina, crear_paginacion_ui, renderizar_tabla_premium
from utils.db import ejecutar_query, verificar_conexion

@st.cache_data(ttl=60, show_spinner=False)
def cargar_variedades_db() -> pd.DataFrame:
    """Obtiene el catalogo de variedades desde MDM."""
    return ejecutar_query("""
        SELECT 
            Nombre_Canonico AS [Nombre canónico], 
            Breeder, 
            Es_Activa        AS [Activa]
        FROM MDM.Catalogo_Variedades
        ORDER BY Nombre_Canonico
    """)

def render():
    conectado = verificar_conexion()
    header_pagina("🍇", "Catálogos · Variedades", "Gestión del catálogo oficial de variedades")

    if conectado:
        df = cargar_variedades_db()
    else:
        df = pd.DataFrame(columns=["Nombre canónico", "Breeder", "Activa"])

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Total variedades", len(df))
    c2.metric("Activas", int(df["Activa"].sum()) if not df.empty else 0)
    c3.metric("Inactivas", int((~df["Activa"].astype(bool)).sum()) if not df.empty else 0)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar variedad ─────────────────────────────────────────────────
    with st.expander("➕ Agregar variedad nueva", expanded=False):
        a1, a2, a3 = st.columns([2, 2, 1])
        with a1:
            st.text_input("Nombre canónico", key="var_nuevo_nombre")
        with a2:
            st.text_input("Breeder", key="var_nuevo_breeder")
        with a3:
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("✅ Agregar", key="btn_var_agregar", type="primary", disabled=True)
        st.caption("Conexión a BD no disponible.")

    st.markdown("---")

    # ── Tabla premium ───────────────────────────────────────────────────
    st.markdown("### 📋 Variedades registradas")
    if df.empty:
        st.info("No hay variedades registradas en el catálogo.")
    else:
        st.caption("Visualización del catálogo con paginación profesional.")
        renderizar_tabla_premium(df, key="variedades_cat", page_size=15,
                                  columnas_check=["Activa"])

        if st.button("💾 Guardar cambios", key="btn_var_guardar", type="primary"):
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

import streamlit as st
import pandas as pd
from utils.formato import header_pagina

# DataFrame vacío como placeholder para geografía
GEOGRAFIA = pd.DataFrame(columns=["Fundo", "Sector", "Módulo", "Turno", "Es Test Block", "Activa"])

def render():
    header_pagina("📚", "Catálogos · Geografía", "Fundos, sectores y módulos · cambios activan SCD2")

    st.markdown("""
        <div class="banner-aviso">
            ⚠️ <b>Atención:</b> Los cambios en geografía activan
            <b>SCD Tipo 2</b> en la próxima ejecución del ETL.
        </div>
    """, unsafe_allow_html=True)

    df = GEOGRAFIA.copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Módulos totales", len(df))
    c2.metric("Activos", df["Activa"].sum() if not df.empty else 0)
    c3.metric("Test Blocks", df["Es Test Block"].sum() if not df.empty else 0)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar ──────────────────────────────────────────────────────────
    with st.expander("➕ Agregar módulo nuevo", expanded=False):
        g1, g2, g3, g4 = st.columns(4)
        with g1:
            st.text_input("Fundo", key="geo_fundo")
        with g2:
            st.text_input("Sector", key="geo_sector")
        with g3:
            st.text_input("Módulo", key="geo_modulo")
        with g4:
            st.selectbox("Turno", ["Mañana", "Tarde"], key="geo_turno")
        if st.button("✅ Agregar", key="btn_geo_agregar", type="primary", disabled=True):
            pass
        st.caption("Conexión a BD no disponible.")

    st.markdown("---")

    st.markdown("### 📋 Módulos registrados")
    if df.empty:
        st.info("No hay información geográfica registrada.")
    else:
        st.caption("Edita **Es Test Block** y **Activa** directamente en la tabla.")
        edited = st.data_editor(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Es Test Block": st.column_config.CheckboxColumn("Es Test Block"),
                "Activa": st.column_config.CheckboxColumn("Activa"),
            },
            disabled=["Fundo", "Sector", "Módulo", "Turno"],
        )

        if st.button("💾 Guardar cambios", key="btn_geo_guardar", type="primary"):
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

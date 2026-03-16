"""
variedades.py — Catálogo de variedades oficiales.
"""
import streamlit as st
from utils.formato import header_pagina
from utils.datos_mock import VARIEDADES


def render():
    header_pagina("📚", "Catálogos · Variedades", "Gestión del catálogo oficial de variedades")

    df = VARIEDADES.copy()

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Total variedades", len(df))
    c2.metric("Activas", df["Activa"].sum())
    c3.metric("Inactivas", (~df["Activa"]).sum())

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar variedad ─────────────────────────────────────────────────
    with st.expander("➕ Agregar variedad nueva", expanded=False):
        a1, a2, a3 = st.columns([2, 2, 1])
        with a1:
            nuevo_nombre = st.text_input("Nombre canónico", key="var_nuevo_nombre")
        with a2:
            nuevo_breeder = st.text_input("Breeder", key="var_nuevo_breeder")
        with a3:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("✅ Agregar", key="btn_var_agregar", type="primary"):
                if nuevo_nombre and nuevo_breeder:
                    st.success(f"✅ Variedad **{nuevo_nombre}** ({nuevo_breeder}) agregada (demo).")
                else:
                    st.warning("Completa nombre y breeder.")

    st.markdown("---")

    # ── Tabla editable ───────────────────────────────────────────────────
    st.markdown("### 📋 Variedades registradas")
    st.caption("Puedes editar el toggle **Activa** directamente en la tabla.")

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Activa": st.column_config.CheckboxColumn("Activa", help="Desactivar = Es_Activa = 0"),
        },
        disabled=["Nombre canónico", "Breeder"],
    )

    if st.button("💾 Guardar cambios", key="btn_var_guardar", type="primary"):
        st.success("✅ Cambios guardados en el catálogo (demo).")

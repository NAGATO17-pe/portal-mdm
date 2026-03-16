"""
personal.py — Catálogo de personal con edición de nombre, rol y estado.
"""
import streamlit as st
from utils.formato import header_pagina
from utils.datos_mock import PERSONAL


def render():
    header_pagina("📚", "Catálogos · Personal", "Revisión y corrección del catálogo de personal")

    df = PERSONAL.copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Personal registrado", len(df))
    c2.metric("Activos", df["Activo"].sum())
    c3.metric("Inactivos", (~df["Activo"]).sum())

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 📋 Personal registrado")
    st.caption("Edita **Nombre completo**, **Rol** y **Activo** directamente en la tabla.")

    ROLES = ["Operario", "Evaluador", "Supervisor"]

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Rol": st.column_config.SelectboxColumn("Rol", options=ROLES),
            "Activo": st.column_config.CheckboxColumn("Activo"),
        },
        disabled=["DNI", "Sexo"],
    )

    if st.button("💾 Guardar cambios", key="btn_per_guardar", type="primary"):
        st.success("✅ Cambios en personal guardados (demo).")

import streamlit as st
import pandas as pd
from utils.formato import header_pagina

# DataFrame vacío como placeholder para personal
PERSONAL = pd.DataFrame(columns=["DNI", "Nombre completo", "Rol", "Sexo", "Activo"])

def render():
    header_pagina("📚", "Catálogos · Personal", "Revisión y corrección del catálogo de personal")

    df = PERSONAL.copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Personal registrado", len(df))
    c2.metric("Activos", df["Activo"].sum() if not df.empty else 0)
    c3.metric("Inactivos", (~df["Activo"]).sum() if not df.empty else 0)

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 📋 Personal registrado")
    if df.empty:
        st.info("No hay personal registrado en el catálogo.")
    else:
        st.caption("Edita **Nombre completo**, **Rol** y **Activo** directamente en la tabla.")
        ROLES = ["Operario", "Evaluador", "Supervisor"]
        edited = st.data_editor(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Rol": st.column_config.SelectboxColumn("Rol", options=ROLES),
                "Activo": st.column_config.CheckboxColumn("Activo"),
            },
            disabled=["DNI", "Sexo"],
        )

        if st.button("💾 Guardar cambios", key="btn_per_guardar", type="primary"):
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

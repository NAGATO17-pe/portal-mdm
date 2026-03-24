import streamlit as st
import pandas as pd
from utils.formato import header_pagina
from utils.db import ejecutar_query, verificar_conexion

# DataFrame vacío como placeholder para personal
# PERSONAL = pd.DataFrame(columns=["DNI", "Nombre completo", "Rol", "Sexo", "Activo"])

@st.cache_data(ttl=60, show_spinner=False)
def cargar_personal_db() -> pd.DataFrame:
    """Obtiene la dimension personal desde Silver."""
    return ejecutar_query("""
        SELECT 
            DNI, 
            Nombre, 
            Cargo, 
            Actividad, 
            Sede, 
            Activo
        FROM Silver.Dim_Personal
        ORDER BY Nombre
    """)

def render():
    conectado = verificar_conexion()
    header_pagina("🍇", "Catálogos · Variedades", "Gestión del catálogo oficial de variedades")

    if conectado:
        df = cargar_personal_db()
    else:
        df = pd.DataFrame(columns=["DNI", "Nombre", "Cargo", "Actividad", "Sede", "Activo"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Personal total", len(df))
    c2.metric("En campo", len(df[df["Actividad"] != "Administrativo"]) if not df.empty else 0)
    c3.metric("Activos", int(df["Activo"].sum()) if not df.empty else 0)
    # The instruction had a duplicate c3.metric for Inactivos, keeping the original structure for now
    # c3.metric("Inactivos", (~df["Activo"]).sum() if not df.empty else 0)


    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 📋 Personal registrado")
    if df.empty:
        st.info("No hay personal registrado en el catálogo.")
    else:
        st.caption("Edita **Nombre**, **Cargo** y **Activo** directamente en la tabla.")
        ROLES = ["Operario", "Evaluador", "Supervisor", "Administrativo"] # Updated roles based on new data
        edited = st.data_editor(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Cargo": st.column_config.SelectboxColumn("Cargo", options=ROLES), # Changed "Rol" to "Cargo"
                "Activo": st.column_config.CheckboxColumn("Activo"),
            },
            disabled=["DNI", "Actividad", "Sede"], # Updated disabled columns based on new data
        )

        if st.button("💾 Guardar cambios", key="btn_per_guardar", type="primary"):
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

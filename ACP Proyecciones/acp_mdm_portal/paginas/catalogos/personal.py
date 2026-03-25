import streamlit as st
import pandas as pd
from utils.formato import header_pagina, crear_paginacion_ui, renderizar_tabla_premium
from utils.db import ejecutar_query, verificar_conexion

# ... (cargar_personal_db remains same)
@st.cache_data(ttl=60, show_spinner=False)
def cargar_personal_db() -> pd.DataFrame:
    """Obtiene la dimension personal desde Silver."""
    return ejecutar_query("""
        SELECT 
            DNI, 
            Nombre_Completo AS [Nombre], 
            Rol, 
            Sexo, 
            ID_Planilla     AS [Sede], 
            Pct_Asertividad AS [Productividad],
            Dias_Ausentismo AS [Faltas]
        FROM Silver.Dim_Personal
        ORDER BY Nombre_Completo
    """)

def render():
    conectado = verificar_conexion()
    header_pagina("👤", "Catálogos · Personal", "Gestión de trabajadores y roles")

    if conectado:
        df = cargar_personal_db()
    else:
        df = pd.DataFrame(columns=["DNI", "Nombre", "Cargo", "Actividad", "Sede", "Activo"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Personal total", len(df))
    campus = len(df[df["Rol"] != "Administrativo"]) if not df.empty else 0
    c2.metric("En campo", campus)
    prod_avg = df["Productividad"].mean() if not df.empty else 0
    c3.metric("Productividad avg", f"{prod_avg:.1f}%")

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 📋 Personal registrado")
    if df.empty:
        st.info("No hay personal registrado en el catálogo.")
    else:
        st.caption("Visualización del personal con paginación profesional.")
        renderizar_tabla_premium(df, key="personal_cat", page_size=15)

        if st.button("💾 Guardar cambios", key="btn_per_guardar", type="primary"):
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

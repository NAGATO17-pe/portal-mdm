from utils.db import ejecutar_query, verificar_conexion

@st.cache_data(ttl=60, show_spinner=False)
def cargar_variedades_db() -> pd.DataFrame:
    """Obtiene el catalogo de variedades desde MDM."""
    return ejecutar_query("""
        SELECT 
            Nombre_Canonico AS [Nombre canónico], 
            Breeder, 
            Es_Activo       AS [Activa]
        FROM MDM.Catalogo_Variedades
        ORDER BY Nombre_Canonico
    """)

def render():
    conectado = verificar_conexion()
    header_pagina("VARIEDAD", "Catálogos · Variedades", "Gestión del catálogo oficial de variedades")

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

    # ── Tabla editable ───────────────────────────────────────────────────
    st.markdown("### 📋 Variedades registradas")
    if df.empty:
        st.info("No hay variedades registradas en el catálogo.")
    else:
        st.caption("Puedes editar el toggle **Activa** directamente en la tabla.")
        edited = st.data_editor(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Activa": st.column_config.CheckboxColumn("Activa", help="Desactivar = Es_Activa = 0"),
            },
            disabled=["Nombre canónico", "Breeder"],
        )

        if st.button("💾 Guardar cambios", key="btn_var_guardar", type="primary"):
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

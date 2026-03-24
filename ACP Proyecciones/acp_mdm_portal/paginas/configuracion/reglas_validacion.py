from utils.db import ejecutar_query, verificar_conexion

@st.cache_data(ttl=60, show_spinner=False)
def cargar_reglas_db() -> pd.DataFrame:
    """Obtiene las reglas de validacion desde Config."""
    return ejecutar_query("""
        SELECT 
            Tabla_Destino   AS [Tabla destino], 
            Columna, 
            Tipo_Validacion AS [Tipo validación], 
            Valor_Min        AS [Valor min], 
            Valor_Max        AS [Valor max], 
            Accion          AS [Acción], 
            Activo
        FROM Config.Reglas_Validacion
        ORDER BY Tabla_Destino, Columna
    """)

def render():
    conectado = verificar_conexion()
    header_pagina(
        "LIST", "Configuración · Reglas de Validación",
        "Ajusta rangos y reglas de calidad de datos sin tocar código"
    )

    st.markdown("""
        <div class="banner-aviso">
            ⚠️ <b>Atención:</b> Los cambios en reglas aplican en la <b>próxima ejecución del ETL</b>.
        </div>
    """, unsafe_allow_html=True)

    if conectado:
        df = cargar_reglas_db()
    else:
        df = pd.DataFrame(columns=["Tabla destino", "Columna", "Tipo validación", "Valor min", "Valor max", "Acción", "Activo"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Total reglas", len(df))
    c2.metric("Activas", int(df["Activo"].sum()) if not df.empty else 0)
    c3.metric("Inactivas", int((~df["Activo"].astype(bool)).sum()) if not df.empty else 0)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar regla ─────────────────────────────────────────────────────
    with st.expander("➕ Agregar nueva regla", expanded=False):
        r1, r2, r3, r4, r5 = st.columns([2, 2, 1, 1, 1.5])
        with r1:
            st.text_input("Tabla destino", key="reg_tabla")
        with r2:
            st.text_input("Columna", key="reg_col")
        with r3:
            st.number_input("Valor min", key="reg_min", value=0.0)
        with r4:
            st.number_input("Valor max", key="reg_max", value=100.0)
        with r5:
            st.markdown("<br>", unsafe_allow_html=True)
            st.button("✅ Agregar", key="btn_reg_agregar", type="primary", disabled=True)
        st.caption("Conexión a BD no disponible.")

    st.markdown("---")

    st.markdown("### 📋 Reglas de validación")
    if df.empty:
        st.info("No hay reglas de validación configuradas.")
    else:
        st.caption("Edita **Valor min**, **Valor max** y **Activa** directamente en la tabla.")
        edited = st.data_editor(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Activa": st.column_config.CheckboxColumn("Activa"),
                "Valor min": st.column_config.NumberColumn("Valor min", format="%.2f"),
                "Valor max": st.column_config.NumberColumn("Valor max", format="%.2f"),
            },
            disabled=["Tabla destino", "Columna", "Tipo validación", "Acción"],
        )

        if st.button("💾 Guardar cambios", key="btn_reg_guardar", type="primary"):
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

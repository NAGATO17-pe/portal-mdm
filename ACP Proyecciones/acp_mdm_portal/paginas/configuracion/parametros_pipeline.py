from utils.db import ejecutar_query, verificar_conexion

@st.cache_data(ttl=60, show_spinner=False)
def cargar_parametros_db() -> pd.DataFrame:
    """Obtiene los parametros operativos del pipeline."""
    return ejecutar_query("""
        SELECT 
            Nombre_Parametro AS [Parámetro], 
            Valor            AS [Valor actual], 
            Descripcion      AS [Descripción], 
            CONVERT(varchar, Fecha_Modificacion, 120) AS [Última modificación]
        FROM Config.Parametros_Pipeline
        ORDER BY Nombre_Parametro
    """)

def render():
    conectado = verificar_conexion()
    header_pagina(
        "⚙️", "Configuración · Parámetros Pipeline",
        "Parámetros operativos del ETL · edita con confirmación"
    )

    st.markdown("""
        <div class="banner-aviso">
            ⚠️ <b>Atención:</b> Los cambios en parámetros aplican en la <b>próxima ejecución del ETL</b>.
        </div>
    """, unsafe_allow_html=True)

    if conectado:
        df = cargar_parametros_db()
    else:
        df = pd.DataFrame(columns=["Parámetro", "Valor actual", "Descripción", "Última modificación"])

    st.markdown("### Parámetros activos")

    if df.empty:
        st.info("No hay parámetros configurados.")
    else:
        for _, row in df.iterrows():
            with st.container():
                p1, p2, p3 = st.columns([2.5, 2, 4])
                with p1:
                    st.markdown(f"**`{row['Parámetro']}`**")
                    st.caption(f"Última mod.: {row['Última modificación']}")
                with p2:
                    st.text_input(
                        "Valor",
                        value=str(row["Valor actual"]),
                        key=f"param_{row['Parámetro']}",
                        label_visibility="collapsed",
                    )
                with p3:
                    st.markdown(f"<span style='color:#666; font-size:0.85rem;'>{row['Descripción']}</span>",
                                unsafe_allow_html=True)

            st.markdown("---")

        if st.button("💾 Guardar todos los cambios", key="btn_param_guardar", type="primary"):
            st.toast("Guardado simulado: Funcionalidad de escritura pendiente.", icon="💾")

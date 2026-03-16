import streamlit as st
import pandas as pd
from utils.formato import header_pagina

# DataFrame vacío como placeholder para parámetros
PARAMETROS_PIPELINE = pd.DataFrame(columns=["Parámetro", "Valor actual", "Descripción", "Última modificación"])

def render():
    header_pagina(
        "⚙️", "Configuración · Parámetros Pipeline",
        "Parámetros operativos del ETL · edita con confirmación"
    )

    st.markdown("""
        <div class="banner-aviso">
            ⚠️ <b>Atención:</b> Los cambios en parámetros aplican en la <b>próxima ejecución del ETL</b>.
        </div>
    """, unsafe_allow_html=True)

    df = PARAMETROS_PIPELINE.copy()

    st.markdown("### ⚙️ Parámetros activos")

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
            st.toast("Guardado simulado: DB desconectada.", icon="💾")

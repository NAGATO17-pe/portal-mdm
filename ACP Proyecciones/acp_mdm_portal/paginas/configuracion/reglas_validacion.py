"""
reglas_validacion.py — Configuración de reglas de validación de DQ.
"""
import streamlit as st
from utils.formato import header_pagina
from utils.datos_mock import REGLAS_VALIDACION


def render():
    header_pagina(
        "⚙️", "Configuración · Reglas de Validación",
        "Ajusta rangos y reglas de calidad de datos sin tocar código"
    )

    st.markdown("""
        <div class="banner-aviso">
            ⚠️ <b>Atención:</b> Los cambios en reglas aplican en la <b>próxima ejecución del ETL</b>.
            Modifica con cuidado los rangos biológicos.
        </div>
    """, unsafe_allow_html=True)

    df = REGLAS_VALIDACION.copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total reglas", len(df))
    c2.metric("Activas", df["Activa"].sum())
    c3.metric("Inactivas", (~df["Activa"]).sum())

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar regla ─────────────────────────────────────────────────────
    with st.expander("➕ Agregar nueva regla", expanded=False):
        r1, r2, r3, r4, r5 = st.columns([2, 2, 1, 1, 1.5])
        with r1:
            rt = st.text_input("Tabla destino", key="reg_tabla")
        with r2:
            rc = st.text_input("Columna", key="reg_col")
        with r3:
            rmin = st.number_input("Valor min", key="reg_min", value=0.0)
        with r4:
            rmax = st.number_input("Valor max", key="reg_max", value=100.0)
        with r5:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("✅ Agregar", key="btn_reg_agregar", type="primary"):
                if rt and rc:
                    st.success(f"✅ Regla para **{rc}** ({rt}) agregada (demo).")
                else:
                    st.warning("Completa tabla y columna.")

    st.markdown("---")

    st.markdown("### 📋 Reglas de validación")
    st.caption("Edita **Valor min**, **Valor max** y **Activa** directamente en la tabla.")

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Activa": st.column_config.CheckboxColumn("Activa"),
            "Valor min": st.column_config.NumberColumn("Valor min", format="%.2f"),
            "Valor max": st.column_config.NumberColumn("Valor max", format="%.2f"),
        },
        disabled=["Tabla destino", "Columna", "Tipo validación", "Acción"],
    )

    if st.button("💾 Guardar cambios", key="btn_reg_guardar", type="primary"):
        st.success("✅ Reglas actualizadas. Aplicarán en la próxima ejecución del ETL (demo).")

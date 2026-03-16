"""
inicio.py — Página de inicio del portal MDM.
Dashboard con métricas del día, estado por tabla, alertas y log de cargas.
"""
import streamlit as st
from utils.formato import header_pagina, colorear_estado
from utils.datos_mock import ESTADO_TABLAS, LOG_CARGAS


def render():
    header_pagina("🏠", "Inicio", "Estado del pipeline · hoy 16/03/2026 — carga de las 06:15")

    # ── Métricas principales ──────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⏱️ Última carga", "Hace 1h 45m", delta=None)
    c2.metric("✅ Filas OK", "13,260", delta="+843 vs ayer", delta_color="normal")
    c3.metric("🔴 En cuarentena", "566", delta="+145 vs ayer", delta_color="inverse")
    c4.metric("🔗 Homologaciones pendientes", "8", delta="-5 vs ayer", delta_color="inverse")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Alertas activas ───────────────────────────────────────────────────
    hay_criticos = True
    hay_sin_homologar = True

    if hay_criticos:
        st.error(
            "🚨 **Alerta crítica:** Hay 312 filas con peso de baya fuera de rango en "
            "`Bronce.Evaluacion_Pesos`. Revisar en **Cuarentena**.",
            icon=None,
        )
    if hay_sin_homologar:
        st.warning(
            "⚠️ **8 variedades** sin homologar detectadas por el ETL. "
            "Ir a **Homologación** para aprobar o corregir.",
            icon=None,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Estado por tabla ──────────────────────────────────────────────────
    st.markdown("### 📊 Estado de la última carga por tabla")
    st.dataframe(
        ESTADO_TABLAS.style
            .applymap(colorear_estado, subset=["Estado"])
            .format({"Filas insertadas": "{:,}", "Filas rechazadas": "{:,}"}),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Log últimas 10 cargas ─────────────────────────────────────────────
    with st.expander("📋 Log de últimas 10 cargas (solo lectura)", expanded=False):
        st.dataframe(
            LOG_CARGAS.style
                .applymap(colorear_estado, subset=["Resultado"])
                .format({"Total filas": "{:,}", "Rechazadas": "{:,}"}),
            use_container_width=True,
            hide_index=True,
        )

"""
homologacion.py — Página de homologación.
Aprobación de sugerencias automáticas de Levenshtein e historial.
"""
import streamlit as st
from utils.formato import header_pagina, score_a_color
from utils.datos_mock import HOMOLOGACION_PENDIENTE, HOMOLOGACION_HISTORIAL


def render():
    header_pagina(
        "🔗", "Homologación",
        "Sugerencias automáticas pendientes de revisión · aprueba, corrige o rechaza"
    )

    tab1, tab2 = st.tabs(["📬 Pendientes de aprobación", "📚 Historial aprobado"])

    # ── Tab 1: Pendientes ─────────────────────────────────────────────────
    with tab1:
        df = HOMOLOGACION_PENDIENTE.copy()

        st.markdown(f"**{len(df)} sugerencias** generadas por el ETL esperan revisión.")
        st.markdown("<br>", unsafe_allow_html=True)

        for idx, row in df.iterrows():
            icono = score_a_color(row["Score"])
            with st.container():
                st.markdown(f"""
                    <div style="background:white; border:1px solid #D5E0D8; border-left:4px solid {'#1E6B35' if row['Score']>=0.85 else '#E67E22'};
                         border-radius:8px; padding:12px 18px; margin-bottom:10px;">
                        <span style="font-size:0.8rem; color:#888;">#{idx+1} · {row['Tabla origen']} · visto {row['Veces visto']}x · {row['Fecha']}</span><br>
                        <b style="font-size:1rem;">{icono} &nbsp;"{row['Texto crudo']}"</b>
                        &nbsp;→&nbsp;
                        <b style="color:#1E6B35; font-size:1rem;">"{row['Valor canónico sugerido']}"</b>
                        &nbsp;&nbsp;
                        <span style="background:#EAF4EC; color:#1E6B35; padding:2px 10px; border-radius:12px; font-size:0.8rem; font-weight:600;">
                            Score {row['Score']:.2f}
                        </span>
                    </div>
                """, unsafe_allow_html=True)

                acc1, acc2, acc3, acc4 = st.columns([1, 2, 1, 3])
                with acc1:
                    if st.button("✅ Aprobar", key=f"apr_{idx}", type="primary"):
                        st.success(f"Aprobado: **{row['Texto crudo']}** → **{row['Valor canónico sugerido']}** (demo)")
                with acc2:
                    nuevo = st.text_input(
                        "Corregir valor canónico",
                        value=row["Valor canónico sugerido"],
                        key=f"corr_{idx}",
                        label_visibility="collapsed",
                    )
                    if st.button("✏️ Aprobar con corrección", key=f"apc_{idx}"):
                        st.success(f"Aprobado con corrección: **{row['Texto crudo']}** → **{nuevo}** (demo)")
                with acc3:
                    if st.button("❌ Rechazar", key=f"rej_{idx}"):
                        st.error(f"Rechazado → enviado a cuarentena (demo)")
                st.markdown("---")

    # ── Tab 2: Historial ──────────────────────────────────────────────────
    with tab2:
        st.markdown("### Historial de homologaciones aprobadas")

        h1, h2, h3 = st.columns(3)
        with h1:
            filtro_tabla = st.selectbox(
                "Tabla",
                ["Todas"] + list(HOMOLOGACION_HISTORIAL["Tabla"].unique()),
                key="hist_tabla"
            )
        with h2:
            filtro_usuario = st.selectbox(
                "Aprobado por",
                ["Todos"] + list(HOMOLOGACION_HISTORIAL["Aprobado por"].unique()),
                key="hist_usuario"
            )
        with h3:
            filtro_fecha = st.date_input("Desde fecha", key="hist_fecha", value=None)

        df_hist = HOMOLOGACION_HISTORIAL.copy()
        if filtro_tabla != "Todas":
            df_hist = df_hist[df_hist["Tabla"] == filtro_tabla]
        if filtro_usuario != "Todos":
            df_hist = df_hist[df_hist["Aprobado por"] == filtro_usuario]

        st.dataframe(df_hist, use_container_width=True, hide_index=True)

"""
paginas/homologacion.py — Página de Homologación del portal MDM ACP
=====================================================================
Muestra sugerencias automáticas de homologación pendientes y el historial
de decisiones tomadas. Sin dependencia de streamlit_lottie.
"""

import pandas as pd
import streamlit as st

from utils.componentes import estado_vacio_html, seccion_tabla_con_guardar
from utils.db import ejecutar_query, verificar_conexion
from utils.formato import crear_paginacion_ui, header_pagina


# ── Queries cacheadas ─────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def cargar_sugerencias_pendientes() -> pd.DataFrame:
    """Registros de homologación pendientes en la cuarentena MDM."""
    return ejecutar_query("""
        SELECT
            Tabla_Origen       AS [Tabla],
            Campo_Origen       AS [Campo],
            Valor_Recibido     AS [Texto crudo],
            Valor_Corregido    AS [Valor canónico sugerido],
            Score_Levenshtein  AS [Score],
            CONVERT(varchar, Fecha_Ingreso, 23) AS [Fecha]
        FROM MDM.Cuarentena
        WHERE Estado = 'PENDIENTE' AND Tipo_Regla = 'MDM'
        ORDER BY Score_Levenshtein DESC, Fecha_Ingreso DESC
    """)


@st.cache_data(ttl=60, show_spinner=False)
def cargar_historial_homologacion() -> pd.DataFrame:
    """Todas las decisiones tomadas en el MDM (estado RESUELTO)."""
    return ejecutar_query("""
        SELECT
            Tabla_Origen       AS [Tabla],
            Campo_Origen       AS [Campo],
            Valor_Recibido     AS [Texto crudo],
            Valor_Corregido    AS [Valor canónico],
            Score_Levenshtein  AS [Score],
            Aprobado_Por       AS [Aprobado por],
            CONVERT(varchar, Fecha_Resolucion, 23) AS [Fecha aprobación]
        FROM MDM.Cuarentena
        WHERE Estado = 'RESUELTO'
        ORDER BY Fecha_Resolucion DESC
    """)


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    conectado = verificar_conexion()
    header_pagina(
        "🔗", "Homologación",
        "Sugerencias automáticas pendientes · aprueba, corrige o rechaza",
    )

    tab1, tab2 = st.tabs(["📬 Pendientes de aprobación", "📚 Historial aprobado"])

    # ── Tab 1: Pendientes ─────────────────────────────────────────────────────
    with tab1:
        if not conectado:
            st.warning("Sin conexión a la base de datos.")
            return

        df = cargar_sugerencias_pendientes()

        if df.empty:
            st.markdown("<br>", unsafe_allow_html=True)
            c1, c2, c3 = st.columns([1, 2, 1])
            with c2:
                estado_vacio_html(
                    icono="🎉",
                    titulo="No hay sugerencias pendientes.",
                    subtitulo="El motor de homologación no encontró anomalías recientes.",
                )
        else:
            # ── KPIs rápidos ──────────────────────────────────────────────────
            total     = len(df)
            alta_conf = len(df[df["Score"] >= 0.85]) if "Score" in df.columns else 0
            baja_conf = total - alta_conf

            c1, c2, c3 = st.columns(3)
            c1.metric("📋 Total pendientes",       total)
            c2.metric("🟢 Alta confianza (≥0.85)", alta_conf)
            c3.metric("🟡 Baja confianza (<0.85)", baja_conf)

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Filtros ───────────────────────────────────────────────────────
            f1, f2 = st.columns(2)
            with f1:
                tabla_sel = st.selectbox(
                    "Filtrar por tabla",
                    ["Todas"] + sorted(df["Tabla"].unique().tolist()),
                    key="pend_tabla",
                )
            with f2:
                campo_sel = st.selectbox(
                    "Filtrar por campo",
                    ["Todos"] + sorted(df["Campo"].unique().tolist()),
                    key="pend_campo",
                )

            df_filtrado = df.copy()
            if tabla_sel != "Todas":
                df_filtrado = df_filtrado[df_filtrado["Tabla"] == tabla_sel]
            if campo_sel != "Todos":
                df_filtrado = df_filtrado[df_filtrado["Campo"] == campo_sel]

            st.markdown(f"**{len(df_filtrado)}** sugerencias coinciden con los filtros.")
            st.markdown("---")

            # ── Tabla editable con paginación ─────────────────────────────────
            df_edit = df_filtrado.copy()
            df_edit.insert(0, "Seleccionar", False)
            df_edit["Acción"]     = "⏳ Pendiente"
            df_edit["Corrección"] = df_edit["Valor canónico sugerido"]

            ACCIONES = ["⏳ Pendiente", "✅ Aprobar", "✏️ Corregir", "❌ Rechazar"]

            count = len(df_edit)
            start, end = crear_paginacion_ui(count, 15, "pendientes_edit")
            df_page = df_edit.iloc[start:end].copy()

            st.caption("Selecciona registros, elige una acción y edita la corrección si es necesario.")

            edited = st.data_editor(
                df_page,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Seleccionar": st.column_config.CheckboxColumn(
                        "✔", help="Seleccionar para acción masiva", width="small"
                    ),
                    "Acción": st.column_config.SelectboxColumn(
                        "Acción", options=ACCIONES, width="medium"
                    ),
                    "Corrección": st.column_config.TextColumn(
                        "Corrección", help="Edita el valor canónico si necesitas corregirlo", width="large"
                    ),
                    "Score": st.column_config.ProgressColumn(
                        "Score", min_value=0.0, max_value=1.0, format="%.2f"
                    ),
                    "Texto crudo":            st.column_config.TextColumn("Texto crudo", width="large"),
                    "Valor canónico sugerido": st.column_config.TextColumn("Sugerido",    width="large"),
                    "Tabla":                  st.column_config.TextColumn("Tabla",        width="medium"),
                    "Campo":                  st.column_config.TextColumn("Campo",        width="medium"),
                    "Fecha":                  st.column_config.TextColumn("Fecha",        width="small"),
                },
                disabled=["Tabla", "Campo", "Texto crudo", "Valor canónico sugerido", "Score", "Fecha"],
                key="editor_pendientes",
            )

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Botones de acción masiva ───────────────────────────────────────
            b1, b2, b3, b4 = st.columns([2, 2, 2, 4])
            with b1:
                if st.button("✅ Aprobar seleccionados", key="btn_aprobar_masivo", type="primary"):
                    seleccionados = edited[edited["Seleccionar"] == True]
                    if len(seleccionados) == 0:
                        st.toast("Selecciona al menos un registro.", icon="⚠️")
                    else:
                        st.toast(f"{len(seleccionados)} registro(s) aprobados (simulación).", icon="✅")
            with b2:
                if st.button("❌ Rechazar seleccionados", key="btn_rechazar_masivo"):
                    seleccionados = edited[edited["Seleccionar"] == True]
                    if len(seleccionados) == 0:
                        st.toast("Selecciona al menos un registro.", icon="⚠️")
                    else:
                        st.toast(f"{len(seleccionados)} registro(s) rechazados (simulación).", icon="❌")
            with b3:
                if st.button("💾 Aplicar decisiones", key="btn_aplicar_decisiones", type="primary"):
                    aprobados  = edited[edited["Acción"] == "✅ Aprobar"]
                    corregidos = edited[edited["Acción"] == "✏️ Corregir"]
                    rechazados = edited[edited["Acción"] == "❌ Rechazar"]
                    total_acc  = len(aprobados) + len(corregidos) + len(rechazados)
                    if total_acc == 0:
                        st.toast("No hay acciones pendientes de aplicar.", icon="ℹ️")
                    else:
                        st.toast(
                            f"Simulación: {len(aprobados)} aprobados, "
                            f"{len(corregidos)} corregidos, {len(rechazados)} rechazados.",
                            icon="💾",
                        )

    # ── Tab 2: Historial ──────────────────────────────────────────────────────
    with tab2:
        if not conectado:
            st.warning("Sin conexión a la base de datos.")
            return

        df_hist = cargar_historial_homologacion()

        if df_hist.empty:
            st.markdown("<br>", unsafe_allow_html=True)
            estado_vacio_html(
                icono="📚",
                titulo="Sin historial todavía",
                subtitulo="Aún no hay aprobaciones históricas registradas en el sistema.",
            )
        else:
            st.markdown("### Historial de homologaciones aprobadas")
            h1, h2, h3 = st.columns(3)
            with h1:
                filtro_tabla = st.selectbox(
                    "Tabla", ["Todas"] + list(df_hist["Tabla"].unique()), key="hist_tabla"
                )
            with h2:
                filtro_usuario = st.selectbox(
                    "Aprobado por", ["Todos"] + list(df_hist["Aprobado por"].unique()), key="hist_usuario"
                )
            with h3:
                st.date_input("Desde fecha", key="hist_fecha", value=None)

            if filtro_tabla   != "Todas":
                df_hist = df_hist[df_hist["Tabla"]       == filtro_tabla]
            if filtro_usuario != "Todos":
                df_hist = df_hist[df_hist["Aprobado por"] == filtro_usuario]

            seccion_tabla_con_guardar(
                df_hist,
                key="historial",
                titulo="",
                page_size=15,
                caption="",
                mostrar_boton_guardar=False,
            )

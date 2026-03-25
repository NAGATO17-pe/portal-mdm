import streamlit as st
import pandas as pd
from utils.formato import header_pagina, score_a_color, crear_paginacion_ui, renderizar_tabla_premium
from utils.db import ejecutar_query, verificar_conexion

@st.cache_data(ttl=60, show_spinner=False)
def cargar_sugerencias_pendientes() -> pd.DataFrame:
    """Obtiene registros de homologacion pendientes en la cuarentena."""
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
    """Lista de todas las decisiones tomadas en el MDM."""
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


def render():
    conectado = verificar_conexion()
    header_pagina("🔗", "Homologación",
        "Sugerencias automáticas pendientes de revisión · aprueba, corrige o rechaza"
    )

    tab1, tab2 = st.tabs(["📬 Pendientes de aprobación", "📚 Historial aprobado"])

    # ── Tab 1: Pendientes (Tabla Editable) ────────────────────────────────
    with tab1:
        if not conectado:
            st.warning("Sin conexion a la base de datos.")
            return

        df = cargar_sugerencias_pendientes()

        if df.empty:
            from streamlit_lottie import st_lottie
            from utils.formato import load_lottieurl
            
            st.markdown("<br>", unsafe_allow_html=True)
            c1, c2, c3 = st.columns([1, 2, 1])
            with c2:
                lottie_empty = load_lottieurl("https://lottie.host/17238210-dd5a-4efb-86d9-f55ddcf24a0d/iLwEvO4cI3.json")
                if lottie_empty:
                    st_lottie(lottie_empty, height=200, key="homologacion_vacia")
                st.markdown("<h4 style='text-align:center; color:var(--text-color); opacity:0.8;'>No hay sugerencias pendientes.</h4>", unsafe_allow_html=True)
                st.markdown("<p style='text-align:center; color:#888; font-size:0.95rem;'>El motor de homologación no encontró anomalías recientes.</p>", unsafe_allow_html=True)
        else:
            # ── KPIs rápidos ──
            total = len(df)
            alta_conf = len(df[df["Score"] >= 0.85]) if "Score" in df.columns else 0
            baja_conf = total - alta_conf

            k1, k2, k3 = st.columns(3)
            k1.metric("📋 Total pendientes", total)
            k2.metric("🟢 Alta confianza (≥0.85)", alta_conf)
            k3.metric("🟡 Baja confianza (<0.85)", baja_conf)

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Filtros ──
            f1, f2 = st.columns(2)
            with f1:
                tablas_disp = ["Todas"] + sorted(df["Tabla"].unique().tolist())
                tabla_sel = st.selectbox("Filtrar por tabla", tablas_disp, key="pend_tabla")
            with f2:
                campos_disp = ["Todos"] + sorted(df["Campo"].unique().tolist())
                campo_sel = st.selectbox("Filtrar por campo", campos_disp, key="pend_campo")

            df_filtrado = df.copy()
            if tabla_sel != "Todas":
                df_filtrado = df_filtrado[df_filtrado["Tabla"] == tabla_sel]
            if campo_sel != "Todos":
                df_filtrado = df_filtrado[df_filtrado["Campo"] == campo_sel]

            st.markdown(f"**{len(df_filtrado)}** sugerencias coinciden con los filtros.")
            st.markdown("---")

            # ── Preparar DataFrame editable ──
            df_edit = df_filtrado.copy()
            df_edit.insert(0, "Seleccionar", False)
            df_edit["Acción"] = "⏳ Pendiente"
            df_edit["Corrección"] = df_edit["Valor canónico sugerido"]

            ACCIONES = ["⏳ Pendiente", "✅ Aprobar", "✏️ Corregir", "❌ Rechazar"]

            # ── Paginación ──
            count = len(df_edit)
            start, end = crear_paginacion_ui(count, 15, "pendientes_edit")
            df_page = df_edit.iloc[start:end].copy()

            st.caption("Selecciona registros, elige una acción, y edita la corrección si es necesario.")

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
                    "Texto crudo": st.column_config.TextColumn("Texto crudo", width="large"),
                    "Valor canónico sugerido": st.column_config.TextColumn("Sugerido", width="large"),
                    "Tabla": st.column_config.TextColumn("Tabla", width="medium"),
                    "Campo": st.column_config.TextColumn("Campo", width="medium"),
                    "Fecha": st.column_config.TextColumn("Fecha", width="small"),
                },
                disabled=["Tabla", "Campo", "Texto crudo", "Valor canónico sugerido", "Score", "Fecha"],
                key="editor_pendientes",
            )

            st.markdown("<br>", unsafe_allow_html=True)

            # ── Botones de acción masiva ──
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
                    aprobados = edited[edited["Acción"] == "✅ Aprobar"]
                    corregidos = edited[edited["Acción"] == "✏️ Corregir"]
                    rechazados = edited[edited["Acción"] == "❌ Rechazar"]
                    total_acciones = len(aprobados) + len(corregidos) + len(rechazados)
                    if total_acciones == 0:
                        st.toast("No hay acciones pendientes de aplicar.", icon="ℹ️")
                    else:
                        st.toast(
                            f"Simulación: {len(aprobados)} aprobados, {len(corregidos)} corregidos, "
                            f"{len(rechazados)} rechazados.",
                            icon="💾"
                        )

    # ── Tab 2: Historial ──────────────────────────────────────────────────
    with tab2:
        if not conectado:
            st.warning("Sin conexion a la base de datos.")
        else:
            df_hist = cargar_historial_homologacion()

            if df_hist.empty:
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown("<p style='text-align:center; color:var(--text-color); opacity:0.6;'>Aún no hay aprobaciones históricas registradas en el sistema.</p>", unsafe_allow_html=True)
            else:
                st.markdown("### Historial de homologaciones aprobadas")
                h1, h2, h3 = st.columns(3)
                with h1:
                    filtro_tabla = st.selectbox(
                        "Tabla",
                        ["Todas"] + list(df_hist["Tabla"].unique()),
                        key="hist_tabla"
                    )
                with h2:
                    filtro_usuario = st.selectbox(
                        "Aprobado por",
                        ["Todos"] + list(df_hist["Aprobado por"].unique()),
                        key="hist_usuario"
                    )
                with h3:
                    st.date_input("Desde fecha", key="hist_fecha", value=None)

                if filtro_tabla != "Todas":
                    df_hist = df_hist[df_hist["Tabla"] == filtro_tabla]
                if filtro_usuario != "Todos":
                    df_hist = df_hist[df_hist["Aprobado por"] == filtro_usuario]

                # Tabla premium para Historial
                renderizar_tabla_premium(df_hist, key="historial", page_size=15)


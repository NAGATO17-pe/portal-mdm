from utils.db import ejecutar_query, verificar_conexion

@st.cache_data(ttl=60, show_spinner=False)
def cargar_sugerencias_pendientes() -> pd.DataFrame:
    """
    Obtiene registros que requieren homologacion (Estado_Carga = 'EN_CUARENTENA' 
    y que tienen algun valor Sugerido en la tabla de MDM correspondiente).
    Como fallback mostramos solo los registros en cuarentena para revisar.
    """
    return ejecutar_query("""
        SELECT 
            Valor_Raw          AS [Texto crudo],
            Valor_Sugerido     AS [Valor canónico sugerido],
            Score              AS [Score],
            Tabla_Origen       AS [Tabla origen],
            1                  AS [Veces visto],
            CONVERT(varchar, Fecha_Sistema, 23) AS [Fecha]
        FROM MDM.Log_Sugerencias_MDM
        WHERE Estado_Revision = 'PENDIENTE'
        ORDER BY Score DESC, Fecha_Sistema DESC
    """)

@st.cache_data(ttl=60, show_spinner=False)
def cargar_historial_homologacion() -> pd.DataFrame:
    """Lista de ultimas 100 decisiones tomadas en el MDM."""
    return ejecutar_query("""
        SELECT TOP 100
            Valor_Raw          AS [Texto crudo],
            Valor_Mapeado      AS [Valor canónico],
            0.99               AS [Score],
            Tabla_Destino      AS [Tabla],
            Usuario_Revision   AS [Aprobado por],
            CONVERT(varchar, Fecha_Revision, 23) AS [Fecha aprobación]
        FROM MDM.Log_Decisiones_MDM
        ORDER BY Fecha_Revision DESC
    """)


def render():
    conectado = verificar_conexion()
    header_pagina("🔗", "Homologación",
        "Sugerencias automáticas pendientes de revisión · aprueba, corrige o rechaza"
    )

    tab1, tab2 = st.tabs(["📬 Pendientes de aprobación", "📚 Historial aprobado"])

    # ── Tab 1: Pendientes ─────────────────────────────────────────────────
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
                # Using a generic fast cloud sync or check animation for empty queue
                lottie_empty = load_lottieurl("https://lottie.host/17238210-dd5a-4efb-86d9-f55ddcf24a0d/iLwEvO4cI3.json")
                if lottie_empty:
                    st_lottie(lottie_empty, height=200, key="homologacion_vacia")
                st.markdown("<h4 style='text-align:center; color:var(--text-color); opacity:0.8;'>No hay sugerencias pendientes.</h4>", unsafe_allow_html=True)
                st.markdown("<p style='text-align:center; color:#888; font-size:0.95rem;'>El motor de homologación no encontró anomalias recientes.</p>", unsafe_allow_html=True)
        else:
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
                        st.button("✅ Aprobar", key=f"apr_{idx}", type="primary", disabled=True)
                    with acc2:
                        st.text_input(
                            "Corregir valor canónico",
                            value=row["Valor canónico sugerido"],
                            key=f"corr_{idx}",
                            label_visibility="collapsed",
                        )
                        st.button("✏️ Aprobar con corrección", key=f"apc_{idx}", disabled=True)
                    with acc3:
                        st.button("❌ Rechazar", key=f"rej_{idx}", disabled=True)
                    st.markdown("---")

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

            st.dataframe(df_hist, width="stretch", hide_index=True)

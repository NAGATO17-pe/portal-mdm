import streamlit as st
import pandas as pd
from utils.formato import header_pagina, colorear_severidad

# DataFrame vacío como placeholder para registros en cuarentena
CUARENTENA = pd.DataFrame(columns=[
    "ID", "Tabla Origen", "Columna Origen", "Valor Raw", "Motivo", "Severidad", "Fecha ingreso", "Estado"
])

def render():
    header_pagina(
        "🔴", "Cuarentena",
        "Revisión de filas rechazadas · Decide qué hacer con cada registro"
    )

    df = CUARENTENA.copy()

    if df.empty:
        from streamlit_lottie import st_lottie
        from utils.formato import load_lottieurl
        
        st.markdown("<br>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            lottie_empty = load_lottieurl("https://lottie.host/7e0a2e58-fbaa-4f51-a9dd-481cfc684bed/9jY2A5y8jT.json")
            if lottie_empty:
                st_lottie(lottie_empty, height=220, key="cuarentena_vacia")
            st.markdown("<h4 style='text-align:center; color:var(--text-color); opacity:0.8;'>No hay registros en cuarentena.</h4>", unsafe_allow_html=True)
            st.markdown("<p style='text-align:center; color:#888; font-size:0.95rem;'>¡Todo el pipeline fluyó sin errores biológicos ni fallos de formato!</p>", unsafe_allow_html=True)
        return

    # ── Filtros ───────────────────────────────────────────────────────────
    with st.container():
        st.markdown("### 🔍 Filtros")
        fc1, fc2, fc3, fc4, fc5 = st.columns([2, 2, 1.5, 1.5, 2])
        with fc1:
            tablas_disp = ["Todas"] + sorted(df["Tabla Origen"].unique().tolist())
            tabla_sel = st.selectbox("Tabla origen", tablas_disp, key="cuar_tabla")
        with fc2:
            cols_disp = ["Todas"] + sorted(df["Columna Origen"].unique().tolist())
            col_sel = st.selectbox("Columna", cols_disp, key="cuar_col")
        with fc3:
            sev_disp = ["Todas"] + sorted(df["Severidad"].unique().tolist())
            sev_sel = st.selectbox("Severidad", sev_disp, key="cuar_sev")
        with fc4:
            estado_disp = ["Todos"] + sorted(df["Estado"].unique().tolist())
            estado_sel = st.selectbox("Estado", estado_disp, key="cuar_estado")
        with fc5:
            buscar = st.text_input("Buscar valor raw…", key="cuar_buscar")

    # Aplicar filtros
    if tabla_sel != "Todas":
        df = df[df["Tabla Origen"] == tabla_sel]
    if col_sel != "Todas":
        df = df[df["Columna Origen"] == col_sel]
    if sev_sel != "Todas":
        df = df[df["Severidad"] == sev_sel]
    if estado_sel != "Todos":
        df = df[df["Estado"] == estado_sel]
    if buscar:
        df = df[df["Valor Raw"].str.contains(buscar, case=False, na=False)]

    st.markdown(f"**{len(df)} registros** coinciden con los filtros.")
    st.markdown("---")

    # ── Tabla cuarentena ──────────────────────────────────────────────────
    st.markdown("### 📋 Registros en cuarentena")

    cols_tabla = ["ID", "Tabla Origen", "Columna Origen", "Valor Raw", "Motivo", "Severidad", "Fecha ingreso", "Estado"]
    st.dataframe(
        df[cols_tabla].style.map(colorear_severidad, subset=["Severidad"]),
        width="stretch",
        hide_index=True,
        height=280,
    )

    # ── Acciones masivas ──────────────────────────────────────────────────
    with st.expander("⚡ Acciones masivas", expanded=False):
        ac1, ac2 = st.columns(2)
        with ac1:
            ids_selec = st.multiselect(
                "Seleccionar IDs para acción masiva",
                options=df["ID"].tolist(),
                key="cuar_masivo_ids"
            )
        with ac2:
            accion_masiva = st.selectbox(
                "Acción a aplicar",
                ["Descartar", "Marcar como Test Block", "Enviar a homologación"],
                key="cuar_masivo_accion"
            )
        if st.button("🚀 Aplicar a selección", key="btn_masivo", disabled=len(ids_selec) == 0):
            st.info("Conexión a BD no disponible para aplicar cambios.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Panel de decisión individual ──────────────────────────────────────
    st.markdown("### 🧩 Panel de decisión individual")
    st.caption("Selecciona un ID para revisar y resolver el registro.")

    id_sel = st.selectbox("ID de fila a revisar", options=df["ID"].tolist(), key="cuar_id_sel")

    if id_sel:
        fila = df[df["ID"] == id_sel].iloc[0]
        
        st.markdown(f"""
            <div class="decision-panel">
                <h4>🔎 Revisando registro ID {id_sel}</h4>
                <div class="decision-info">
                    <b>Valor raw:</b> &nbsp;<code>{fila['Valor Raw']}</code>&emsp;
                    <b>Columna:</b> &nbsp;{fila['Columna Origen']}&emsp;
                    <b>Tabla:</b> &nbsp;{fila['Tabla Origen']}
                </div>
            </div>
        """, unsafe_allow_html=True)

        st.markdown("**¿Qué es este valor?**")
        opcion = st.radio(
            "Acción",
            ["🌱 Variedad nueva — agregar al catálogo",
             "✏️ Mal escrita — homologar a valor existente",
             "🧪 Es Test Block — marcar módulo",
             "🗑️ Descartar — ignorar este registro"],
            key=f"radio_decision_{id_sel}",
            label_visibility="collapsed",
        )

        d1, d2 = st.columns([3, 1])
        if "nueva" in opcion:
            with d1:
                st.text_input("Nombre canónico nuevo:", key=f"nuevo_{id_sel}")
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                st.button("➕ Agregar", key=f"btn_agregar_{id_sel}", type="primary", disabled=True)

        elif "homologar" in opcion:
            with d1:
                st.text_input("Homologar a:", key=f"homologar_{id_sel}")
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                st.button("✅ Aprobar", key=f"btn_aprobar_{id_sel}", type="primary", disabled=True)

        elif "Test Block" in opcion:
            st.button("🧪 Marcar como Test Block", key=f"btn_tb_{id_sel}", type="primary", disabled=True)

        elif "Descartar" in opcion:
            st.button("🗑️ Descartar registro", key=f"btn_desc_{id_sel}", disabled=True)
        
        st.caption("Los botones de acción están deshabilitados hasta conectar la base de datos.")

"""
cuarentena.py — Página de revisión de filas rechazadas.
Panel de decisión para resolver cada registro en cuarentena.
"""
import streamlit as st
import pandas as pd
from utils.formato import header_pagina, colorear_severidad
from utils.datos_mock import CUARENTENA


def render():
    header_pagina(
        "🔴", "Cuarentena",
        "Revisión de filas rechazadas · Decide qué hacer con cada registro"
    )

    df = CUARENTENA.copy()

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
        df[cols_tabla].style.applymap(colorear_severidad, subset=["Severidad"]),
        use_container_width=True,
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
            st.success(f"✅ Acción **{accion_masiva}** aplicada a {len(ids_selec)} registros (demo).")

        if st.button("📥 Exportar a Excel", key="btn_export"):
            st.info("📄 Exportación simulada (demo). Aquí se generaría el archivo .xlsx.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Panel de decisión individual ──────────────────────────────────────
    st.markdown("### 🧩 Panel de decisión individual")
    st.caption("Selecciona un ID para revisar y resolver el registro.")

    id_sel = st.selectbox("ID de fila a revisar", options=df["ID"].tolist(), key="cuar_id_sel")

    if id_sel:
        fila = CUARENTENA[CUARENTENA["ID"] == id_sel].iloc[0]
        filas_afectadas = int(len(CUARENTENA[CUARENTENA["Valor Raw"] == fila["Valor Raw"]]))

        # Sugerencias mock basadas en el valor
        sugerencias_mock = {
            "FCM14-057": [("Megacrisp", 0.71), ("Kms1530", 0.62)],
            "BILOXY":    [("Biloxi", 0.88), ("Blueray", 0.55)],
            "25.0":      [],
            "0.1":       [],
        }
        sugerencias = sugerencias_mock.get(fila["Valor Raw"], [("Draper", 0.65), ("O'Neal", 0.58)])

        st.markdown(f"""
            <div class="decision-panel">
                <h4>🔎 Revisando registro ID {id_sel}</h4>
                <div class="decision-info">
                    <b>Valor raw:</b> &nbsp;<code>{fila['Valor Raw']}</code>&emsp;
                    <b>Columna:</b> &nbsp;{fila['Columna Origen']}&emsp;
                    <b>Tabla:</b> &nbsp;{fila['Tabla Origen']}&emsp;
                    <b>Filas afectadas:</b> &nbsp;{filas_afectadas}
                </div>
            </div>
        """, unsafe_allow_html=True)

        if sugerencias:
            st.markdown("**🔮 Valores similares en catálogo:**")
            for nombre, score in sugerencias:
                icono = "🟢" if score >= 0.85 else "🟡" if score >= 0.70 else "🔴"
                st.markdown(f"&nbsp;&nbsp;&nbsp;{icono} **{nombre}** &nbsp;(score {score:.2f})")

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
                nuevo_nombre = st.text_input("Nombre canónico nuevo:", key=f"nuevo_{id_sel}")
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("➕ Agregar", key=f"btn_agregar_{id_sel}", type="primary"):
                    if nuevo_nombre:
                        st.success(f"✅ Variedad **{nuevo_nombre}** agregada al catálogo (demo).")
                    else:
                        st.warning("Ingresa un nombre canónico.")

        elif "homologar" in opcion:
            with d1:
                homologar_a = st.text_input(
                    "Homologar a:",
                    value=sugerencias[0][0] if sugerencias else "",
                    key=f"homologar_{id_sel}"
                )
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✅ Aprobar", key=f"btn_aprobar_{id_sel}", type="primary"):
                    st.success(f"✅ Homologado → **{homologar_a}** (demo).")

        elif "Test Block" in opcion:
            if st.button("🧪 Marcar como Test Block", key=f"btn_tb_{id_sel}", type="primary"):
                st.success("✅ Módulo marcado como Test Block en geografía (demo).")

        elif "Descartar" in opcion:
            if st.button("🗑️ Descartar registro", key=f"btn_desc_{id_sel}"):
                st.warning("⚠️ Registro descartado y excluido de próximas cargas (demo).")

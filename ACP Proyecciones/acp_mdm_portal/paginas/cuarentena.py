"""
paginas/cuarentena.py — Página de Cuarentena del portal MDM ACP
================================================================
Presenta los registros rechazados por el ETL y permite al equipo
revisar, aprobar, homologar o descartar cada uno.
"""

import io

import pandas as pd
import streamlit as st

from utils.componentes import badge_html, estado_vacio_html, mostrar_kpis, mostrar_dialogo_confirmacion
from utils.db import ejecutar_query
from utils.formato import header_pagina, renderizar_tabla_premium


# ── Query cacheada ────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def cargar_cuarentena() -> pd.DataFrame:
    """
    Lee todos los registros con Estado_Carga = 'EN_CUARENTENA'
    de las tablas Bronce. Retorna columnas estandarizadas.
    """
    tablas = [
        ("Bronce.Evaluacion_Pesos",      "Variedad_Raw",  "ID_Evaluacion_Pesos"),
        ("Bronce.Conteo_Fruta",          "Variedad_Raw",  "ID_Conteo_Fruta"),
        ("Bronce.Calibres",              "Variedad_Raw",  "ID_Calibre"),
        ("Bronce.Peladas",               "Variedad_Raw",  "ID_Pelada"),
        ("Bronce.Ciclos_Fenologicos",    "Variedad_Raw",  "ID_Ciclo"),
        ("Bronce.Fisiologia",            "Variedad_Raw",  "ID_Fisiologia"),
        ("Bronce.Evaluacion_Vegetativa", "Variedad_Raw",  "ID_Evaluacion_Vegetativa"),
        ("Bronce.Sanidad_Activo",        "Variedad_Raw",  "ID_Sanidad_Activo"),
        ("Bronce.Ciclo_Poda",            "Variedad_Raw",  "ID_Ciclo_Poda"),
        ("Bronce.Tareo",                 "Actividad_Raw", "ID_Tareo"),
        ("Bronce.Data_SAP",              "Material_Raw",  "ID_Data_SAP"),
        ("Bronce.Telemetria_Clima",      "Sector_Raw",    "ID_Telemetria"),
    ]
    partes = []
    for tabla, col_valor, col_id in tablas:
        try:
            df = ejecutar_query(f"""
                SELECT
                    '{tabla}'                   AS [Tabla Origen],
                    CAST({col_id} AS VARCHAR)   AS [ID],
                    '{col_valor}'               AS [Columna Origen],
                    ISNULL({col_valor}, '(NULL)') AS [Valor Raw],
                    Nombre_Archivo              AS [Archivo],
                    Fecha_Sistema               AS [Fecha ingreso],
                    Estado_Carga                AS [Estado],
                    'ALTO'                      AS [Severidad],
                    'Requisito de homologación' AS [Motivo]
                FROM {tabla}
                WHERE Estado_Carga = 'EN_CUARENTENA'
            """)
            if not df.empty:
                partes.append(df)
        except Exception:
            pass  # Tabla no existe aún — se ignora

    if not partes:
        return pd.DataFrame(columns=[
            "Tabla Origen", "ID", "Columna Origen", "Valor Raw",
            "Archivo", "Fecha ingreso", "Estado", "Severidad", "Motivo",
        ])

    resultado = pd.concat(partes, ignore_index=True)
    resultado["Fecha ingreso"] = pd.to_datetime(resultado["Fecha ingreso"]).dt.strftime("%Y-%m-%d")
    return resultado


# ── Helpers ───────────────────────────────────────────────────────────────────

def _exportar_excel(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Cuarentena")
    return buffer.getvalue()


# ── Render ────────────────────────────────────────────────────────────────────

def render() -> None:
    header_pagina(
        "🔴", "Cuarentena",
        "Revisión de registros rechazados · Decide qué hacer con cada uno",
    )

    df_original = cargar_cuarentena()

    # ── KPIs rápidos ──────────────────────────────────────────────────────────
    total      = len(df_original)
    pendientes = int((df_original["Estado"] == "PENDIENTE").sum())
    criticos   = int((df_original["Severidad"] == "CRÍTICO").sum())
    resueltos  = int((df_original["Estado"] == "RESUELTO").sum())

    mostrar_kpis([
        {"label": "📋 Total registros", "value": total},
        {"label": "⏳ Pendientes",       "value": pendientes,
         "delta": f"-{resueltos} resueltos" if resueltos > 0 else None},
        {"label": "🔴 Críticos",         "value": criticos, "delta_color": "inverse"},
        {"label": "✅ Resueltos",        "value": resueltos},
    ])

    if criticos > 0:
        st.error(
            f"⚠️ Hay **{criticos} registro(s) CRÍTICO(S)** pendientes de revisión. "
            "Prioriza su resolución antes de la próxima carga.",
            icon="🚨",
        )

    st.markdown("<hr style='margin:16px 0;'>", unsafe_allow_html=True)

    # ── Filtros ───────────────────────────────────────────────────────────────
    st.markdown("### 🔍 Filtros")
    fc1, fc2, fc3, fc4, fc5 = st.columns([2, 2, 1.5, 1.5, 2])

    with fc1:
        tabla_sel = st.selectbox(
            "Tabla origen",
            ["Todas"] + sorted(df_original["Tabla Origen"].unique().tolist()),
            key="cuar_tabla",
        )
    with fc2:
        col_sel = st.selectbox(
            "Columna",
            ["Todas"] + sorted(df_original["Columna Origen"].unique().tolist()),
            key="cuar_col",
        )
    with fc3:
        sev_sel = st.selectbox("Severidad", ["Todas", "CRÍTICO", "ALTO", "MEDIO"], key="cuar_sev")
    with fc4:
        estado_sel = st.selectbox(
            "Estado",
            ["Todos"] + sorted(df_original["Estado"].unique().tolist()),
            key="cuar_estado",
        )
    with fc5:
        buscar = st.text_input("🔎 Buscar valor raw…", key="cuar_buscar", placeholder="ej: FCM14, Biloxi…")

    # Aplicar filtros
    df = df_original.copy()
    if tabla_sel  != "Todas": df = df[df["Tabla Origen"]   == tabla_sel]
    if col_sel    != "Todas": df = df[df["Columna Origen"] == col_sel]
    if sev_sel    != "Todas": df = df[df["Severidad"]      == sev_sel]
    if estado_sel != "Todos": df = df[df["Estado"]         == estado_sel]
    if buscar.strip():
        df = df[df["Valor Raw"].str.contains(buscar.strip(), case=False, na=False)]

    st.markdown(
        f"**{len(df)}** registro(s) coinciden · "
        f"tabla=*{tabla_sel}* | severidad=*{sev_sel}* | estado=*{estado_sel}*"
    )

    # ── Exportar ──────────────────────────────────────────────────────────────
    c_exp, _ = st.columns([1, 5])
    with c_exp:
        st.download_button(
            label="📥 Exportar a Excel",
            data=_exportar_excel(df),
            file_name="cuarentena_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.markdown("<br>", unsafe_allow_html=True)

    if df.empty:
        estado_vacio_html(
            icono="✅",
            titulo="Sin registros coincidentes",
            subtitulo="No hay registros que coincidan con los filtros seleccionados.",
        )
        return

    # ── Tabla de cuarentena ───────────────────────────────────────────────────
    st.markdown("### 📋 Registros en cuarentena")
    columnas_vista = ["ID", "Tabla Origen", "Columna Origen", "Valor Raw",
                      "Motivo", "Severidad", "Fecha ingreso", "Estado"]
    renderizar_tabla_premium(df[columnas_vista], key="cuarentena_tabla", page_size=15)

    st.markdown("<hr style='margin:24px 0;'>", unsafe_allow_html=True)

    # ── Acciones masivas ──────────────────────────────────────────────────────
    with st.expander("⚡ Acciones masivas sobre registros filtrados", expanded=False):
        ids_disponibles = df[df["Estado"] == "PENDIENTE"]["ID"].tolist()
        if not ids_disponibles:
            st.info("Sin registros PENDIENTE en la selección actual.")
        else:
            am1, am2 = st.columns([2, 2])
            with am1:
                ids_selec = st.multiselect(
                    "Seleccionar IDs (solo PENDIENTE)",
                    options=ids_disponibles,
                    key="cuar_masivo_ids",
                )
            with am2:
                accion_masiva = st.selectbox(
                    "Acción a aplicar",
                    ["Descartar", "Marcar como Test Block", "Enviar a homologación"],
                    key="cuar_masivo_accion",
                )
            if st.button(
                "🚀 Aplicar a selección",
                key="btn_masivo",
                disabled=len(ids_selec) == 0,
                type="primary",
            ):
                st.toast(
                    f"Acción masiva '{accion_masiva}' aplicada a {len(ids_selec)} ID(s).",
                    icon="✅"
                )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Panel de decisión individual ──────────────────────────────────────────
    st.markdown("### 🧩 Panel de decisión individual")
    st.caption("Selecciona un ID de la tabla para revisar y resolver el registro.")

    ids_opciones = df["ID"].tolist()
    id_sel = st.selectbox(
        "ID a revisar",
        options=ids_opciones,
        format_func=lambda i: f"#{i} — {df.loc[df['ID']==i, 'Valor Raw'].iloc[0]}",
        key="cuar_id_sel",
    )

    if id_sel is not None:
        fila = df[df["ID"] == id_sel].iloc[0]
        sev  = fila["Severidad"]
        sev_color = {"CRÍTICO": "#C0392B", "ALTO": "#E67E22", "MEDIO": "#1E6B35"}.get(sev, "#555")

        # Panel de detalle con badges unificados desde componentes
        st.markdown(
            f"""
            <div style="background:var(--secondary-background-color);
                        border:1px solid rgba(128,128,128,0.2);
                        border-left:5px solid {sev_color};
                        border-radius:12px;
                        padding:20px 26px;
                        margin-bottom:20px;
                        box-shadow:0 2px 8px rgba(0,0,0,0.06);">
                <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:8px;">
                    <h4 style="margin:0; color:var(--verde-acp, #1E6B35);">🔎 Registro #{id_sel}</h4>
                    {badge_html(sev)} &nbsp; {badge_html(fila['Estado'])}
                </div>
                <div style="margin-top:14px; display:flex; flex-wrap:wrap; gap:24px; font-size:0.9rem;">
                    <div><b>Valor raw:</b> &nbsp;<code style="background:rgba(0,0,0,0.07);
                         padding:2px 8px; border-radius:6px;">{fila['Valor Raw']}</code></div>
                    <div><b>Columna:</b> &nbsp;{fila['Columna Origen']}</div>
                    <div><b>Tabla:</b> &nbsp;{fila['Tabla Origen']}</div>
                    <div><b>Fecha ingreso:</b> &nbsp;{fila['Fecha ingreso']}</div>
                </div>
                <div style="margin-top:12px; background:rgba(0,0,0,0.04);
                            border-radius:8px; padding:10px 14px;
                            font-size:0.88rem; color:var(--text-color);">
                    <b>Motivo de rechazo:</b> {fila['Motivo']}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # ── Opciones de resolución ────────────────────────────────────────────
        st.markdown("**¿Qué es este valor?**")
        opcion = st.radio(
            "Acción de resolución",
            options=[
                "🌱 Variedad nueva — agregar al catálogo",
                "✏️ Mal escrita — homologar a valor existente",
                "🧪 Es Test Block — marcar módulo",
                "🗑️ Descartar — ignorar este registro",
            ],
            key=f"radio_decision_{id_sel}",
            label_visibility="collapsed",
            horizontal=True,
        )

        st.markdown("<br>", unsafe_allow_html=True)
        d1, d2, _ = st.columns([3, 2, 1])

        if "nueva" in opcion:
            with d1:
                nuevo_nombre = st.text_input(
                    "Nombre canónico (formato oficial del catálogo):",
                    placeholder="ej: Biloxi, Emerald, Legacy…",
                    key=f"nuevo_{id_sel}",
                )
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("➕ Agregar variedad", key=f"btn_agregar_{id_sel}",
                             type="primary", disabled=not nuevo_nombre.strip()):
                    def do_agregar(nom):
                        st.toast(f"Variedad '{nom}' marcada para agregar.", icon="✅")
                    mostrar_dialogo_confirmacion(
                        "Confirmación de Cuarentena",
                        f"¿Confirmas agregar la nueva variedad '{nuevo_nombre}' al catálogo maestro?",
                        do_agregar, nuevo_nombre
                    )
        elif "homologar" in opcion:
            with d1:
                valor_homo = st.text_input(
                    "Homologar a valor canónico:",
                    placeholder="ej: Biloxi",
                    key=f"homologar_{id_sel}",
                )
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✅ Aprobar homologación", key=f"btn_aprobar_{id_sel}",
                             type="primary", disabled=not valor_homo.strip()):
                    st.toast(
                        f"'{fila['Valor Raw']}' → '{valor_homo}' guardado en diccionario.", icon="✅"
                    )

        elif "Test Block" in opcion:
            with d1:
                st.info(
                    f"El módulo asociado a **'{fila['Valor Raw']}'** será marcado como "
                    f"**Test Block (Es_Test_Block = 1)**.",
                    icon="🧪",
                )
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("🧪 Confirmar Test Block", key=f"btn_tb_{id_sel}", type="primary"):
                    st.toast("Módulo marcado como Test Block.", icon="🧪")

        elif "Descartar" in opcion:
            with d1:
                st.warning(
                    f"El registro **#{id_sel}** (*{fila['Valor Raw']}*) será descartado "
                    f"y no se procesará en futuras cargas.",
                    icon="🗑️",
                )
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("🗑️ Confirmar descarte", key=f"btn_desc_{id_sel}"):
                    st.toast("Registro descartado exitosamente.", icon="🗑️")

        st.caption(
            "💡 Los cambios se persistirán en la BD cuando la conexión esté activa. "
            "Por ahora operan en modo simulación."
        )

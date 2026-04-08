import io
import pandas as pd
import streamlit as st
import time

from utils.componentes import badge_html, estado_vacio_html, mostrar_kpis, mostrar_dialogo_confirmacion
from utils.formato import header_pagina, renderizar_tabla_premium
from utils.api_client import get_api, mostrar_error_api, patch_api

@st.cache_data(ttl=5, show_spinner=False)
def cargar_cuarentena() -> pd.DataFrame:
    columnas_vacias = [
        "Tabla Origen", "ID", "Columna Origen", "Valor Raw",
        "Archivo", "Fecha ingreso", "Estado", "Severidad", "Motivo",
    ]
    resultado = get_api("/cuarentena?pagina=1&tamano=100")
    if resultado.ok and isinstance(resultado.data, dict):
        datos = resultado.data.get("datos", [])
        if not datos:
            return pd.DataFrame(columns=columnas_vacias)
        
        df = pd.DataFrame(datos)
        df.rename(columns={
            "tabla_origen": "Tabla Origen",
            "id_registro": "ID",
            "columna_origen": "Columna Origen",
            "valor_raw": "Valor Raw",
            "nombre_archivo": "Archivo",
            "fecha_ingreso": "Fecha ingreso",
            "estado": "Estado",
            "motivo": "Motivo",
        }, inplace=True)
        df["Severidad"] = "ALTO"
        return df
    return pd.DataFrame(columns=columnas_vacias)

def _exportar_excel(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Cuarentena")
    return buffer.getvalue()

def render() -> None:
    header_pagina("🔴", "Cuarentena", "Revisión de registros rechazados · Decide qué hacer con cada uno")
    df_original = cargar_cuarentena()

    total = len(df_original)
    pendientes = int((df_original["Estado"] == "PENDIENTE").sum()) if not df_original.empty else 0
    criticos = int((df_original["Severidad"] == "CRÍTICO").sum()) if not df_original.empty else 0
    resueltos = int((df_original["Estado"] == "RESUELTO").sum()) if not df_original.empty else 0

    mostrar_kpis([
        {"label": "📋 Total registros", "value": total},
        {"label": "⏳ Pendientes", "value": pendientes},
        {"label": "🔴 Críticos", "value": criticos, "delta_color": "inverse"},
        {"label": "✅ Resueltos", "value": resueltos},
    ])

    st.markdown("<hr style='margin:16px 0;'>", unsafe_allow_html=True)

    if df_original.empty:
        estado_vacio_html("✅", "Sin registros coincidentes", "No hay registros en cuarentena.")
        return

    st.markdown("### 📋 Registros en cuarentena")
    columnas_vista = ["ID", "Tabla Origen", "Columna Origen", "Valor Raw", "Motivo", "Severidad", "Fecha ingreso", "Estado"]
    renderizar_tabla_premium(df_original[columnas_vista], key="cuarentena_tabla", page_size=15)

    st.markdown("<hr style='margin:24px 0;'>", unsafe_allow_html=True)
    st.markdown("### 🧩 Panel de decisión individual")
    st.caption("Selecciona un ID de la tabla para revisar y resolver el registro.")

    ids_opciones = df_original["ID"].tolist()
    id_sel = st.selectbox(
        "ID a revisar",
        options=ids_opciones,
        format_func=lambda i: f"#{i} — {df_original.loc[df_original['ID']==i, 'Valor Raw'].iloc[0]}",
        key="cuar_id_sel",
    )

    if id_sel is not None:
        fila = df_original[df_original["ID"] == id_sel].iloc[0]
        
        st.markdown("**¿Qué es este valor?**")
        opcion = st.radio(
            "Acción de resolución",
            options=["✏️ Mal escrita — homologar a valor existente", "🗑️ Descartar — ignorar este registro"],
            key=f"radio_decision_{id_sel}",
            horizontal=True,
        )

        d1, d2, _ = st.columns([3, 2, 1])

        if "homologar" in opcion:
            with d1:
                valor_homo = st.text_input("Homologar a valor canónico:", key=f"homologar_{id_sel}")
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("✅ Aprobar homologación", key=f"btn_aprobar_{id_sel}", type="primary"):
                    resultado = patch_api(f"/cuarentena/{fila['Tabla Origen']}/{id_sel}/resolver", {"valor_canonico": valor_homo})
                    if resultado.ok:
                        st.toast(f"Homologación a '{valor_homo}' procesada por Backend.", icon="✅")
                        time.sleep(1)
                        st.rerun()
                    else:
                        mostrar_error_api(resultado, "Error del backend al resolver cuarentena.")

        elif "Descartar" in opcion:
            with d2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("🗑️ Confirmar descarte", key=f"btn_desc_{id_sel}"):
                    resultado = patch_api(f"/cuarentena/{fila['Tabla Origen']}/{id_sel}/rechazar", {"motivo": "Rechazado manualmente"})
                    if resultado.ok:
                        st.toast("Registro descartado en el Data Warehouse.", icon="🗑️")
                        time.sleep(1)
                        st.rerun()
                    else:
                        mostrar_error_api(resultado, "Error del backend al descartar cuarentena.")

import os
import sys
import threading
import pandas as pd
import streamlit as st

from utils.componentes import health_status_panel, seccion_tabla_con_guardar
from utils.formato import crear_tarjeta_kpi, header_pagina
from utils.api_client import get_api, post_api

@st.cache_data(ttl=60, show_spinner=False)
def _cargar_resumen_ultima_carga() -> pd.DataFrame:
    res = get_api("/etl/corridas")
    if res and res.status_code == 200:
        corridas = res.json()
        if corridas:
            return pd.DataFrame(corridas)
    # Valores de mockups para UI si no hay log:
    return pd.DataFrame()

@st.cache_data(ttl=60, show_spinner=False)
def _cargar_log_reciente() -> pd.DataFrame:
    res = get_api("/etl/corridas")
    if res and res.status_code == 200:
        return pd.DataFrame(res.json())
    return pd.DataFrame()

def render():
    header_pagina("Inicio", "Estado del pipeline · Data Warehouse ACP")
    conectado = health_status_panel()

    df_estado = _cargar_resumen_ultima_carga()

    total_ok = 0
    total_rechaz = 0
    ultima_carga = "Sin datos"
    tablas_con_error = 0

    if not df_estado.empty and "estado" in df_estado.columns:
        ultima_carga = df_estado.iloc[0]["fecha_inicio"] if "fecha_inicio" in df_estado.columns else "Sin datos"
        tablas_con_error = int((df_estado["estado"] == "ERROR").sum())
        total_ok = int(df_estado["filas_insertadas"].sum()) if "filas_insertadas" in df_estado.columns else 0
        total_rechaz = int(df_estado["filas_rechazadas"].sum()) if "filas_rechazadas" in df_estado.columns else 0

        # Mapeo reverso para que la tabla de historial se vea bien
        df_estado = df_estado.rename(columns={
            "id_log": "ID Log", "nombre_proceso": "Proceso", "tabla_destino": "Tabla DW",
            "nombre_archivo": "Archivo", "fecha_inicio": "Inicio", "fecha_fin": "Fin",
            "estado": "Estado", "filas_insertadas": "Filas OK", "filas_rechazadas": "Rechazadas",
            "duracion_segundos": "Segundos", "mensaje_error": "Detalle Error"
        })

    html_kpis = f"""<div class="kpi-container" style="margin-bottom: 32px;">
    {crear_tarjeta_kpi("Ultima carga", ultima_carga, "🕒", "info")}
    {crear_tarjeta_kpi("Filas OK (Aprox)", f"{total_ok:,}", "✅", "success")}
    {crear_tarjeta_kpi("Rechazadas", f"{total_rechaz:,}", "❌", "danger" if total_rechaz > 0 else "")}
    {crear_tarjeta_kpi("Corridas con error", str(tablas_con_error), "⚠️", "warning" if tablas_con_error > 0 else "success")}
    </div>"""
    st.markdown(html_kpis, unsafe_allow_html=True)

    st.markdown("### ⚙️ Centro de Comando MDM")
    with st.container(border=True):
        st.info("💡 **Operación:** Ejecuta el proceso ETL de forma asíncrona usando la nueva API.", icon="ℹ️")
        c_up, c_run = st.columns([1.5, 1], gap="large")

        with c_up:
            archivo_subido = st.file_uploader("📂 1. Seleccionar reporte", type=["xlsx", "xls"])
            if archivo_subido is not None:
                st.success(f"Archivo guardado en cola: `{archivo_subido.name}`")

        with c_run:
            st.markdown("<br>", unsafe_allow_html=True)
            etl_running = st.session_state.get("etl_running", False)
            if st.button("🚀 2. Ejecutar Pipeline ETL", use_container_width=True, type="primary"):
                with st.status("🛠️ Ejecutando Pipeline ETL...", expanded=True) as status:
                    res = post_api("/etl/corridas", {"comentario": "Portal Streamlit"})
                    if not res or res.status_code != 200:
                        st.error("Falla de API al lanzar la corrida. Verifica los logs del backend.")
                        status.update(label="❌ Falla de Pipeline", state="error")
                    else:
                        datos = res.json()
                        st.write(f"Conectado a Backend (ID: {datos['id_corrida'][:8]})")
                        status.update(label="✅ Pipeline corriendo de manera segura", state="complete")
                        st.toast("La ejecución ha sido despachada.", icon="🎉")
                        st.balloons()
    
    st.markdown("<hr style='margin: 32px 0;'>", unsafe_allow_html=True)

    if not conectado or df_estado.empty:
        st.info("No hay registros recientes.")
    else:
        seccion_tabla_con_guardar(
            df_estado,
            key="inicio_estado",
            titulo="📋 Historial de Corridas (v3 API)",
            page_size=10,
            caption="Última actualización reciente.",
            mostrar_boton_guardar=False,
        )

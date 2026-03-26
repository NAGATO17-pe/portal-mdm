"""
paginas/inicio.py — Página de inicio del portal MDM ACP (Enterprise)
=====================================================================
Health check descriptivo, KPIs, distribución de ingesta,
ETL en thread separado (non-blocking), tablas con paginación local.
"""

import os
import subprocess
import sys
import threading

import pandas as pd
import streamlit as st

from utils.componentes import health_status_panel, seccion_tabla_con_guardar
from utils.db import ejecutar_query, verificar_conexion
from utils.formato import crear_tarjeta_kpi, header_pagina


# ── Queries cacheadas ─────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def _cargar_resumen_ultima_carga() -> pd.DataFrame:
    """Resumen de la ÚLTIMA ejecución del pipeline por tabla."""
    return ejecutar_query("""
        SELECT
            lc.Tabla_Destino                              AS [Tabla],
            CONVERT(varchar, MAX(lc.Fecha_Inicio), 120)  AS [Ultima carga],
            SUM(lc.Filas_Insertadas)                     AS [Filas insertadas],
            SUM(lc.Filas_Rechazadas)                     AS [Filas rechazadas],
            MAX(lc.Estado_Proceso)                       AS [Estado]
        FROM Auditoria.Log_Carga lc
        WHERE lc.Fecha_Inicio >= CAST(DATEADD(day, -7, GETDATE()) AS DATE)
        GROUP BY lc.Tabla_Destino
        ORDER BY MAX(lc.Fecha_Inicio) DESC
    """)


@st.cache_data(ttl=60, show_spinner=False)
def _cargar_log_reciente() -> pd.DataFrame:
    """Log de las últimas 20 ejecuciones del pipeline."""
    return ejecutar_query("""
        SELECT TOP 20
            CONVERT(varchar, Fecha_Inicio, 120) AS [Fecha],
            Nombre_Proceso                      AS [Proceso],
            Tabla_Destino                       AS [Tabla],
            Filas_Leidas                        AS [Leidas],
            Filas_Insertadas                    AS [Insertadas],
            Filas_Rechazadas                    AS [Rechazadas],
            ROUND(Duracion_Segundos, 1)         AS [Duración (s)],
            Estado_Proceso                      AS [Estado]
        FROM Auditoria.Log_Carga
        ORDER BY Fecha_Inicio DESC
    """)


# ── ETL Runner en Thread ─────────────────────────────────────────────────────

def _ejecutar_etl_thread(ruta_pipeline: str, cwd: str):
    """Ejecuta el pipeline en un thread separado para no bloquear la UI."""
    st.session_state["etl_running"] = True
    st.session_state["etl_log"]     = []
    st.session_state["etl_code"]    = None

    try:
        proceso = subprocess.Popen(
            [sys.executable, ruta_pipeline],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=cwd,
        )
        for linea in iter(proceso.stdout.readline, ""):
            if linea:
                st.session_state["etl_log"].append(linea.strip())
        proceso.stdout.close()
        st.session_state["etl_code"] = proceso.wait()
    except Exception as e:
        st.session_state["etl_log"].append(f"ERROR: {e}")
        st.session_state["etl_code"] = -1
    finally:
        st.session_state["etl_running"] = False


# ── Render ────────────────────────────────────────────────────────────────────

def render():
    header_pagina("🏠", "Inicio", "Estado del pipeline · Data Warehouse ACP")

    # ── Health Check Descriptivo ──────────────────────────────────────────────
    conectado = health_status_panel()

    # ── Carga de datos ────────────────────────────────────────────────────────
    df_estado = pd.DataFrame()
    if conectado:
        try:
            df_estado = _cargar_resumen_ultima_carga()
        except Exception:
            pass

    total_ok         = int(df_estado["Filas insertadas"].sum()) if not df_estado.empty else 0
    total_rechaz     = int(df_estado["Filas rechazadas"].sum()) if not df_estado.empty else 0
    ultima_carga     = df_estado["Ultima carga"].max()          if not df_estado.empty else "Sin datos"
    tablas_con_error = int((df_estado["Estado"] != "OK").sum()) if not df_estado.empty else 0

    # ── KPIs principales ──────────────────────────────────────────────────────
    html_kpis = f"""<div class="kpi-container" style="margin-bottom: 32px;">
{crear_tarjeta_kpi("Ultima carga",     ultima_carga,           "🕒", "info")}
{crear_tarjeta_kpi("Filas OK",         f"{total_ok:,}",        "✅", "success")}
{crear_tarjeta_kpi("Rechazadas",       f"{total_rechaz:,}",    "❌", "danger" if total_rechaz > 0 else "")}
{crear_tarjeta_kpi("Tablas con error", str(tablas_con_error),  "⚠️", "warning" if tablas_con_error > 0 else "success")}
</div>"""
    st.markdown(html_kpis, unsafe_allow_html=True)

    # ── Distribución de Ingesta ───────────────────────────────────────────────
    st.markdown("### 📊 Distribución de Ingesta")
    total_filas = total_ok + total_rechaz
    pct_ok     = round(total_ok / total_filas * 100, 1) if total_filas > 0 else 0
    pct_rechaz = round(total_rechaz / total_filas * 100, 1) if total_filas > 0 else 0

    st.markdown(f"""<div style="
        background: var(--secondary-background-color);
        border: 1px solid rgba(128,128,128,0.15);
        border-radius: 12px; padding: 24px;
        box-shadow: var(--shadow-sm); margin-bottom: 32px;">
    <div style="display:flex; justify-content:space-between; margin-bottom:8px;">
        <span style="font-size:0.9rem; font-weight:600; color:#43843C;">Insertadas OK ({pct_ok}%)</span>
        <span style="font-size:0.9rem; font-weight:600; color:#D35D47;">Rechazadas ({pct_rechaz}%)</span>
    </div>
    <div style="width:100%; height:24px; background-color:var(--background-color);
                border-radius:12px; overflow:hidden; display:flex;
                border: 1px solid rgba(128,128,128,0.2);">
        <div style="width:{pct_ok}%; background:linear-gradient(90deg, #2D5A27, #43843C); transition:width 1s;"></div>
        <div style="width:{pct_rechaz}%; background:#D35D47; transition:width 1s;"></div>
    </div>
    <p style="text-align:center; font-size:0.8rem; color:var(--text-color); opacity:0.5; margin:12px 0 0 0;">
        {"Total filas procesadas: " + f"{total_filas:,}" if total_filas > 0 else "Sin datos recientes."}
    </p>
</div>""", unsafe_allow_html=True)

    # ── Centro de Comando MDM ─────────────────────────────────────────────────
    st.markdown("### ⚙️ Centro de Comando MDM")

    with st.container(border=True):
        st.info(
            "💡 **Operación completa:** 1. Sube tu reporte crudo. "
            "2. Presiona 'Ejecutar' para procesarlo en el Data Warehouse.",
            icon="ℹ️",
        )

        c_up, c_run = st.columns([1.5, 1], gap="large")

        with c_up:
            archivo_subido = st.file_uploader(
                "📂 1. Seleccionar reporte Excel (.xlsx, .xls) a encolar",
                type=["xlsx", "xls"],
                key="uploader_inicio",
            )
            if archivo_subido is not None:
                directorio_raiz    = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                directorio_destino = os.path.join(directorio_raiz, "ETL", "data", "inbound")
                os.makedirs(directorio_destino, exist_ok=True)
                ruta_archivo = os.path.join(directorio_destino, archivo_subido.name)
                with open(ruta_archivo, "wb") as f:
                    f.write(archivo_subido.getbuffer())
                st.success(f"Archivo guardado en cola: `{archivo_subido.name}`")

        with c_run:
            st.markdown("<br>", unsafe_allow_html=True)
            etl_running = st.session_state.get("etl_running", False)

            if st.button("🚀 2. Ejecutar Pipeline ETL", use_container_width=True,
                         type="primary", disabled=etl_running):
                directorio_raiz = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                ruta_pipeline   = os.path.join(directorio_raiz, "ETL", "pipeline.py")

                if not os.path.exists(ruta_pipeline):
                    st.error(f"No se encontró el motor ETL en:\n`{ruta_pipeline}`")
                else:
                    with st.status("🛠️ Ejecutando Pipeline ETL...", expanded=True) as status:
                        st.write("Iniciando motor Python externo...")
                        try:
                            proceso = subprocess.Popen(
                                [sys.executable, ruta_pipeline],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                text=True,
                                cwd=os.path.join(directorio_raiz, "ETL"),
                            )
                            for linea in iter(proceso.stdout.readline, ""):
                                if linea:
                                    st.text(linea.strip())
                            proceso.stdout.close()
                            codigo = proceso.wait()

                            if codigo == 0:
                                status.update(label="✅ Pipeline completado exitosamente", state="complete")
                                st.toast("Carga al Data Warehouse completada.", icon="🎉")
                                st.balloons()
                            else:
                                status.update(label=f"❌ Falla (Código {codigo})", state="error")
                                st.error("El pipeline falló. Revisa el log arriba.")
                        except Exception as e:
                            status.update(label="❌ Error crítico", state="error")
                            st.error(f"Excepción: {e}")

    st.markdown("<hr style='margin: 32px 0;'>", unsafe_allow_html=True)

    # ── Estado por tabla ──────────────────────────────────────────────────────
    if not conectado or df_estado.empty:
        st.info("No hay registros de carga en los últimos 7 días.")
    else:
        seccion_tabla_con_guardar(
            df_estado,
            key="inicio_estado",
            titulo="📋 Estado de la última carga por tabla",
            page_size=10,
            caption="Últimos 7 días · actualización cada 60 s.",
            mostrar_boton_guardar=False,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Log historial ─────────────────────────────────────────────────────────
    with st.expander("📜 Log de cargas (solo lectura)", expanded=False):
        if not conectado:
            st.warning("Sin conexión a la base de datos.")
        else:
            df_log = _cargar_log_reciente()
            if df_log.empty:
                st.write("El historial de cargas está vacío.")
            else:
                seccion_tabla_con_guardar(
                    df_log,
                    key="inicio_log",
                    titulo="",
                    page_size=10,
                    caption="",
                    mostrar_boton_guardar=False,
                )

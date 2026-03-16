"""
inicio.py — Página de inicio del portal MDM.
Dashboard con métricas del día, estado por tabla, alertas y log de cargas.
"""
import streamlit as st
import pandas as pd
from utils.formato import header_pagina, colorear_estado, crear_tarjeta_kpi

# DataFrames vacíos como placeholders para futura conexión a BD
ESTADO_TABLAS = pd.DataFrame(columns=["Tabla", "Última carga", "Filas insertadas", "Filas rechazadas", "Estado"])
LOG_CARGAS = pd.DataFrame(columns=["Fecha", "Tablas procesadas", "Total filas", "Rechazadas", "Resultado"])

def render():
    header_pagina("🏠", "Inicio", "Estado del pipeline · Conexión a Base de Datos pendiente")

    # ── Métricas principales (Premium HTML) ──────────────────────────────
    filas_ok_txt = "0"
    en_cuarentena_txt = "0"
    pendientes_txt = "0"

    html_kpis = f"""<div class="kpi-container" style="margin-bottom: 32px;">
{crear_tarjeta_kpi("Última carga", "N/A", "⏱️", "")}
{crear_tarjeta_kpi("Filas OK", filas_ok_txt, "✅", "success")}
{crear_tarjeta_kpi("En cuarentena", en_cuarentena_txt, "🔴", "danger")}
{crear_tarjeta_kpi("Homologaciones", pendientes_txt, "🔗", "warning")}
</div>"""
    st.markdown(html_kpis, unsafe_allow_html=True)

    # ── Distribución de Ingesta (CSS Nativo, Cero Lag) ────────────────────
    st.markdown("### 📊 Distribución de Ingesta")
    
    html_dist = """<div style="background: var(--secondary-background-color); border: 1px solid rgba(128,128,128,0.2); border-radius: 12px; padding: 24px; box-shadow: var(--shadow-sm); margin-bottom: 32px;">
<div style="display:flex; justify-content:space-between; margin-bottom:8px;">
<span style="font-size:0.9rem; font-weight:600; color:#43843C;">Insertadas OK (0%)</span>
<span style="font-size:0.9rem; font-weight:600; color:#D35D47;">Rechazadas (0%)</span>
</div>
<div style="width:100%; height:24px; background-color:var(--background-color); border-radius:12px; overflow:hidden; display:flex; border: 1px solid rgba(128,128,128,0.3);">
<div style="width:0%; background:linear-gradient(90deg, #2D5A27, #43843C); transition:width 1s;"></div>
<div style="width:0%; background:#D35D47; transition:width 1s;"></div>
</div>
<p style="text-align:center; font-size:0.8rem; color:var(--text-color); opacity:0.6; margin:12px 0 0 0;">Sin datos recientes para distribuir.</p>
</div>"""
    st.markdown(html_dist, unsafe_allow_html=True)

    # ── Carga Manual y Ejecución ETL ──────────────────────────────────────
    import os
    import sys
    import subprocess
    st.markdown("### ⚙️ Centro de Comando MDM")
    
    with st.container(border=True):
        st.info("💡 **Operación completa:** 1. Sube tu reporte crudo aquí. 2. Presiona 'Ejecutar' para procesarlo automáticamente en el Data Warehouse.", icon="ℹ️")
        
        c_up, c_run = st.columns([1.5, 1], gap="large")
        
        with c_up:
            archivo_subido = st.file_uploader(
                "📂 1. Seleccionar reporte Excel (.xlsx, .xls) a encolar", 
                type=["xlsx", "xls"],
                key="uploader_inicio",
                label_visibility="visible"
            )
            
            if archivo_subido is not None:
                directorio_raiz = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                directorio_destino = os.path.join(directorio_raiz, "ETL", "data", "inbound")
                os.makedirs(directorio_destino, exist_ok=True)
                ruta_archivo = os.path.join(directorio_destino, archivo_subido.name)
                
                with open(ruta_archivo, "wb") as f:
                    f.write(archivo_subido.getbuffer())
                st.success(f"Archivo guardado en cola: `{archivo_subido.name}`")
        
        with c_run:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🚀 2. Ejecutar Pipeline ETL ahora", use_container_width=True, type="primary"):
                directorio_raiz = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                ruta_pipeline = os.path.join(directorio_raiz, "ETL", "pipeline.py")
                
                if not os.path.exists(ruta_pipeline):
                    st.error(f"No se encontró el motor ETL en la ruta:\n`{ruta_pipeline}`")
                else:
                    with st.status("🛠️ Ejecutando Pipeline ETL...", expanded=True) as status:
                        st.write("Iniciando motor Python externo...")
                        try:
                            # Ejecutar el script usando el mismo intérprete de Python
                            proceso = subprocess.Popen(
                                [sys.executable, ruta_pipeline],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                text=True,
                                cwd=os.path.join(directorio_raiz, "ETL")
                            )
                            
                            # Imprimir log remoto en vivo
                            for linea in iter(proceso.stdout.readline, ''):
                                if linea:
                                    st.text(linea.strip())
                            
                            proceso.stdout.close()
                            codigo_retorno = proceso.wait()
                            
                            if codigo_retorno == 0:
                                status.update(label="✅ Pipeline completado exitosamente", state="complete")
                                st.toast("Carga al Data Warehouse completada.", icon="🎉")
                                st.balloons()
                            else:
                                status.update(label=f"❌ Falla de ejecución (Código {codigo_retorno})", state="error")
                                st.error("El pipeline falló durante el procesamiento. Revisa el log detallado arriba.")
                        except Exception as e:
                            status.update(label="❌ Error crítico intentando arrancar el proceso", state="error")
                            st.error(f"Excepción: {e}")

    st.markdown("<hr style='margin: 32px 0;'>", unsafe_allow_html=True)

    # ── Estado por tabla ──────────────────────────────────────────────────
    st.markdown("### 📊 Estado de la última carga por tabla")
    if ESTADO_TABLAS.empty:
        st.write("No hay datos de carga recientes.")
    else:
        st.dataframe(
            ESTADO_TABLAS.style
                .map(colorear_estado, subset=["Estado"])
                .format({"Filas insertadas": "{:,}", "Filas rechazadas": "{:,}"}),
            width="stretch",
            hide_index=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Log últimas 10 cargas ─────────────────────────────────────────────
    with st.expander("📋 Log de cargas (solo lectura)", expanded=False):
        if LOG_CARGAS.empty:
            st.write("El historial de cargas está vacío.")
        else:
            st.dataframe(
                LOG_CARGAS.style
                    .map(colorear_estado, subset=["Resultado"])
                    .format({"Total filas": "{:,}", "Rechazadas": "{:,}"}),
                width="stretch",
                hide_index=True,
            )

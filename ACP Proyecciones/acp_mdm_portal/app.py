"""
app.py — Portal MDM ACP Geographic Phenology
Punto de entrada principal. Ejecutar con: streamlit run app.py
"""
import streamlit as st
from utils.formato import aplicar_css

st.set_page_config(
    page_title="ACP MDM Portal",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

aplicar_css()

# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div class="sidebar-logo">
            <div style="font-size:2.4rem; filter: drop-shadow(0 4px 6px rgba(45,90,39,0.3)); margin-bottom: 6px;">🌿</div>
            <h2>ACP MDM</h2>
            <p style="font-weight: 500; letter-spacing: 0.3px;">Data Quality Premium</p>
        </div>
    """, unsafe_allow_html=True)

    pagina = st.radio(
        "Navegación",
        options=[
            "Inicio",
            "Cuarentena",
            "Homologación",
            "Variedades",
            "Geografía",
            "Personal",
            "Reglas de Validación",
            "Parámetros Pipeline"
        ],
        label_visibility="collapsed",
    )

    st.markdown("""
        <div class="sidebar-footer">
            ACP Equipo de Proyecciones · 2026
        </div>
    """, unsafe_allow_html=True)

# ── Enrutamiento ──────────────────────────────────────────────────────────
def header_pagina(emoji, titulo, subtitulo=None):
    st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 24px;">
            <div style="font-size: 3.5rem; filter: drop-shadow(0 4px 6px rgba(45,90,39,0.3));">{emoji}</div>
            <div>
                <h1 style="margin: 0; padding: 0; font-size: 2.5rem; line-height: 1.2;">{titulo}</h1>
                {"<p style='margin: 0; padding: 0; font-size: 1rem; color: #666;'>"+subtitulo+"</p>" if subtitulo else ""}
            </div>
        </div>
    """, unsafe_allow_html=True)

if pagina == "Inicio":
    from paginas.inicio import render
elif pagina == "Cuarentena":
    from paginas.cuarentena import render
elif pagina == "Homologación":
    from paginas.homologacion import render
elif pagina == "Variedades":
    from paginas.catalogos.variedades import render
elif pagina == "Geografía":
    from paginas.catalogos.geografia import render
elif pagina == "Personal":
    from paginas.catalogos.personal import render
elif pagina == "Reglas de Validación":
    from paginas.configuracion.reglas_validacion import render
elif pagina == "Parámetros Pipeline":
    from paginas.configuracion.parametros_pipeline import render
else:
    from paginas.inicio import render

render()
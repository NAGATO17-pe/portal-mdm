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

# Limpiar cache al cambiar de página para evitar "state leaks"
if "current_page" not in st.session_state:
    st.session_state.current_page = pagina

if st.session_state.current_page != pagina:
    st.cache_data.clear()
    st.session_state.current_page = pagina

# Enrutamiento dinámico
if pagina == "Inicio":
    import paginas.inicio as pg
elif pagina == "Cuarentena":
    import paginas.cuarentena as pg
elif pagina == "Homologación":
    import paginas.homologacion as pg
elif pagina == "Variedades":
    import paginas.catalogos.variedades as pg
elif pagina == "Geografía":
    import paginas.catalogos.geografia as pg
elif pagina == "Personal":
    import paginas.catalogos.personal as pg
elif pagina == "Reglas de Validación":
    import paginas.configuracion.reglas_validacion as pg
elif pagina == "Parámetros Pipeline":
    import paginas.configuracion.parametros_pipeline as pg
else:
    import paginas.inicio as pg

pg.render()
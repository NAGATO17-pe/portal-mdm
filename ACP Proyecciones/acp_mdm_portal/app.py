"""
app.py — Portal MDM ACP Geographic Phenology (Enterprise)
Punto de entrada principal. Ejecutar con: streamlit run app.py
"""
import importlib

import streamlit as st

from utils.auth import cerrar_sesion, login_gate, obtener_usuario, tiene_permiso
from utils.formato import aplicar_css

st.set_page_config(
    page_title="ACP MDM Portal",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Login Gate ────────────────────────────────────────────────────────────────
# Si no está autenticado, muestra la pantalla de login y detiene la ejecución.
if not login_gate():
    st.stop()

# ── A partir de aquí, el usuario está autenticado ────────────────────────────
aplicar_css()

usuario = obtener_usuario()
rol_badge = {"admin": "🔑 Admin", "editor": "✏️ Editor", "viewer": "👁️ Viewer"}

# ── Sidebar con secciones agrupadas ───────────────────────────────────────
with st.sidebar:
    # ── Header: Logo + info de usuario ──
    st.markdown(f"""
        <div class="sidebar-logo">
            <div style="font-size:2.4rem; filter: drop-shadow(0 4px 6px rgba(45,90,39,0.3));
                        margin-bottom: 6px;">🌿</div>
            <h2>ACP MDM</h2>
            <p>Data Quality · Enterprise</p>
        </div>
    """, unsafe_allow_html=True)

    # ── User card ──
    st.markdown(f"""
        <div style="
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 12px;
            padding: 14px 16px;
            margin: 0 8px 12px 8px;
            display: flex;
            align-items: center;
            gap: 12px;
        ">
            <div style="font-size: 1.6rem; line-height: 1;">{usuario['avatar']}</div>
            <div>
                <div style="font-size: 0.88rem; font-weight: 600; color: #ffffff;
                            line-height: 1.3;">{usuario['nombre']}</div>
                <div style="font-size: 0.72rem; color: rgba(255,255,255,0.45);
                            margin-top: 2px;">{rol_badge.get(usuario['rol'], usuario['rol'])}</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    # ── Sección: Principal ──
    st.markdown('<div class="sidebar-section">Principal</div>', unsafe_allow_html=True)
    pagina = st.radio(
        "Navegación",
        options=[
            "🏠  Inicio",
            "🔴  Cuarentena",
            "🔗  Homologación",
        ],
        label_visibility="collapsed",
        key="nav_principal",
    )

    # ── Sección: Catálogos ──
    st.markdown('<div class="sidebar-section">Catálogos</div>', unsafe_allow_html=True)
    pagina_cat = st.radio(
        "Catálogos",
        options=[
            "🍇  Variedades",
            "📍  Geografía",
            "👤  Personal",
        ],
        label_visibility="collapsed",
        key="nav_catalogos",
    )

    # ── Sección: Configuración (solo admin/editor) ──
    pagina_cfg = None
    if tiene_permiso("configurar") or tiene_permiso("escribir"):
        st.markdown('<div class="sidebar-section">Configuración</div>', unsafe_allow_html=True)
        pagina_cfg = st.radio(
            "Configuración",
            options=[
                "📋  Reglas de Validación",
                "⚙️  Parámetros Pipeline",
                "🛠️  Pruebas BD",
            ],
            label_visibility="collapsed",
            key="nav_config",
        )

    # ── Botón de logout ──
    st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)
    if st.button("🚪 Cerrar sesión", key="btn_logout", use_container_width=True):
        cerrar_sesion()
        st.rerun()

    st.markdown("""
        <div class="sidebar-footer">
            ACP Equipo de Proyecciones · 2026
        </div>
    """, unsafe_allow_html=True)

# ── Consolidar la selección activa ────────────────────────────────────────
_SECCIONES = {
    "nav_principal": pagina,
    "nav_catalogos": pagina_cat,
}
if pagina_cfg is not None:
    _SECCIONES["nav_config"] = pagina_cfg

if "ultima_seccion" not in st.session_state:
    st.session_state.ultima_seccion = "nav_principal"
    st.session_state.ultima_pagina  = pagina

# Detectar cuál cambió
for sec_key, sec_val in _SECCIONES.items():
    if sec_val is None:
        continue
    prev_key = f"_prev_{sec_key}"
    if prev_key not in st.session_state:
        st.session_state[prev_key] = sec_val
    if st.session_state[prev_key] != sec_val:
        st.session_state.ultima_seccion = sec_key
        st.session_state.ultima_pagina  = sec_val
        st.session_state[prev_key]      = sec_val

pagina_activa = st.session_state.ultima_pagina

# ── Limpiar cache al cambiar de página para evitar state leaks ────────────
if "current_page" not in st.session_state:
    st.session_state.current_page = pagina_activa

if st.session_state.current_page != pagina_activa:
    st.cache_data.clear()
    st.session_state.current_page = pagina_activa

# ── Enrutamiento dinámico ─────────────────────────────────────────────────
_nombre = pagina_activa.split("  ", 1)[-1].strip() if "  " in pagina_activa else pagina_activa.strip()

_RUTAS = {
    "Inicio":              "paginas.inicio",
    "Cuarentena":          "paginas.cuarentena",
    "Homologación":        "paginas.homologacion",
    "Variedades":          "paginas.catalogos.variedades",
    "Geografía":           "paginas.catalogos.geografia",
    "Personal":            "paginas.catalogos.personal",
    "Reglas de Validación": "paginas.configuracion.reglas_validacion",
    "Parámetros Pipeline": "paginas.configuracion.parametros_pipeline",
    "Pruebas BD":          "paginas.configuracion.pruebas_bd",
}

modulo = _RUTAS.get(_nombre, "paginas.inicio")
pg = importlib.import_module(modulo)
pg.render()
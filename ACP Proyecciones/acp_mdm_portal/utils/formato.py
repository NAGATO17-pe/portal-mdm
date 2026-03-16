"""
formato.py — CSS personalizado y helpers de formato para el portal ACP.
"""
import streamlit as st

# ── Paleta de colores ACP ─────────────────────────────────────────────────
VERDE_ACP    = "#1E6B35"
VERDE_CLARO  = "#2E8B57"
BRONCE       = "#CD7F32"
BRONCE_CLARO = "#E8A44A"
ROJO         = "#C0392B"
AMARILLO     = "#E67E22"
GRIS_FONDO   = "#F4F6F4"
GRIS_BORDE   = "#D5E0D8"

CSS_PORTAL = f"""
<style>
/* ── Fuente ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] {{
    font-family: 'Inter', sans-serif;
}}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, {VERDE_ACP} 0%, #154d26 100%);
    border-right: 2px solid {BRONCE};
}}
section[data-testid="stSidebar"] .stRadio label {{
    color: #ffffff !important;
    font-size: 0.9rem;
    padding: 6px 4px;
}}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] {{
    gap: 4px;
}}
/* Logo / título en sidebar */
.sidebar-logo {{
    text-align: center;
    padding: 16px 8px;
    border-bottom: 1px solid rgba(205,127,50,0.5);
    margin-bottom: 16px;
}}
.sidebar-logo h2 {{
    color: {BRONCE_CLARO};
    font-size: 1.05rem;
    font-weight: 700;
    margin: 4px 0 0 0;
    letter-spacing: 0.5px;
}}
.sidebar-logo p {{
    color: rgba(255,255,255,0.65);
    font-size: 0.72rem;
    margin: 2px 0 0 0;
}}

/* ── Encabezado de página ── */
.page-header {{
    background: linear-gradient(90deg, {VERDE_ACP} 0%, {VERDE_CLARO} 100%);
    color: white;
    padding: 18px 24px;
    border-radius: 10px;
    margin-bottom: 20px;
    border-left: 5px solid {BRONCE};
}}
.page-header h1 {{
    margin: 0;
    font-size: 1.5rem;
    font-weight: 700;
}}
.page-header p {{
    margin: 4px 0 0 0;
    font-size: 0.85rem;
    opacity: 0.85;
}}

/* ── Tarjeta de sección ── */
.card {{
    background: white;
    border: 1px solid {GRIS_BORDE};
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 14px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}}
.card h3 {{
    color: {VERDE_ACP};
    font-size: 1rem;
    font-weight: 600;
    margin: 0 0 12px 0;
    border-bottom: 2px solid {GRIS_BORDE};
    padding-bottom: 6px;
}}

/* ── Badges de severidad ── */
.badge-critico {{
    background: #FDEDEC; color: {ROJO};
    padding: 2px 8px; border-radius: 12px;
    font-size: 0.78rem; font-weight: 600;
}}
.badge-alto {{
    background: #FEF9E7; color: {AMARILLO};
    padding: 2px 8px; border-radius: 12px;
    font-size: 0.78rem; font-weight: 600;
}}
.badge-medio {{
    background: #EAF4EC; color: {VERDE_ACP};
    padding: 2px 8px; border-radius: 12px;
    font-size: 0.78rem; font-weight: 600;
}}

/* ── Panel decisión ── */
.decision-panel {{
    background: linear-gradient(135deg, #F8FAF8 0%, #EFF5F0 100%);
    border: 1.5px solid {VERDE_ACP};
    border-radius: 12px;
    padding: 20px 24px;
    margin-top: 12px;
}}
.decision-panel h4 {{
    color: {VERDE_ACP};
    margin: 0 0 12px 0;
    font-size: 1.05rem;
}}
.decision-info {{
    background: white;
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 14px;
    border-left: 4px solid {BRONCE};
    font-size: 0.88rem;
}}

/* ── Botones de acción ── */
.stButton > button {{
    border-radius: 6px;
    font-weight: 500;
    transition: all 0.2s ease;
}}
.stButton > button:hover {{
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
}}

/* ── Metric cards extra ── */
div[data-testid="metric-container"] {{
    background: white;
    border: 1px solid {GRIS_BORDE};
    border-radius: 10px;
    padding: 12px 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}}
div[data-testid="metric-container"] label {{
    color: #555;
    font-size: 0.8rem;
}}

/* ── Banner aviso ── */
.banner-aviso {{
    background: #FFF8E1;
    border: 1px solid #FFD54F;
    border-left: 4px solid {AMARILLO};
    border-radius: 8px;
    padding: 10px 16px;
    font-size: 0.85rem;
    color: #5D4037;
    margin-bottom: 12px;
}}

/* ── Footer sidebar ── */
.sidebar-footer {{
    position: fixed;
    bottom: 12px;
    left: 0;
    width: 260px;
    text-align: center;
    color: rgba(255,255,255,0.4);
    font-size: 0.68rem;
}}
</style>
"""

def aplicar_css():
    st.markdown(CSS_PORTAL, unsafe_allow_html=True)


def header_pagina(icono: str, titulo: str, descripcion: str):
    st.markdown(f"""
        <div class="page-header">
            <h1>{icono} {titulo}</h1>
            <p>{descripcion}</p>
        </div>
    """, unsafe_allow_html=True)


def card_inicio(titulo: str):
    st.markdown(f'<div class="card"><h3>{titulo}</h3>', unsafe_allow_html=True)


def card_fin():
    st.markdown('</div>', unsafe_allow_html=True)


def colorear_estado(val):
    colores = {
        "✅ OK":        "background-color:#EAF4EC; color:#1E6B35; font-weight:600",
        "⚠️ Con errores": "background-color:#FEF9E7; color:#E67E22; font-weight:600",
        "❌ Falló":     "background-color:#FDEDEC; color:#C0392B; font-weight:600",
        "Pendiente":    "background-color:#EEF2FF; color:#3949AB; font-weight:600",
    }
    return colores.get(val, "")


def colorear_severidad(val):
    colores = {
        "CRÍTICO": "background-color:#FDEDEC; color:#C0392B; font-weight:bold",
        "ALTO":    "background-color:#FEF9E7; color:#E67E22; font-weight:bold",
        "MEDIO":   "background-color:#EAF4EC; color:#1E6B35; font-weight:bold",
    }
    return colores.get(val, "")


def score_a_color(score: float) -> str:
    if score >= 0.85:
        return "🟢"
    elif score >= 0.70:
        return "🟡"
    return "🔴"

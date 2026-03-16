import streamlit as st

# ── Organic Premium Palette (Theme Aware) ──
VERDE_ACP    = "#2D5A27" # Forest Green variant
VERDE_CLARO  = "#43843C" 
BRONCE       = "#C38D4F" # Soft Copper

CSS_PORTAL = f"""
<style>
/* ── Variables & Typography ── */
:root {{
    --verde-acp: {VERDE_ACP};
    --verde-claro: {VERDE_CLARO};
    --bronce: {BRONCE};
    --shadow-sm: 0 2px 8px rgba(0,0,0,0.08);
    --shadow-md: 0 4px 16px rgba(0,0,0,0.12);
}}

/* System font stack favoring clean rendering */
html, body, [class*="css"] {{
    font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    /* Removed hardcoded color, trust Streamlit var(--text-color) */
}}

/* Fast smooth transitions globally */
* {{
    transition: background-color 0.15s ease, border-color 0.15s ease, box-shadow 0.15s ease, transform 0.1s ease;
}}

/* ── App Background ── */
.stApp {{
    /* Let Streamlit handle base background color */
    background-image: radial-gradient(circle at top right, rgba(195, 141, 79, 0.08), transparent 40%),
                      radial-gradient(circle at bottom left, rgba(45, 90, 39, 0.08), transparent 40%);
    background-attachment: fixed;
}}

/* ── Sidebar Glassmorphism & Fixed Dark Theme ── */
section[data-testid="stSidebar"] {{
    background-color: transparent !important; 
    border-right: 1px solid rgba(128, 128, 128, 0.2);
}}
/* Force the Dark Green Gradient ALWAYS, even in light mode */
section[data-testid="stSidebar"] > div:first-child {{
    background: linear-gradient(180deg, rgba(45,90,39,1) 0%, rgba(20,50,25,1) 100%) !important;
}}
/* Force all text inside sidebar to be white */
section[data-testid="stSidebar"] * {{
    color: #ffffff !important;
}}
section[data-testid="stSidebar"] .stRadio p,
section[data-testid="stSidebar"] .stRadio span,
section[data-testid="stSidebar"] .stRadio div[data-testid="stMarkdownContainer"] {{
    color: #ffffff !important;
    font-size: 0.95rem;
    font-weight: 500;
}}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label {{
    padding: 8px 12px;
    border-radius: 8px;
    margin-bottom: 2px;
    background: transparent;
    cursor: pointer;
}}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:hover {{
    background: rgba(255,255,255,0.1) !important;
}}

/* Logo / título en sidebar (force light text because sidebar bg is dark green) */
.sidebar-logo {{
    text-align: center;
    padding: 24px 8px;
    border-bottom: 1px solid rgba(255,255,255,0.1);
    margin-bottom: 16px;
}}
.sidebar-logo h2, .sidebar-logo p {{
    color: #ffffff;
}}
.sidebar-logo h2 {{
    color: var(--bronce);
    font-size: 1.15rem;
    font-weight: 700;
    margin: 8px 0 0 0;
    letter-spacing: 0.5px;
}}

/* ── Encabezado de página Premium ── */
.page-header {{
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.2);
    border-left: 6px solid var(--verde-acp);
    padding: 24px 30px;
    border-radius: 16px;
    margin-bottom: 24px;
    box-shadow: var(--shadow-md);
}}
.page-header h1 {{
    margin: 0;
    font-size: 1.75rem;
    font-weight: 700;
    color: var(--verde-acp);
    letter-spacing: -0.5px;
}}
.page-header p {{
    margin: 4px 0 0 0;
    font-size: 0.95rem;
    color: var(--text-color);
    opacity: 0.8;
}}

/* ── Dataframes & Metrics ── */
div[data-testid="stDataFrame"] {{
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid rgba(128,128,128,0.2);
    box-shadow: var(--shadow-sm);
}}

/* Custom HTML KPI Cards */
.kpi-container {{
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
}}
.kpi-card {{
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.2);
    border-radius: 16px;
    padding: 20px 24px;
    flex: 1;
    min-width: 200px;
    box-shadow: var(--shadow-sm);
    display: flex;
    align-items: center;
    gap: 16px;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    border-top: 4px solid var(--bronce);
}}
.kpi-card:hover {{
    transform: translateY(-4px);
    box-shadow: var(--shadow-md);
}}
.kpi-icon {{
    font-size: 2.5rem;
    color: var(--bronce);
    opacity: 0.9;
    flex-shrink: 0;
}}
.kpi-content {{
    display: flex;
    flex-direction: column;
}}
.kpi-title {{
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-color);
    opacity: 0.7;
    margin-bottom: 4px;
    font-weight: 600;
}}
.kpi-value {{
    font-size: 1.8rem;
    font-weight: 700;
    color: var(--verde-acp);
    line-height: 1;
}}
.kpi-card.success {{ border-top-color: var(--verde-acp); }}
.kpi-card.success .kpi-icon, .kpi-card.success .kpi-value {{ color: var(--verde-acp); }}
.kpi-card.warning {{ border-top-color: #E29D45; }}
.kpi-card.warning .kpi-icon, .kpi-card.warning .kpi-value {{ color: #E29D45; }}
.kpi-card.danger {{ border-top-color: #D35D47; }}
.kpi-card.danger .kpi-icon, .kpi-card.danger .kpi-value {{ color: #D35D47; }}


/* ── Panel decisión ── */
.decision-panel {{
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.2);
    border-left: 4px solid var(--verde-claro);
    border-radius: 12px;
    padding: 20px 24px;
    margin-top: 12px;
    box-shadow: var(--shadow-sm);
}}
.decision-panel h4 {{
    color: var(--verde-acp);
    margin: 0 0 12px 0;
    font-size: 1.1rem;
}}
.decision-info {{
    background: var(--background-color);
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 14px;
    border: 1px solid rgba(128,128,128,0.3);
    font-size: 0.9rem;
}}

/* ── Botones Premium ── */
.stButton > button {{
    border-radius: 8px;
    font-weight: 600;
    border: 1px solid transparent;
    box-shadow: var(--shadow-sm);
}}
.stButton > button[kind="primary"] {{
    background: linear-gradient(135deg, var(--verde-acp) 0%, var(--verde-claro) 100%);
    color: white;
    border: none;
}}
.stButton > button[kind="primary"]:hover {{
    background: linear-gradient(135deg, var(--verde-claro) 0%, var(--verde-acp) 100%);
    transform: translateY(-2px);
    box-shadow: 0 6px 14px rgba(45,90,39,0.25);
    border: none;
}}
.stButton > button[kind="secondary"]:hover {{
    transform: translateY(-1px);
    border-color: var(--verde-claro);
    color: var(--verde-acp);
}}

/* ── Banner aviso ── */
.banner-aviso {{
    background: var(--secondary-background-color);
    border: 1px solid var(--bronce);
    border-left: 4px solid var(--bronce);
    border-radius: 8px;
    padding: 12px 20px;
    font-size: 0.9rem;
    color: var(--text-color);
    margin-bottom: 16px;
    box-shadow: var(--shadow-sm);
}}
</style>
"""

@st.cache_data
def obtener_css() -> str:
    return CSS_PORTAL

def aplicar_css():
    st.markdown(obtener_css(), unsafe_allow_html=True)

def header_pagina(icono: str, titulo: str, descripcion: str):
    st.markdown(f"""
        <div class="page-header">
            <h1>{icono} {titulo}</h1>
            <p>{descripcion}</p>
        </div>
    """, unsafe_allow_html=True)

def colorear_estado(val):
    # Utilizamos rgba(..., 0.15) para el fondo, de modo que se vea bien tanto en modo oscuro como claro
    colores = {
        "✅ OK":        "background-color:rgba(46, 139, 87, 0.15); color:#2E8B57; font-weight:600",
        "⚠️ Con errores": "background-color:rgba(230, 126, 34, 0.15); color:#E67E22; font-weight:600",
        "❌ Falló":     "background-color:rgba(192, 57, 43, 0.15); color:#C0392B; font-weight:600",
        "Pendiente":    "background-color:rgba(57, 73, 171, 0.15); color:#3949AB; font-weight:600",
    }
    return colores.get(val, "")

def colorear_severidad(val):
    colores = {
        "CRÍTICO": "background-color:rgba(192, 57, 43, 0.15); color:#C0392B; font-weight:bold",
        "ALTO":    "background-color:rgba(230, 126, 34, 0.15); color:#E67E22; font-weight:bold",
        "MEDIO":   "background-color:rgba(46, 139, 87, 0.15); color:#2E8B57; font-weight:bold",
    }
    return colores.get(val, "")

def score_a_color(score: float) -> str:
    if score >= 0.85:
        return "🟢"
    elif score >= 0.70:
        return "🟡"
    return "🔴"

def crear_tarjeta_kpi(titulo: str, valor: str, icono: str, tipo: str = "") -> str:
    """
    Renderiza una tarjeta HTML personalizada para métricas.
    tipos esperados: "", "success", "warning", "danger"
    """
    clase_tipo = f" {tipo}" if tipo else ""
    return f"""<div class="kpi-card{clase_tipo}">
<div class="kpi-icon">{icono}</div>
<div class="kpi-content">
<div class="kpi-title">{titulo}</div>
<div class="kpi-value">{valor}</div>
</div>
</div>"""

@st.cache_data
def load_lottieurl(url: str):
    import requests
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

@st.cache_data
def load_lottieurl(url: str):
    import requests
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()


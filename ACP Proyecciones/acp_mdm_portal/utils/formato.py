"""
utils/formato.py — Sistema de diseño visual del Portal MDM ACP (Enterprise)
=============================================================================
CSS Premium + Componentes de renderizado HTML.

Cambios v2 Enterprise:
  - Google Fonts Inter via @import (tipografía profesional)
  - Glassmorphism refinado en sidebar y paneles
  - Micro-animaciones @keyframes (fadeIn, slideUp, pulse)
  - Navegación sidebar con secciones agrupadas visualmente
  - renderizar_tabla_premium_raw() — tabla sin paginación propia (para SQL paginado)
  - Eliminada función load_lottieurl (dependencia removida)
"""

import math

import streamlit as st

# ── Organic Premium Palette (Theme Aware) ──
VERDE_ACP   = "#2D5A27"
VERDE_CLARO = "#43843C"
BRONCE      = "#C38D4F"

CSS_PORTAL = f"""
<style>
/* ── Google Fonts ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Variables & Typography ── */
:root {{
    --verde-acp: {VERDE_ACP};
    --verde-claro: {VERDE_CLARO};
    --bronce: {BRONCE};
    --shadow-sm: 0 2px 8px rgba(0,0,0,0.06);
    --shadow-md: 0 6px 20px rgba(0,0,0,0.10);
    --shadow-lg: 0 12px 40px rgba(0,0,0,0.14);
    --radius-sm: 8px;
    --radius-md: 12px;
    --radius-lg: 16px;
    --radius-xl: 20px;
}}

html, body, [class*="css"] {{
    font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
}}

/* ── Micro-animations ── */
@keyframes fadeIn {{
    from {{ opacity: 0; transform: translateY(8px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}
@keyframes slideUp {{
    from {{ opacity: 0; transform: translateY(16px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}
@keyframes pulseGlow {{
    0%, 100% {{ box-shadow: 0 0 0 0 rgba(45,90,39,0.15); }}
    50% {{ box-shadow: 0 0 12px 4px rgba(45,90,39,0.12); }}
}}

/* Global smooth transitions */
* {{
    transition: background-color 0.2s ease, border-color 0.2s ease,
                box-shadow 0.2s ease, transform 0.15s ease, opacity 0.2s ease;
}}

/* ── App Background ── */
.stApp {{
    background-image:
        radial-gradient(ellipse at 10% 0%, rgba(45,90,39,0.06), transparent 50%),
        radial-gradient(ellipse at 90% 100%, rgba(195,141,79,0.06), transparent 50%);
    background-attachment: fixed;
}}

/* ── Sidebar — Dark Green Glassmorphism ── */
section[data-testid="stSidebar"] {{
    background-color: transparent !important;
    border-right: 1px solid rgba(128,128,128,0.15);
}}
section[data-testid="stSidebar"] > div:first-child {{
    background: linear-gradient(180deg, rgba(45,90,39,0.98) 0%, rgba(18,42,22,0.99) 100%) !important;
    backdrop-filter: blur(12px);
}}
section[data-testid="stSidebar"] * {{
    color: #ffffff !important;
}}

/* Sidebar nav items — hover glow */
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label {{
    padding: 10px 14px;
    border-radius: var(--radius-sm);
    margin-bottom: 2px;
    background: transparent;
    cursor: pointer;
    border-left: 3px solid transparent;
}}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:hover {{
    background: rgba(255,255,255,0.08) !important;
    border-left-color: var(--bronce);
}}
section[data-testid="stSidebar"] .stRadio p,
section[data-testid="stSidebar"] .stRadio span,
section[data-testid="stSidebar"] .stRadio div[data-testid="stMarkdownContainer"] {{
    color: #ffffff !important;
    font-size: 0.93rem;
    font-weight: 500;
    letter-spacing: 0.2px;
}}

/* Sidebar section dividers */
.sidebar-section {{
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: rgba(255,255,255,0.35) !important;
    padding: 16px 14px 4px 14px;
    font-weight: 700;
}}

/* Logo */
.sidebar-logo {{
    text-align: center;
    padding: 28px 12px 20px 12px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
    margin-bottom: 8px;
}}
.sidebar-logo h2, .sidebar-logo p {{ color: #ffffff; }}
.sidebar-logo h2 {{
    color: var(--bronce);
    font-size: 1.2rem;
    font-weight: 700;
    margin: 10px 0 2px 0;
    letter-spacing: 0.5px;
}}
.sidebar-logo p {{
    font-size: 0.78rem;
    opacity: 0.6;
    margin: 0;
}}

/* Sidebar footer */
.sidebar-footer {{
    text-align: center;
    padding: 20px 12px;
    border-top: 1px solid rgba(255,255,255,0.06);
    margin-top: 20px;
    font-size: 0.72rem;
    opacity: 0.4;
    letter-spacing: 0.3px;
}}

/* ── Page Header Premium ── */
.page-header {{
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.15);
    border-left: 6px solid var(--verde-acp);
    padding: 26px 32px;
    border-radius: var(--radius-lg);
    margin-bottom: 28px;
    box-shadow: var(--shadow-md);
    animation: fadeIn 0.4s ease;
}}
.page-header h1 {{
    margin: 0;
    font-size: 1.65rem;
    font-weight: 700;
    color: var(--verde-acp);
    letter-spacing: -0.5px;
}}
.page-header p {{
    margin: 6px 0 0 0;
    font-size: 0.92rem;
    color: var(--text-color);
    opacity: 0.7;
}}

/* ── Dataframes (nativo Streamlit) ── */
div[data-testid="stDataFrame"] {{
    border-radius: var(--radius-md);
    overflow: hidden;
    border: 1px solid rgba(128,128,128,0.15);
    box-shadow: var(--shadow-sm);
}}

/* ── KPI Cards ── */
.kpi-container {{
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
    animation: slideUp 0.5s ease;
}}
.kpi-card {{
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.15);
    border-radius: var(--radius-lg);
    padding: 24px;
    flex: 1;
    min-width: 200px;
    box-shadow: var(--shadow-sm);
    display: flex;
    flex-direction: column;
    justify-content: center;
    position: relative;
    overflow: hidden;
    border-top: 4px solid var(--bronce);
}}
.kpi-card:hover {{
    transform: translateY(-4px);
    box-shadow: var(--shadow-md);
    border-top-color: var(--verde-acp);
}}
.kpi-icon {{
    position: absolute;
    right: -8px;
    bottom: -8px;
    font-size: 4.5rem;
    opacity: 0.04;
    transform: rotate(-12deg);
    pointer-events: none;
}}
.kpi-content {{ display: flex; flex-direction: column; }}
.kpi-title {{
    font-size: 0.82rem;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    color: var(--text-color);
    opacity: 0.6;
    margin-bottom: 6px;
    font-weight: 600;
}}
.kpi-value {{
    font-size: 1.75rem;
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

/* ── Decision Panel (cuarentena) ── */
.decision-panel {{
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.15);
    border-left: 4px solid var(--verde-claro);
    border-radius: var(--radius-md);
    padding: 20px 24px;
    margin-top: 12px;
    box-shadow: var(--shadow-sm);
    animation: fadeIn 0.3s ease;
}}
.decision-panel h4 {{ color: var(--verde-acp); margin: 0 0 12px 0; font-size: 1.05rem; }}
.decision-info {{
    background: var(--background-color);
    border-radius: var(--radius-sm);
    padding: 12px 16px;
    margin-bottom: 14px;
    border: 1px solid rgba(128,128,128,0.2);
    font-size: 0.88rem;
}}

/* ── Buttons Premium ── */
.stButton > button {{
    border-radius: var(--radius-sm);
    font-weight: 600;
    font-size: 0.88rem;
    border: 1px solid transparent;
    box-shadow: var(--shadow-sm);
    letter-spacing: 0.2px;
}}
.stButton > button[kind="primary"] {{
    background: linear-gradient(135deg, var(--verde-acp) 0%, var(--verde-claro) 100%);
    color: white;
    border: none;
}}
.stButton > button[kind="primary"]:hover {{
    background: linear-gradient(135deg, var(--verde-claro) 0%, var(--verde-acp) 100%);
    transform: translateY(-2px);
    box-shadow: 0 8px 20px rgba(45,90,39,0.20);
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
    border: 1px solid rgba(195,141,79,0.4);
    border-left: 4px solid var(--bronce);
    border-radius: var(--radius-sm);
    padding: 14px 20px;
    font-size: 0.88rem;
    color: var(--text-color);
    margin-bottom: 20px;
    box-shadow: var(--shadow-sm);
    animation: fadeIn 0.3s ease;
}}

/* ── Tabla Premium (Zebra + Teal Header) ── */
.tabla-premium-wrapper {{
    border-radius: var(--radius-md);
    overflow: hidden;
    border: 1px solid rgba(128,128,128,0.18);
    box-shadow: var(--shadow-sm);
    margin-bottom: 8px;
    animation: fadeIn 0.35s ease;
}}
.tabla-premium {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.86rem;
}}
.tabla-premium thead tr {{
    background: linear-gradient(135deg, #3D8B7A 0%, #2D6B5F 100%);
}}
.tabla-premium thead th {{
    color: #ffffff;
    font-weight: 700;
    padding: 13px 16px;
    text-align: left;
    font-size: 0.82rem;
    letter-spacing: 0.4px;
    text-transform: uppercase;
    border-right: 1px solid rgba(255,255,255,0.12);
    white-space: nowrap;
}}
.tabla-premium thead th:last-child {{ border-right: none; }}
.tabla-premium tbody td {{
    padding: 11px 16px;
    border-bottom: 1px solid rgba(128,128,128,0.10);
    color: var(--text-color);
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.tabla-premium tbody tr.zebra-even {{ background: rgba(61,139,122,0.05); }}
.tabla-premium tbody tr.zebra-odd  {{ background: var(--background-color); }}
.tabla-premium tbody tr:hover {{
    background: rgba(61,139,122,0.12) !important;
    cursor: default;
}}
.tabla-premium tbody td input[type="checkbox"] {{
    width: 16px; height: 16px; accent-color: #3D8B7A;
}}

/* ── Pagination Info Bar ── */
.pagi-info-box {{ padding: 8px 0; }}
.pagi-info {{
    font-size: 0.86rem;
    color: var(--text-color);
    opacity: 0.7;
    font-weight: 500;
}}
.paginacion-bar {{
    text-align: center;
    padding: 8px 0;
    margin-bottom: 8px;
}}

/* ── Streamlit metric cards enhancement ── */
div[data-testid="stMetric"] {{
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.12);
    border-radius: var(--radius-md);
    padding: 16px 20px;
    box-shadow: var(--shadow-sm);
    animation: fadeIn 0.4s ease;
}}
div[data-testid="stMetric"]:hover {{
    box-shadow: var(--shadow-md);
    transform: translateY(-2px);
}}

/* ── Expanders ── */
details[data-testid="stExpander"] {{
    border: 1px solid rgba(128,128,128,0.15) !important;
    border-radius: var(--radius-md) !important;
    box-shadow: var(--shadow-sm);
}}
details[data-testid="stExpander"] summary {{
    font-weight: 600;
    font-size: 0.95rem;
}}

/* ── Tabs ── */
button[data-baseweb="tab"] {{
    font-weight: 600 !important;
    font-size: 0.92rem !important;
    letter-spacing: 0.2px;
}}

/* ── Scroll bars premium ── */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{
    background: rgba(128,128,128,0.25);
    border-radius: 4px;
}}
::-webkit-scrollbar-thumb:hover {{ background: rgba(128,128,128,0.4); }}

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


# ── Funciones de color (legacy — usados por pandas .style) ────────────────────

def colorear_estado(val):
    colores = {
        "✅ OK":          "background-color:rgba(46,139,87,0.15); color:#2E8B57; font-weight:600",
        "⚠️ Con errores": "background-color:rgba(230,126,34,0.15); color:#E67E22; font-weight:600",
        "❌ Falló":       "background-color:rgba(192,57,43,0.15); color:#C0392B; font-weight:600",
        "Pendiente":      "background-color:rgba(57,73,171,0.15); color:#3949AB; font-weight:600",
    }
    return colores.get(val, "")

def colorear_severidad(val):
    colores = {
        "CRÍTICO": "background-color:rgba(192,57,43,0.15); color:#C0392B; font-weight:bold",
        "ALTO":    "background-color:rgba(230,126,34,0.15); color:#E67E22; font-weight:bold",
        "MEDIO":   "background-color:rgba(46,139,87,0.15); color:#2E8B57; font-weight:bold",
    }
    return colores.get(val, "")

def score_a_color(score: float) -> str:
    if score is None:
        return "⚪"
    try:
        s = float(score)
        if s >= 0.85:
            return "🟢"
        elif s >= 0.70:
            return "🟡"
    except (ValueError, TypeError):
        pass
    return "🔴"

def crear_tarjeta_kpi(titulo: str, valor: str, icono: str, tipo: str = "") -> str:
    """Renderiza una tarjeta KPI HTML."""
    clase_tipo = f" {tipo}" if tipo else ""
    return f"""<div class="kpi-card{clase_tipo}">
<div class="kpi-icon">{icono}</div>
<div class="kpi-content">
<div class="kpi-title">{titulo}</div>
<div class="kpi-value">{valor}</div>
</div>
</div>"""


# ── Componente de Paginación Premium (local — para DataFrames en memoria) ─────

def crear_paginacion_ui(count: int, page_size: int, key: str) -> tuple[int, int]:
    """
    Renderiza controles de paginación local y retorna (start_idx, end_idx).
    Para paginación SQL usar seccion_tabla_sql_paginada() de componentes.py.
    """
    total_pages = max(1, math.ceil(count / page_size)) if count > 0 else 1

    st_key = f"pagi_{key}"
    if st_key not in st.session_state:
        st.session_state[st_key] = 1
    st.session_state[st_key] = max(1, min(st.session_state[st_key], total_pages))

    current = st.session_state[st_key]
    start_idx = (current - 1) * page_size
    end_idx = min(start_idx + page_size, count)

    if total_pages <= 1 and count > 0:
        st.markdown(f"""
            <div class="paginacion-bar">
                <span class="pagi-info">{start_idx + 1} a {end_idx} de {count}</span>
            </div>
        """, unsafe_allow_html=True)
        return 0, count

    if count == 0:
        return 0, 0

    col_info, col_nav = st.columns([1, 2])

    with col_info:
        st.markdown(f"""
            <div class="pagi-info-box">
                <span class="pagi-info">{start_idx + 1} a {end_idx} de {count}</span>
            </div>
        """, unsafe_allow_html=True)

    with col_nav:
        b1, b2, b3, b4, b5 = st.columns([1, 1, 3, 1, 1])
        with b1:
            if st.button("⏮", key=f"btn_first_{key}", disabled=current <= 1, use_container_width=True, help="Primera página"):
                st.session_state[st_key] = 1
                st.rerun()
        with b2:
            if st.button("◀", key=f"btn_prev_{key}", disabled=current <= 1, use_container_width=True, help="Anterior"):
                st.session_state[st_key] -= 1
                st.rerun()
        with b3:
            st.markdown(f"""
                <div style="text-align:center; padding:6px 0; font-size:0.9rem; font-weight:600; color:var(--text-color);">
                    Pág {current} de {total_pages}
                </div>
            """, unsafe_allow_html=True)
        with b4:
            if st.button("▶", key=f"btn_next_{key}", disabled=current >= total_pages, use_container_width=True, help="Siguiente"):
                st.session_state[st_key] += 1
                st.rerun()
        with b5:
            if st.button("⏭", key=f"btn_last_{key}", disabled=current >= total_pages, use_container_width=True, help="Última página"):
                st.session_state[st_key] = total_pages
                st.rerun()

    return start_idx, end_idx


# ── Generador de HTML de tabla (núcleo compartido) ────────────────────────────

def _generar_html_tabla(df, columnas_check=None, columnas_ocultas=None) -> str:
    """Genera HTML de tabla premium a partir de un DataFrame (slice ya cortado)."""
    if columnas_ocultas:
        df = df.drop(columns=[c for c in columnas_ocultas if c in df.columns], errors='ignore')

    cols = list(df.columns)
    header_html = "".join(f"<th>{c}</th>" for c in cols)

    rows_html = ""
    for i, (_, row) in enumerate(df.iterrows()):
        row_class = "zebra-even" if i % 2 == 0 else "zebra-odd"
        cells = ""
        for col in cols:
            val = row[col]
            if columnas_check and col in columnas_check:
                checked = "checked" if val else ""
                cells += f'<td><input type="checkbox" {checked} disabled /></td>'
            else:
                display_val = "" if val is None else str(val)
                cells += f"<td>{display_val}</td>"
        rows_html += f'<tr class="{row_class}">{cells}</tr>'

    return f"""
    <div class="tabla-premium-wrapper">
        <table class="tabla-premium">
            <thead><tr>{header_html}</tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </div>
    """


def renderizar_tabla_premium(df, key: str, page_size: int = 15,
                              columnas_check: list = None,
                              columnas_ocultas: list = None):
    """
    Tabla HTML premium con paginación LOCAL (datos en memoria).
    Para tablas grandes, usar seccion_tabla_sql_paginada() de componentes.py.
    """
    if df.empty:
        st.info("No hay datos para mostrar.")
        return

    if columnas_ocultas:
        df = df.drop(columns=[c for c in columnas_ocultas if c in df.columns], errors='ignore')

    count = len(df)
    start, end = crear_paginacion_ui(count, page_size, key)
    df_slice = df.iloc[start:end]

    st.markdown(
        _generar_html_tabla(df_slice, columnas_check=columnas_check),
        unsafe_allow_html=True,
    )


def renderizar_tabla_premium_raw(df, columnas_check=None, columnas_ocultas=None):
    """
    Tabla HTML premium SIN paginación propia.
    Diseñada para usarse con seccion_tabla_sql_paginada() donde
    los datos ya vienen cortados desde SQL Server.
    """
    if df.empty:
        st.info("No hay datos para mostrar.")
        return

    st.markdown(
        _generar_html_tabla(df, columnas_check=columnas_check, columnas_ocultas=columnas_ocultas),
        unsafe_allow_html=True,
    )

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
    padding: 24px;
    flex: 1;
    min-width: 220px;
    box-shadow: var(--shadow-sm);
    display: flex;
    flex-direction: column;
    justify-content: center;
    position: relative;
    overflow: hidden;
    transition: all 0.3s ease;
    border-top: 4px solid var(--bronce);
}}
.kpi-card:hover {{
    transform: translateY(-4px);
    box-shadow: var(--shadow-md);
    border-top-color: var(--verde-acp);
}}
.kpi-icon {{
    position: absolute;
    right: -10px;
    bottom: -10px;
    font-size: 5rem;
    opacity: 0.05;
    transform: rotate(-15deg);
    pointer-events: none;
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

/* ── Tabla Premium (Zebra Striping + Teal Header) ── */
.tabla-premium-wrapper {{
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid rgba(128,128,128,0.25);
    box-shadow: var(--shadow-sm);
    margin-bottom: 8px;
}}
.tabla-premium {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88rem;
}}
.tabla-premium thead tr {{
    background: linear-gradient(135deg, #3D8B7A 0%, #2D6B5F 100%);
}}
.tabla-premium thead th {{
    color: #ffffff;
    font-weight: 700;
    padding: 12px 16px;
    text-align: left;
    font-size: 0.85rem;
    letter-spacing: 0.3px;
    border-right: 1px solid rgba(255,255,255,0.15);
    white-space: nowrap;
}}
.tabla-premium thead th:last-child {{
    border-right: none;
}}
.tabla-premium tbody td {{
    padding: 10px 16px;
    border-bottom: 1px solid rgba(128,128,128,0.12);
    color: var(--text-color);
    max-width: 280px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.tabla-premium tbody tr.zebra-even {{
    background: rgba(61, 139, 122, 0.06);
}}
.tabla-premium tbody tr.zebra-odd {{
    background: var(--background-color);
}}
.tabla-premium tbody tr:hover {{
    background: rgba(61, 139, 122, 0.14) !important;
    cursor: default;
}}
.tabla-premium tbody td input[type="checkbox"] {{
    width: 16px;
    height: 16px;
    accent-color: #3D8B7A;
}}

/* ── Pagination Info Bar ── */
.pagi-info-box {{
    padding: 8px 0;
}}
.pagi-info {{
    font-size: 0.88rem;
    color: var(--text-color);
    opacity: 0.75;
    font-weight: 500;
}}
.paginacion-bar {{
    text-align: center;
    padding: 8px 0;
    margin-bottom: 8px;
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


# ── Componente de Paginación Premium ──
def crear_paginacion_ui(count: int, page_size: int, key: str) -> tuple[int, int]:
    """
    Renderiza controles de paginación profesional con navegación completa
    y retorna (start_idx, end_idx).
    Diseño: |< < ··· Page X of Y ··· > >|  +  "X to Y of Z"
    """
    import math
    total_pages = max(1, math.ceil(count / page_size)) if count > 0 else 1

    st_key = f"pagi_{key}"
    if st_key not in st.session_state:
        st.session_state[st_key] = 1
    if st.session_state[st_key] > total_pages:
        st.session_state[st_key] = total_pages
    if st.session_state[st_key] < 1:
        st.session_state[st_key] = 1

    current = st.session_state[st_key]
    start_idx = (current - 1) * page_size
    end_idx = min(start_idx + page_size, count)

    if total_pages <= 1 and count > 0:
        # Mostrar solo el conteo de registros sin botones
        st.markdown(f"""
            <div class="paginacion-bar">
                <span class="pagi-info">{start_idx + 1} a {end_idx} de {count}</span>
            </div>
        """, unsafe_allow_html=True)
        return 0, count

    if count == 0:
        return 0, 0

    # Layout: Info de registros | Botones de navegación
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
                    Page {current} of {total_pages}
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


def renderizar_tabla_premium(df, key: str, page_size: int = 15,
                              columnas_check: list = None,
                              columnas_ocultas: list = None):
    """
    Renderiza un DataFrame como una tabla HTML premium con:
    - Encabezado estilizado con el tema del portal (verde ACP)
    - Filas con zebra-striping (alternancia de color)
    - Paginación profesional inferior (|< < Page X of Y > >|)
    - Contador de registros (X to Y of Z)

    Args:
        df: DataFrame a renderizar
        key: Clave única para session_state
        page_size: Registros por página
        columnas_check: Lista de columnas que se muestran como checkbox
        columnas_ocultas: Lista de columnas a ocultar
    """
    if df.empty:
        st.info("No hay datos para mostrar.")
        return

    if columnas_ocultas:
        df = df.drop(columns=[c for c in columnas_ocultas if c in df.columns], errors='ignore')

    count = len(df)
    start, end = crear_paginacion_ui(count, page_size, key)
    df_slice = df.iloc[start:end]

    # ── Generar HTML de la tabla ──
    cols = list(df_slice.columns)
    header_html = "".join(f"<th>{c}</th>" for c in cols)

    rows_html = ""
    for i, (_, row) in enumerate(df_slice.iterrows()):
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

    tabla_html = f"""
    <div class="tabla-premium-wrapper">
        <table class="tabla-premium">
            <thead><tr>{header_html}</tr></thead>
            <tbody>{rows_html}</tbody>
        </table>
    </div>
    """
    st.markdown(tabla_html, unsafe_allow_html=True)

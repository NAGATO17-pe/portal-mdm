"""
utils/formato.py — Sistema de diseño visual del Portal MDM ACP (Enterprise)
=============================================================================
CSS Premium + Componentes de renderizado HTML.

Cambios v3 Light Premium Glassmorphism:
  - Google Fonts Inter + Outfit (tipografía premium dual)
  - Light Glassmorphism: fondos translúcidos con backdrop-filter blur
  - Sidebar con vidrio esmerilado semi-transparente (teal suave)
  - Paleta de colores agradable a la vista (descanso visual)
  - Header con gradiente sutil y bordes redondeados
  - Micro-animaciones refinadas (fadeIn, slideUp, shimmer)
  - Diseño 100% responsivo
"""

import math

import streamlit as st

# ── Light Premium Palette ──
TEAL_PRIMARY  = "#1B6B5A"
TEAL_LIGHT    = "#2D9B7E"
TEAL_SOFT     = "#E8F5F1"
AMBER_ACCENT  = "#D4915E"
SLATE_700     = "#374151"
SLATE_500     = "#64748B"
SLATE_200     = "#E2E8F0"
SURFACE_GLASS = "rgba(255, 255, 255, 0.72)"

CSS_PORTAL = f"""
<style>
/* ── Google Fonts ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@400;500;600;700&display=swap');

/* ── Design Tokens ── */
:root {{
    --teal-primary: {TEAL_PRIMARY};
    --teal-light: {TEAL_LIGHT};
    --teal-soft: {TEAL_SOFT};
    --amber-accent: {AMBER_ACCENT};
    --slate-700: {SLATE_700};
    --slate-500: {SLATE_500};
    --slate-200: {SLATE_200};

    /* Legacy aliases (backward compat) */
    --verde-acp: {TEAL_PRIMARY};
    --verde-claro: {TEAL_LIGHT};
    --bronce: {AMBER_ACCENT};

    --glass-bg: {SURFACE_GLASS};
    --glass-border: rgba(255, 255, 255, 0.45);
    --glass-shadow: 0 8px 32px rgba(27, 107, 90, 0.08);

    --shadow-xs: 0 1px 3px rgba(0,0,0,0.04);
    --shadow-sm: 0 2px 8px rgba(0,0,0,0.05);
    --shadow-md: 0 4px 16px rgba(0,0,0,0.08);
    --shadow-lg: 0 8px 32px rgba(0,0,0,0.10);
    --shadow-xl: 0 16px 48px rgba(0,0,0,0.12);

    --radius-sm: 10px;
    --radius-md: 14px;
    --radius-lg: 18px;
    --radius-xl: 24px;
}}

/* ── Typography ── */
html, body, [class*="css"] {{
    font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}}

h1, h2, h3, h4 {{
    font-family: 'Outfit', 'Inter', system-ui, sans-serif;
}}

/* ── Micro-animations ── */
@keyframes fadeIn {{
    from {{ opacity: 0; transform: translateY(6px); }}
    to   {{ opacity: 1; transform: translateY(0); }}
}}
@keyframes slideUp {{
    from {{ opacity: 0; transform: translateY(14px); }}
    to   {{ opacity: 1; transform: translateY(0); }}
}}
@keyframes shimmer {{
    0%   {{ background-position: -200% 0; }}
    100% {{ background-position: 200% 0; }}
}}
@keyframes softPulse {{
    0%, 100% {{ opacity: 1; }}
    50%      {{ opacity: 0.7; }}
}}

/* Smooth transitions — solo elementos del design system, no widgets Streamlit */
.kpi-card, .glass-card, .nav-item, .step-item, .badge,
.stButton > button, .sidebar-nav-item {{
    transition: background-color 0.25s ease, border-color 0.25s ease,
                box-shadow 0.25s ease, transform 0.2s ease, opacity 0.25s ease;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   APP BACKGROUND — Soft warm gradient with floating orbs
   ══════════════════════════════════════════════════════════════════════════════ */
.stApp {{
    background:
        radial-gradient(ellipse at 15% 5%, rgba(27,107,90,0.05) 0%, transparent 50%),
        radial-gradient(ellipse at 85% 90%, rgba(212,145,94,0.04) 0%, transparent 50%),
        radial-gradient(ellipse at 50% 50%, rgba(45,155,126,0.02) 0%, transparent 60%),
        linear-gradient(170deg, #FAFBFC 0%, #F1F5F4 40%, #F5F3F0 100%) !important;
    background-attachment: fixed !important;
    animation: fadeIn 0.4s ease-out;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   SIDEBAR — Light Premium (Senior FE)
   Pure white background, teal left-border accent. No dark backgrounds.
   ══════════════════════════════════════════════════════════════════════════════ */
section[data-testid="stSidebar"] {{
    background-color: transparent !important;
    border-right: none !important;
}}
section[data-testid="stSidebar"] > div:first-child {{
    background: #FFFFFF !important;
    border-right: 1px solid #E9EEF2 !important;
    box-shadow: 2px 0 16px rgba(27,107,90,0.05) !important;
}}

/* Reset all sidebar text to dark — excluye badges/spans con color inline */
section[data-testid="stSidebar"] *:not(span[style]) {{
    color: #374151 !important;
}}

/* ── Logo block — compact ── */
.sidebar-logo {{
    display: flex;
    align-items: center;
    gap: 9px;
    padding: 14px 14px 12px 14px;
    border-bottom: 1px solid #F1F5F8;
    margin-bottom: 4px;
}}
.sidebar-logo h2 {{
    font-family: 'Outfit', 'Inter', sans-serif;
    color: #1B6B5A !important;
    font-size: 1.0rem;
    font-weight: 700;
    margin: 0;
    letter-spacing: 0.3px;
}}
.sidebar-logo p {{
    display: none;
}}

/* ── Section header label ── */
.sidebar-section {{
    font-size: 0.58rem;
    text-transform: uppercase;
    letter-spacing: 1.6px;
    color: #9CA3AF !important;
    padding: 14px 14px 4px 14px;
    font-weight: 700;
}}

/* ── Radio nav: hide native circle bullet ── */
section[data-testid="stSidebar"] div[role="radiogroup"] > label > div:first-child {{
    display: none !important;
}}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label {{
    padding: 8px 14px;
    border-radius: 7px;
    margin-bottom: 1px;
    background: transparent;
    cursor: pointer;
    border-left: 3px solid transparent;
    transition: background 0.15s ease, border-color 0.15s ease;
}}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:hover {{
    background: #EFF8F5 !important;
    border-left-color: #2D9B7E !important;
}}
/* ── Página activa: resaltar ítem seleccionado ── */
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label[data-checked="true"],
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label[aria-checked="true"] {{
    background: rgba(27,107,90,0.10) !important;
    border-left-color: var(--teal-primary) !important;
}}
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label[data-checked="true"] p,
section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label[aria-checked="true"] p {{
    color: var(--teal-primary) !important;
    font-weight: 700 !important;
}}
section[data-testid="stSidebar"] .stRadio p,
section[data-testid="stSidebar"] .stRadio span,
section[data-testid="stSidebar"] .stRadio div[data-testid="stMarkdownContainer"] {{
    color: #374151 !important;
    font-size: 0.87rem;
    font-weight: 500;
}}

/* ── Footer ── */
.sidebar-footer {{
    text-align: center;
    padding: 16px 12px;
    border-top: 1px solid #F1F5F8;
    margin-top: 20px;
    font-size: 0.68rem;
    color: #CBD5E1 !important;
}}

/* ── Logout button ── */
section[data-testid="stSidebar"] .stButton > button {{
    background: #F9FAFB !important;
    border: 1px solid #E5E7EB !important;
    color: #6B7280 !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
}}
section[data-testid="stSidebar"] .stButton > button:hover {{
    background: #FEF2F2 !important;
    border-color: #FECACA !important;
    color: #DC2626 !important;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   PAGE HEADER — Glass card with teal accent
   ══════════════════════════════════════════════════════════════════════════════ */
.page-header {{
    background: #FFFFFF;
    border: 1px solid #EEF2F6;
    border-left: 4px solid var(--teal-primary);
    padding: 14px 20px;
    border-radius: 10px;
    margin-bottom: 18px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
    animation: fadeIn 0.3s ease;
    display: flex;
    align-items: center;
    gap: 10px;
}}
.page-header h1 {{
    font-family: 'Outfit', 'Inter', sans-serif;
    margin: 0;
    font-size: 1.15rem;
    font-weight: 700;
    color: var(--teal-primary);
    letter-spacing: -0.2px;
}}
.page-header p {{
    margin: 2px 0 0 0;
    font-size: 0.78rem;
    color: var(--slate-500);
    font-weight: 400;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   DATAFRAMES (native Streamlit) - Contrast not lines
   ══════════════════════════════════════════════════════════════════════════════ */
div[data-testid="stDataFrame"] {{
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid #EEF2F6;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}}
/* Compact table font */
div[data-testid="stDataFrame"] table {{
    font-size: 0.82rem !important;
}}
div[data-testid="stDataFrame"] thead th {{
    background: #F8FAFC !important;
    color: #374151 !important;
    font-weight: 600 !important;
    font-size: 0.72rem !important;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    padding: 8px 12px !important;
    border-bottom: 2px solid #EEF2F6 !important;
    border-right: none !important;
    border-left: none !important;
}}
div[data-testid="stDataFrame"] tbody tr:nth-child(even) td {{
    background: #F8FAFC !important;
}}
div[data-testid="stDataFrame"] tbody tr:nth-child(odd) td {{
    background: #FFFFFF !important;
}}
div[data-testid="stDataFrame"] tbody td {{
    padding: 6px 12px !important;
    border: none !important;
    color: #374151 !important;
    font-size: 0.82rem !important;
}}
div[data-testid="stDataFrame"] tbody tr:hover td {{
    background: #EFF8F5 !important;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   KPI CARDS — Glassmorphism float cards
   ══════════════════════════════════════════════════════════════════════════════ */
.kpi-container {{
    display: flex;
    gap: 14px;
    flex-wrap: wrap;
    animation: slideUp 0.5s ease;
}}
.kpi-card {{
    background: #FFFFFF;
    border: 1px solid #EEF2F6;
    border-radius: 10px;
    padding: 14px 18px 14px 16px;
    flex: 1;
    min-width: 150px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 12px;
    position: relative;
    overflow: hidden;
    border-left: 3px solid var(--amber-accent);
    border-top: none;
    transition: box-shadow 0.15s ease, transform 0.15s ease;
}}
.kpi-card:hover {{
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.07);
}}
.kpi-icon {{
    font-size: 1.6rem;
    opacity: 0.75;
    flex-shrink: 0;
    line-height: 1;
}}
.kpi-content {{ display: flex; flex-direction: column; }}
.kpi-title {{
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--slate-500);
    margin-bottom: 3px;
    font-weight: 600;
}}
.kpi-value {{
    font-family: 'Outfit', 'Inter', sans-serif;
    font-size: 1.35rem;
    font-weight: 700;
    color: var(--teal-primary);
    line-height: 1.1;
}}
.kpi-card.success {{ border-left-color: #10B981; }}
.kpi-card.success .kpi-value {{ color: #059669; }}
.kpi-card.warning {{ border-left-color: #F59E0B; }}
.kpi-card.warning .kpi-value {{ color: #D97706; }}
.kpi-card.danger  {{ border-left-color: #EF4444; }}
.kpi-card.danger  .kpi-value {{ color: #DC2626; }}
.kpi-card.info    {{ border-left-color: var(--teal-light); }}
.kpi-card.info    .kpi-value {{ color: var(--teal-primary); }}

/* ══════════════════════════════════════════════════════════════════════════════
   DECISION PANEL (cuarentena)
   ══════════════════════════════════════════════════════════════════════════════ */
.decision-panel {{
    background: var(--glass-bg);
    backdrop-filter: blur(10px);
    border: 1px solid var(--glass-border);
    border-left: 4px solid var(--teal-light);
    border-radius: var(--radius-md);
    padding: 20px 24px;
    margin-top: 12px;
    box-shadow: var(--shadow-sm);
    animation: fadeIn 0.3s ease;
}}
.decision-panel h4 {{ color: var(--teal-primary); margin: 0 0 12px 0; font-size: 1.02rem; }}
.decision-info {{
    background: rgba(248, 250, 252, 0.8);
    border-radius: var(--radius-sm);
    padding: 12px 16px;
    margin-bottom: 14px;
    border: 1px solid rgba(0,0,0,0.05);
    font-size: 0.86rem;
    color: var(--slate-700);
}}

/* ══════════════════════════════════════════════════════════════════════════════
   BUTTONS — Soft gradient with micro-lift
   ══════════════════════════════════════════════════════════════════════════════ */
.stButton > button {{
    border-radius: var(--radius-sm);
    font-weight: 600;
    font-size: 0.86rem;
    border: 1px solid transparent;
    box-shadow: var(--shadow-xs);
    letter-spacing: 0.2px;
}}
.stButton > button[kind="primary"] {{
    background: linear-gradient(135deg, var(--teal-primary) 0%, var(--teal-light) 100%) !important;
    color: white !important;
    border: none;
    box-shadow: 0 2px 8px rgba(27,107,90,0.18);
}}
.stButton > button[kind="primary"]:hover {{
    background: linear-gradient(135deg, var(--teal-light) 0%, var(--teal-primary) 100%) !important;
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(27,107,90,0.22) !important;
}}
.stButton > button[kind="secondary"]:hover {{
    transform: translateY(-1px);
    border-color: var(--teal-light) !important;
    color: var(--teal-primary) !important;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   BANNER
   ══════════════════════════════════════════════════════════════════════════════ */
.banner-aviso {{
    background: rgba(255, 251, 235, 0.85);
    backdrop-filter: blur(8px);
    border: 1px solid rgba(212,145,94,0.25);
    border-left: 4px solid var(--amber-accent);
    border-radius: var(--radius-sm);
    padding: 14px 20px;
    font-size: 0.86rem;
    color: var(--slate-700);
    margin-bottom: 20px;
    box-shadow: var(--shadow-xs);
    animation: fadeIn 0.3s ease;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   TABLE PREMIUM — Clean glass with soft teal header
   ══════════════════════════════════════════════════════════════════════════════ */
.tabla-premium-wrapper {{
    border-radius: var(--radius-md);
    overflow: hidden;
    border: 1px solid rgba(0,0,0,0.06);
    box-shadow: var(--shadow-sm);
    margin-bottom: 8px;
    animation: fadeIn 0.35s ease;
    background: var(--glass-bg);
    backdrop-filter: blur(8px);
}}
.tabla-premium {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.84rem;
}}
.tabla-premium thead tr {{
    background: linear-gradient(135deg, {TEAL_PRIMARY} 0%, {TEAL_LIGHT} 100%);
}}
.tabla-premium thead th {{
    color: #ffffff;
    font-weight: 600;
    padding: 12px 16px;
    text-align: left;
    font-size: 0.76rem;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    border-right: 1px solid rgba(255,255,255,0.10);
    white-space: nowrap;
    font-family: 'Inter', sans-serif;
}}
.tabla-premium thead th:last-child {{ border-right: none; }}
.tabla-premium tbody td {{
    padding: 10px 16px;
    border-bottom: 1px solid rgba(0,0,0,0.04);
    color: var(--slate-700);
    max-width: 300px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.tabla-premium tbody tr.zebra-even {{ background: rgba(27,107,90,0.03); }}
.tabla-premium tbody tr.zebra-odd  {{ background: rgba(255,255,255,0.6); }}
.tabla-premium tbody tr:hover {{
    background: rgba(27,107,90,0.07) !important;
    cursor: default;
}}
.tabla-premium tbody td input[type="checkbox"] {{
    width: 16px; height: 16px;
    accent-color: var(--teal-primary);
}}

/* ══════════════════════════════════════════════════════════════════════════════
   PAGINATION
   ══════════════════════════════════════════════════════════════════════════════ */
.pagi-info-box {{ padding: 8px 0; }}
.pagi-info {{
    font-size: 0.84rem;
    color: var(--slate-500);
    font-weight: 500;
}}
.paginacion-bar {{
    text-align: center;
    padding: 8px 0;
    margin-bottom: 8px;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   METRIC CARDS (native Streamlit)
   ══════════════════════════════════════════════════════════════════════════════ */
div[data-testid="stMetric"] {{
    background: var(--glass-bg);
    backdrop-filter: blur(10px);
    border: 1px solid var(--glass-border);
    border-radius: var(--radius-md);
    padding: 16px 20px;
    box-shadow: var(--shadow-sm);
    animation: fadeIn 0.4s ease;
}}
div[data-testid="stMetric"]:hover {{
    box-shadow: var(--shadow-md);
    transform: translateY(-2px);
}}

/* ══════════════════════════════════════════════════════════════════════════════
   EXPANDERS
   ══════════════════════════════════════════════════════════════════════════════ */
details[data-testid="stExpander"] {{
    background: var(--glass-bg) !important;
    backdrop-filter: blur(8px) !important;
    border: 1px solid rgba(0,0,0,0.06) !important;
    border-radius: var(--radius-md) !important;
    box-shadow: var(--shadow-xs);
}}
details[data-testid="stExpander"] summary {{
    font-weight: 600;
    font-size: 0.92rem;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   TABS — Active tab visual differentiation
   ══════════════════════════════════════════════════════════════════════════════ */
button[data-baseweb="tab"] {{
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    letter-spacing: 0.2px;
    border-bottom: 2px solid transparent !important;
    transition: border-color 0.15s ease, color 0.15s ease !important;
    padding-bottom: 10px !important;
}}
button[data-baseweb="tab"]:hover {{
    color: var(--teal-primary) !important;
    border-bottom-color: rgba(45,155,126,0.45) !important;
}}
button[data-baseweb="tab"][aria-selected="true"] {{
    color: var(--teal-primary) !important;
    border-bottom: 2px solid var(--teal-primary) !important;
    font-weight: 700 !important;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   CONTAINERS & FORMS (Glass treatment)
   ══════════════════════════════════════════════════════════════════════════════ */
div[data-testid="stForm"],
div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] > div > div > div > div[data-testid="stForm"] {{
    background: var(--glass-bg) !important;
    backdrop-filter: blur(12px) !important;
    border: 1px solid var(--glass-border) !important;
    border-radius: var(--radius-lg) !important;
}}

/* ══════════════════════════════════════════════════════════════════════════════
   SCROLLBARS — Visible en Windows
   ══════════════════════════════════════════════════════════════════════════════ */
::-webkit-scrollbar {{ width: 7px; height: 7px; }}
::-webkit-scrollbar-track {{ background: transparent; border-radius: 10px; }}
::-webkit-scrollbar-thumb {{
    background: rgba(27,107,90,0.25);
    border-radius: 10px;
}}
::-webkit-scrollbar-thumb:hover {{ background: rgba(27,107,90,0.45); }}
/* Scrollbar lateral del stepper (neural-monitor) */
.stepper-panel::-webkit-scrollbar {{ width: 5px; }}
.stepper-panel::-webkit-scrollbar-thumb {{ background: rgba(100,116,139,0.2); }}

/* ══════════════════════════════════════════════════════════════════════════════
   RESPONSIVE
   ══════════════════════════════════════════════════════════════════════════════ */
@media (max-width: 768px) {{
    .page-header {{ padding: 18px 20px; }}
    .page-header h1 {{ font-size: 1.25rem; }}
    .kpi-container {{ gap: 10px; }}
    .kpi-card {{ min-width: 140px; padding: 16px 18px; }}
    .kpi-value {{ font-size: 1.35rem; }}
    .tabla-premium {{ font-size: 0.78rem; }}
    .tabla-premium thead th {{ padding: 10px 12px; font-size: 0.72rem; }}
    .tabla-premium tbody td {{ padding: 8px 12px; }}
}}

/* ══════════════════════════════════════════════════════════════════════════════
   NEURAL MONITOR — Data Flow Visualization
   ══════════════════════════════════════════════════════════════════════════════ */
.neural-wrapper {{
    display: flex;
    align-items: stretch;
    gap: 20px;
    animation: fadeIn 0.4s ease;
}}
.neural-viz {{
    flex: 0 0 260px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: linear-gradient(145deg, #FAFBFC, #F1F5F4);
    border-radius: var(--radius-lg);
    border: 1px solid #EEF2F6;
    padding: 16px;
    position: relative;
    overflow: hidden;
}}
.neural-viz svg {{
    width: 100%;
    height: auto;
    max-height: 200px;
}}
/* Phase label below the SVG */
.neural-phase-label {{
    position: absolute;
    bottom: 10px;
    left: 0; right: 0;
    text-align: center;
    font-family: 'Outfit', 'Inter', sans-serif;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 1.2px;
    text-transform: uppercase;
}}

/* ── Vertical Stepper ── */
.stepper-panel {{
    flex: 1;
    max-height: 280px;
    overflow-y: auto;
    padding: 4px 0;
}}
.step-item {{
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 8px 12px;
    border-radius: 8px;
    transition: opacity 0.4s ease, background 0.2s ease;
    position: relative;
}}
.step-item + .step-item::before {{
    content: '';
    position: absolute;
    left: 22px;
    top: -5px;
    width: 2px;
    height: 12px;
    background: #E2E8F0;
}}
.step-item.step-done {{
    opacity: 0.38;
}}
.step-item.step-active {{
    opacity: 1;
    background: rgba(27,107,90,0.04);
    border-radius: 8px;
}}
.step-item.step-pending {{
    opacity: 0.25;
}}
.step-dot {{
    width: 16px;
    height: 16px;
    border-radius: 50%;
    flex-shrink: 0;
    margin-top: 2px;
    border: 2px solid #CBD5E1;
    background: #FFFFFF;
    transition: all 0.3s ease;
}}
.step-done .step-dot {{
    background: #10B981;
    border-color: #10B981;
}}
.step-active .step-dot {{
    border-color: var(--teal-primary);
    background: var(--teal-primary);
    box-shadow: 0 0 0 4px rgba(27,107,90,0.15);
    animation: softPulse 1.5s ease-in-out infinite;
}}
.step-error .step-dot {{
    background: #EF4444;
    border-color: #EF4444;
}}
.step-label {{
    font-size: 0.82rem;
    font-weight: 500;
    color: var(--slate-700);
    line-height: 1.35;
}}
.step-active .step-label {{
    font-weight: 600;
    color: var(--teal-primary);
}}
.step-meta {{
    font-size: 0.68rem;
    color: var(--slate-500);
    margin-top: 1px;
}}

</style>
"""

def obtener_css() -> str:
    return CSS_PORTAL

def aplicar_css():
    st.markdown(obtener_css(), unsafe_allow_html=True)

def header_pagina(icono: str, titulo: str, descripcion: str = ""):
    st.markdown(f"""
        <div class="page-header">
            <div style="display:flex; flex-direction:column; min-width:0;">
                <h1 style="margin:0;">{icono} {titulo}</h1>
                {'<p style="margin:2px 0 0 0;">' + descripcion + '</p>' if descripcion else ''}
            </div>
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
            if st.button("⏮", key=f"btn_first_{key}", disabled=current <= 1, width='stretch', help="Primera página"):
                st.session_state[st_key] = 1
                st.rerun()
        with b2:
            if st.button("◀", key=f"btn_prev_{key}", disabled=current <= 1, width='stretch', help="Anterior"):
                st.session_state[st_key] -= 1
                st.rerun()
        with b3:
            st.markdown(f"""
                <div style="text-align:center; padding:6px 0; font-size:0.9rem; font-weight:600; color:var(--text-color);">
                    Pág {current} de {total_pages}
                </div>
            """, unsafe_allow_html=True)
        with b4:
            if st.button("▶", key=f"btn_next_{key}", disabled=current >= total_pages, width='stretch', help="Siguiente"):
                st.session_state[st_key] += 1
                st.rerun()
        with b5:
            if st.button("⏭", key=f"btn_last_{key}", disabled=current >= total_pages, width='stretch', help="Última página"):
                st.session_state[st_key] = total_pages
                st.rerun()

    return start_idx, end_idx


# ── Generador de vista interactiva (nativa) ──────────────────────────────────

def _configurar_columnas_dataframe(cols: list, columnas_check: list | None) -> dict:
    """Configura las columnas de casillas de verificación si se proveen."""
    config = {}
    if columnas_check:
        for c in columnas_check:
            if c in cols:
                config[c] = st.column_config.CheckboxColumn(c, disabled=True)
    return config

def renderizar_tabla_premium(df, key: str, page_size: int = 15,
                              columnas_check: list = None,
                              columnas_ocultas: list = None):
    """
    Tabla interactiva nativa con paginación LOCAL externa.
    """
    if df is None or df.empty:
        st.info("No hay datos para mostrar.")
        return

    if columnas_ocultas:
        df = df.drop(columns=[c for c in columnas_ocultas if c in df.columns], errors='ignore')

    count = len(df)
    start, end = crear_paginacion_ui(count, page_size, key)
    df_slice = df.iloc[start:end]

    config = _configurar_columnas_dataframe(df_slice.columns, columnas_check)

    st.dataframe(
        df_slice,
        width='stretch',
        hide_index=True,
        column_config=config,
    )


def renderizar_tabla_premium_raw(df, columnas_check=None, columnas_ocultas=None):
    """
    Tabla interactiva nativa SIN paginación propia.
    (Diseñada para SQL Server paginado).
    """
    if df is None or df.empty:
        st.info("No hay datos para mostrar.")
        return

    if columnas_ocultas:
        df = df.drop(columns=[c for c in columnas_ocultas if c in df.columns], errors='ignore')

    config = _configurar_columnas_dataframe(df.columns, columnas_check)

    st.dataframe(
        df,
        width='stretch',
        hide_index=True,
        column_config=config,
    )

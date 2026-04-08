"""
utils/componentes.py — Componentes reutilizables del Portal MDM ACP (Enterprise)
==================================================================================
Módulo ÚNICO de UI compartida. Todas las páginas importan desde aquí.

Componentes disponibles
-----------------------
  cargar_con_fallback()           — Carga BD con df vacío como fallback
  mostrar_kpis()                  — Fila de métricas st.metric estandarizada
  badge_html()                    — Badge HTML semántico (severidad/estado)
  estado_vacio_html()             — Estado vacío elegante (HTML puro)
  seccion_tabla_sql_paginada()    — Tabla con paginación SQL server-side
  seccion_tabla_con_guardar()     — Tabla con paginación local + guardar
  banner_aviso()                  — Banner de aviso estándar
  health_status_panel()           — Panel de salud de conexión descriptivo
"""

from __future__ import annotations

import math
from typing import Callable

import pandas as pd
import streamlit as st

from utils.db import ejecutar_query_paginado, verificar_conexion
from utils.formato import renderizar_tabla_premium_raw


# ── 1. Carga con fallback ─────────────────────────────────────────────────────

def cargar_con_fallback(
    fn_carga: Callable[[], pd.DataFrame],
    columnas_fallback: list[str],
) -> pd.DataFrame:
    """
    Intenta cargar datos desde la BD. Si la conexión falla,
    retorna un DataFrame vacío con las columnas esperadas.
    """
    if verificar_conexion():
        try:
            return fn_carga()
        except Exception:
            pass
    return pd.DataFrame(columns=columnas_fallback)


# ── 2. KPIs normalizados ──────────────────────────────────────────────────────

def mostrar_kpis(metricas: list[dict]) -> None:
    """
    Renderiza una fila de métricas con `st.metric` en columnas iguales.

    Cada dict:  label, value, delta (opt), delta_color (opt: "normal"|"inverse"|"off")
    """
    cols = st.columns(len(metricas))
    for col, m in zip(cols, metricas):
        col.metric(
            label=m["label"],
            value=m["value"],
            delta=m.get("delta"),
            delta_color=m.get("delta_color", "normal"),
        )


# ── 3. Badge HTML unificado ───────────────────────────────────────────────────

_BADGE_PALETTE: dict[str, tuple[str, str, str]] = {
    "CRÍTICO":         ("#C0392B", "rgba(192, 57,  43, 0.12)", "#C0392B"),
    "ALTO":            ("#E67E22", "rgba(230,126,  34, 0.12)", "#E67E22"),
    "MEDIO":           ("#1E6B35", "rgba( 30,107,  53, 0.12)", "#1E6B35"),
    "PENDIENTE":       ("#3949AB", "rgba( 57, 73, 171, 0.12)", "#3949AB"),
    "EN_REVISIÓN":     ("#E29D45", "rgba(226,157,  69, 0.12)", "#E29D45"),
    "RESUELTO":        ("#1E6B35", "rgba( 30,107,  53, 0.12)", "#1E6B35"),
    "EN_CUARENTENA":   ("#C0392B", "rgba(192, 57,  43, 0.12)", "#C0392B"),
    "✅ OK":           ("#2E8B57", "rgba( 46,139,  87, 0.12)", "#2E8B57"),
    "⚠️ Con errores":  ("#E67E22", "rgba(230,126,  34, 0.12)", "#E67E22"),
    "❌ Falló":        ("#C0392B", "rgba(192, 57,  43, 0.12)", "#C0392B"),
    "OK":              ("#2E8B57", "rgba( 46,139,  87, 0.12)", "#2E8B57"),
}
_BADGE_DEFAULT = ("#555555", "rgba(128,128,128,0.10)", "#aaaaaa")


def badge_html(texto: str, tipo: str | None = None) -> str:
    """Genera un <span> badge semántico HTML."""
    clave = tipo if tipo is not None else texto
    fg, bg, border = _BADGE_PALETTE.get(clave, _BADGE_DEFAULT)
    return (
        f"<span style='"
        f"background:{bg}; color:{fg}; border:1px solid {border}; "
        f"padding:2px 10px; border-radius:12px; "
        f"font-size:0.78rem; font-weight:700; "
        f"display:inline-block; line-height:1.6;"
        f"'>{texto}</span>"
    )


# ── 4. Estado vacío elegante ─────────────────────────────────────────────────

def estado_vacio_html(
    icono: str = "📭",
    titulo: str = "Sin datos disponibles",
    subtitulo: str = "No se encontraron registros para mostrar.",
) -> None:
    """Renderiza un estado vacío con HTML premium (reemplaza Lottie)."""
    st.markdown(
        f"""
        <div style="
            display: flex; flex-direction: column; align-items: center;
            justify-content: center; padding: 56px 24px; text-align: center;
            background: var(--secondary-background-color);
            border: 1px solid rgba(128,128,128,0.15); border-radius: 16px;
            margin: 16px 0;
        ">
            <div style="font-size:3.8rem; margin-bottom:16px; opacity:0.55;
                        filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1));">
                {icono}
            </div>
            <h4 style="margin:0 0 8px 0; color:var(--text-color); opacity:0.8;
                       font-size:1.1rem; font-weight:600;">
                {titulo}
            </h4>
            <p style="margin:0; color:var(--text-color); opacity:0.50;
                      font-size:0.92rem; max-width:360px;">
                {subtitulo}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── 5. Paginación SQL Server-Side ────────────────────────────────────────────

def _controles_paginacion_sql(total_count: int, page_size: int, key: str) -> int:
    """
    Renderiza controles de paginación y retorna el número de página actual.
    Los datos ya vienen paginados desde SQL — aquí solo se controla la UI.
    """
    total_pages = max(1, math.ceil(total_count / page_size))
    st_key = f"pagi_sql_{key}"

    if st_key not in st.session_state:
        st.session_state[st_key] = 1
    st.session_state[st_key] = max(1, min(st.session_state[st_key], total_pages))

    current = st.session_state[st_key]
    start_display = (current - 1) * page_size + 1
    end_display   = min(current * page_size, total_count)

    if total_count == 0:
        return 1

    if total_pages <= 1:
        st.markdown(
            f'<div class="paginacion-bar">'
            f'<span class="pagi-info">{start_display} a {end_display} de {total_count}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        return 1

    col_info, col_nav = st.columns([1, 2])

    with col_info:
        st.markdown(
            f'<div class="pagi-info-box">'
            f'<span class="pagi-info">{start_display} a {end_display} de {total_count}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    with col_nav:
        b1, b2, b3, b4, b5 = st.columns([1, 1, 3, 1, 1])
        with b1:
            if st.button("⏮", key=f"btn_first_{key}", disabled=current <= 1,
                         use_container_width=True, help="Primera página"):
                st.session_state[st_key] = 1
                st.rerun()
        with b2:
            if st.button("◀", key=f"btn_prev_{key}", disabled=current <= 1,
                         use_container_width=True, help="Anterior"):
                st.session_state[st_key] -= 1
                st.rerun()
        with b3:
            st.markdown(
                f'<div style="text-align:center; padding:6px 0; font-size:0.9rem; '
                f'font-weight:600; color:var(--text-color);">Pág {current} de {total_pages}</div>',
                unsafe_allow_html=True,
            )
        with b4:
            if st.button("▶", key=f"btn_next_{key}", disabled=current >= total_pages,
                         use_container_width=True, help="Siguiente"):
                st.session_state[st_key] += 1
                st.rerun()
        with b5:
            if st.button("⏭", key=f"btn_last_{key}", disabled=current >= total_pages,
                         use_container_width=True, help="Última página"):
                st.session_state[st_key] = total_pages
                st.rerun()

    return current


def seccion_tabla_sql_paginada(
    query_base: str,
    order_by: str,
    key: str,
    titulo: str = "📋 Registros",
    page_size: int = 15,
    columnas_check: list[str] | None = None,
    columnas_ocultas: list[str] | None = None,
    params: dict | None = None,
    caption: str = "Paginación delegada a SQL Server.",
    mostrar_boton_guardar: bool = True,
    btn_key: str | None = None,
    btn_label: str = "💾 Guardar cambios",
) -> pd.DataFrame | None:
    """
    Componente de tabla con paginación REAL a nivel de SQL Server.

    Solo viajan por la red los N registros de la página actual.
    Ideal para tablas con miles o millones de filas.

    Args:
        query_base:  SELECT sin ORDER BY (ej: "SELECT * FROM Silver.Dim_Geografia WHERE ...")
        order_by:    Cláusula ORDER BY (ej: "Fundo, Sector")
        key:         Clave única para session_state
        titulo:      Título de la sección
        page_size:   Registros por página
        params:      Parámetros SQL opcionales

    Returns:
        DataFrame de la página actual (para que la página pueda usarlo para KPIs locales)
    """
    if titulo:
        st.markdown(f"### {titulo}")

    # Obtener página actual desde session_state
    st_key_page = f"pagi_sql_{key}"
    current_page = st.session_state.get(st_key_page, 1)

    # Ejecutar query paginada en SQL Server
    if not verificar_conexion():
        estado_vacio_html(icono="🔌", titulo="Sin conexión",
                          subtitulo="No se pudo conectar a la base de datos.")
        return None

    try:
        df_page, total_count = ejecutar_query_paginado(
            query_base, order_by, current_page, page_size, params
        )
    except Exception as e:
        st.error(f"Error ejecutando query paginada: {e}")
        return None

    if total_count == 0:
        estado_vacio_html(
            icono="📭", titulo="Sin registros",
            subtitulo="No hay datos registrados en este catálogo.",
        )
        return None

    # Controles de paginación
    _controles_paginacion_sql(total_count, page_size, key)

    if caption:
        st.caption(caption)

    # Renderizar tabla HTML premium (ya viene paginada desde SQL)
    renderizar_tabla_premium_raw(
        df_page,
        columnas_check=columnas_check,
        columnas_ocultas=columnas_ocultas,
    )

    if mostrar_boton_guardar:
        _btn_key = btn_key or f"btn_{key}_guardar"
        if st.button(btn_label, key=_btn_key, type="primary"):
            st.toast("Cambios guardados con éxito (Simulación).", icon="✅")

    return df_page


# ── 6. Tabla local con guardar (fallback para DataFrames ya en memoria) ──────

def seccion_tabla_con_guardar(
    df: pd.DataFrame,
    key: str,
    titulo: str = "📋 Registros",
    page_size: int = 15,
    columnas_check: list[str] | None = None,
    columnas_ocultas: list[str] | None = None,
    btn_key: str | None = None,
    btn_label: str = "💾 Guardar cambios",
    caption: str = "Paginación profesional activa.",
    mostrar_boton_guardar: bool = True,
) -> None:
    """
    Tabla con paginación local (para datos que ya están en memoria).
    Usar seccion_tabla_sql_paginada() cuando sea posible.
    """
    if titulo:
        st.markdown(f"### {titulo}")

    if df.empty:
        estado_vacio_html(
            icono="📭", titulo="Sin registros",
            subtitulo="No hay datos registrados en este catálogo.",
        )
        return

    if caption:
        st.caption(caption)

    from utils.formato import renderizar_tabla_premium
    renderizar_tabla_premium(
        df, key=key, page_size=page_size,
        columnas_check=columnas_check,
        columnas_ocultas=columnas_ocultas,
    )

    if mostrar_boton_guardar:
        _btn_key = btn_key or f"btn_{key}_guardar"
        if st.button(btn_label, key=_btn_key, type="primary"):
            st.toast("Cambios guardados con éxito (Simulación).", icon="✅")


# ── 7. Banner de aviso estandarizado ─────────────────────────────────────────

def banner_aviso(mensaje: str) -> None:
    """Renderiza el banner de aviso naranja estándar del portal."""
    st.markdown(
        f'<div class="banner-aviso">⚠️ <b>Atención:</b> {mensaje}</div>',
        unsafe_allow_html=True,
    )


# ── 8. Panel de salud de conexión ────────────────────────────────────────────

def health_status_panel() -> bool:
    """Muestra un badge de conexión basado en la disponibilidad del Backend."""
    from utils.api_client import get_api
    resultado = get_api("/health/live", base_url="http://127.0.0.1:8000")
    if resultado.ok:
        st.sidebar.markdown(badge_html("Servidor API Conectado", bg_color="#ECFDF3", text_color="#027A48"), unsafe_allow_html=True)
        return True
    
    st.sidebar.markdown(badge_html("Servidor API Desconectado", bg_color="#FEF3F2", text_color="#B42318"), unsafe_allow_html=True)
    return False

def mostrar_dialogo_confirmacion(titulo: str, mensaje: str, callback: callable, *args, **kwargs):
    """
    Despliega un modal de confirmación limpio usando st.dialog (1.37+) o 
    st.experimental_dialog (1.34+).
    """
    if hasattr(st, "dialog"):
        decorator = st.dialog
    elif hasattr(st, "experimental_dialog"):
        decorator = st.experimental_dialog
    else:
        # Fallback para versiones muy antiguas
        st.warning(mensaje)
        if st.button("Confirmar fallback"):
            callback(*args, **kwargs)
        return

    @decorator(titulo)
    def _modal():
        st.markdown(f"**{mensaje}**")
        st.markdown("<br>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("✅ Sí, Confirmar", type="primary", use_container_width=True):
                callback(*args, **kwargs)
                st.rerun()
        with col2:
            if st.button("❌ Cancelar", use_container_width=True):
                st.rerun()

    _modal()

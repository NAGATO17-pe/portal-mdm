"""
paginas/configuracion/parametros_pipeline.py — Parámetros Pipeline (Enterprise)
Paginación SQL Server + health panel descriptivo.
"""

import streamlit as st

from utils.componentes import (
    banner_aviso,
    estado_vacio_html,
    health_status_panel,
)
from utils.db import ejecutar_query_paginado
from utils.formato import header_pagina

import math

_QUERY_BASE = """
    SELECT
        Nombre_Parametro AS [Parámetro],
        Valor            AS [Valor actual],
        Descripcion      AS [Descripción],
        CONVERT(varchar, Fecha_Modificacion, 120) AS [Última modificación]
    FROM Config.Parametros_Pipeline
"""

_ORDER_BY = "Nombre_Parametro"


def render():
    header_pagina(
        "⚙️", "Configuración · Parámetros Pipeline",
        "Parámetros operativos del ETL · edita con confirmación",
    )

    banner_aviso("Los cambios en parámetros aplican en la <b>próxima ejecución del ETL</b>.")

    conectado = health_status_panel()

    if not conectado:
        return

    # ── Paginación SQL ──
    page_size = 10
    st_key = "pagi_sql_params_cfg"
    if st_key not in st.session_state:
        st.session_state[st_key] = 1

    current_page = st.session_state[st_key]

    try:
        df, total_count = ejecutar_query_paginado(
            _QUERY_BASE, _ORDER_BY, current_page, page_size
        )
    except Exception as e:
        st.error(f"Error: {e}")
        return

    st.markdown("### ⚙️ Parámetros activos")

    if total_count == 0:
        estado_vacio_html(icono="⚙️", titulo="Sin parámetros",
                          subtitulo="No hay parámetros configurados.")
        return

    # ── Info paginación ──
    total_pages = max(1, math.ceil(total_count / page_size))
    st.session_state[st_key] = max(1, min(st.session_state[st_key], total_pages))
    current_page = st.session_state[st_key]
    start_display = (current_page - 1) * page_size + 1
    end_display   = min(current_page * page_size, total_count)

    st.caption(f"Mostrando {start_display} a {end_display} de {total_count} · Paginación SQL Server")

    # ── Formulario de edición ──
    for _, row in df.iterrows():
        with st.container():
            p1, p2, p3 = st.columns([2.5, 2, 4])
            with p1:
                st.markdown(f"**`{row['Parámetro']}`**")
                st.caption(f"Última mod.: {row['Última modificación']}")
            with p2:
                st.text_input(
                    "Valor",
                    value=str(row["Valor actual"]),
                    key=f"param_{row['Parámetro']}",
                    label_visibility="collapsed",
                )
            with p3:
                st.markdown(
                    f"<span style='color:var(--text-color); opacity:0.6; font-size:0.85rem;'>"
                    f"{row['Descripción']}</span>",
                    unsafe_allow_html=True,
                )
        st.markdown("---")

    # ── Controles de paginación ──
    if total_pages > 1:
        b1, b2, b3, b4, b5 = st.columns([1, 1, 3, 1, 1])
        with b1:
            if st.button("⏮", key="btn_first_params", disabled=current_page <= 1, use_container_width=True):
                st.session_state[st_key] = 1
                st.rerun()
        with b2:
            if st.button("◀", key="btn_prev_params", disabled=current_page <= 1, use_container_width=True):
                st.session_state[st_key] -= 1
                st.rerun()
        with b3:
            st.markdown(
                f'<div style="text-align:center; padding:6px 0; font-size:0.9rem; '
                f'font-weight:600; color:var(--text-color);">Pág {current_page} de {total_pages}</div>',
                unsafe_allow_html=True)
        with b4:
            if st.button("▶", key="btn_next_params", disabled=current_page >= total_pages, use_container_width=True):
                st.session_state[st_key] += 1
                st.rerun()
        with b5:
            if st.button("⏭", key="btn_last_params", disabled=current_page >= total_pages, use_container_width=True):
                st.session_state[st_key] = total_pages
                st.rerun()

    if st.button("💾 Guardar todos los cambios", key="btn_param_guardar", type="primary"):
        st.toast("Cambios guardados con éxito (Simulación).", icon="✅")

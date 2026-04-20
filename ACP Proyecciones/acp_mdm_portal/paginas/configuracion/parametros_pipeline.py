"""
paginas/configuracion/parametros_pipeline.py — Parámetros Pipeline (Enterprise)
Paginación SQL Server + health panel descriptivo.
"""

import math

import pandas as pd
import streamlit as st

from utils.api_client import get_api, patch_api
from utils.componentes import (
    banner_aviso,
    estado_vacio_html,
    health_status_panel,
)
from utils.formato import header_pagina


def cargar_parametros(pagina: int, tamano: int) -> dict:
    resultado = get_api(f"/config/parametros?pagina={pagina}&tamano={tamano}")
    if resultado.ok and isinstance(resultado.data, dict):
        return resultado.data
    return {"total": 0, "pagina": pagina, "tamano": tamano, "datos": []}


def render():
    header_pagina(
        "⚙️", "Configuración · Parámetros Pipeline",
        "Parámetros operativos del ETL · edita con confirmación",
    )

    banner_aviso("Los cambios en parámetros aplican en la <b>próxima ejecución del ETL</b>.")

    health_status_panel()

    page_size = 10
    st_key = "pagi_params_cfg"
    if st_key not in st.session_state:
        st.session_state[st_key] = 1

    current_page = st.session_state[st_key]
    data = cargar_parametros(current_page, page_size)
    total_count = data.get("total", 0)
    df = pd.DataFrame(data.get("datos", [])).rename(columns={
        "nombre_parametro": "Parámetro", "valor": "Valor actual",
        "descripcion": "Descripción", "fecha_modificacion": "Última modificación",
    }) if data.get("datos") else pd.DataFrame()

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

    st.caption(f"Mostrando {start_display} a {end_display} de {total_count}")

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
            if st.button("⏮", key="btn_first_params", disabled=current_page <= 1, width='stretch'):
                st.session_state[st_key] = 1
                st.rerun()
        with b2:
            if st.button("◀", key="btn_prev_params", disabled=current_page <= 1, width='stretch'):
                st.session_state[st_key] -= 1
                st.rerun()
        with b3:
            st.markdown(
                f'<div style="text-align:center; padding:6px 0; font-size:0.9rem; '
                f'font-weight:600; color:var(--text-color);">Pág {current_page} de {total_pages}</div>',
                unsafe_allow_html=True)
        with b4:
            if st.button("▶", key="btn_next_params", disabled=current_page >= total_pages, width='stretch'):
                st.session_state[st_key] += 1
                st.rerun()
        with b5:
            if st.button("⏭", key="btn_last_params", disabled=current_page >= total_pages, width='stretch'):
                st.session_state[st_key] = total_pages
                st.rerun()

    if st.button("💾 Guardar todos los cambios", key="btn_param_guardar", type="primary"):
        cambios = [
            {"nombre_parametro": row["Parámetro"], "valor": st.session_state.get(f"param_{row['Parámetro']}", row["Valor actual"])}
            for _, row in df.iterrows()
        ]
        res = patch_api("/config/parametros", {"parametros": cambios})
        if res.ok:
            st.toast("Cambios guardados con éxito.", icon="✅")
            st.rerun()
        else:
            st.error(f"Error al guardar: {res.error}")

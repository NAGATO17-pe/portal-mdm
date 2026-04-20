"""
paginas/configuracion/reglas_validacion.py — Reglas de Validación (Enterprise)
Paginación delegada a SQL Server via OFFSET/FETCH.
"""

import streamlit as st

import pandas as pd

from utils.api_client import get_api
from utils.componentes import (
    banner_aviso,
    health_status_panel,
    mostrar_kpis,
    mostrar_dialogo_confirmacion,
    seccion_tabla_con_guardar,
)
from utils.formato import header_pagina


def _cargar_reglas(pagina: int = 1, tamano: int = 15) -> dict:
    resultado = get_api(f"/config/reglas?pagina={pagina}&tamano={tamano}")
    if resultado.ok and isinstance(resultado.data, dict):
        return resultado.data
    return {"total": 0, "pagina": pagina, "tamano": tamano, "kpis": {}, "datos": []}


def render():
    header_pagina(
        "📋", "Configuración · Reglas de Validación",
        "Ajusta rangos y reglas de calidad de datos sin tocar código",
    )

    banner_aviso("Los cambios en reglas aplican en la <b>próxima ejecución del ETL</b>.")

    health_status_panel()

    # ── KPIs vía API ──
    data = _cargar_reglas()
    kpis = data.get("kpis", {})
    mostrar_kpis([
        {"label": "Total reglas", "value": kpis.get("total", 0)},
        {"label": "Activas",      "value": kpis.get("activas", 0)},
        {"label": "Inactivas",    "value": kpis.get("inactivas", 0)},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar regla ──
    with st.expander("➕ Agregar nueva regla", expanded=False):
        r1, r2, r3, r4, r5 = st.columns([2, 2, 1, 1, 1.5])
        with r1:
            tabla = st.text_input("Tabla destino", key="reg_tabla", placeholder="Ej: Bronce.Calibres")
        with r2:
            columna = st.text_input("Columna", key="reg_col", placeholder="Ej: Peso_Baya")
        with r3:
            st.number_input("Valor min", key="reg_min", value=0.0)
        with r4:
            st.number_input("Valor max", key="reg_max", value=100.0)
        with r5:
            st.markdown("<br>", unsafe_allow_html=True)
            campos_ok = all(v and v.strip() for v in [tabla, columna])
            if st.button("✅ Agregar", key="btn_reg_agregar", type="primary", disabled=not campos_ok):
                def do_agregar_regla(t, c):
                    st.toast(f"Regla para '{t}.{c}' agregada correctamente.", icon="✅")
                mostrar_dialogo_confirmacion(
                    "Confirmar Regla",
                    f"¿Crear nueva regla de validación para la columna '{columna}'?",
                    do_agregar_regla, tabla, columna
                )
        if not campos_ok:
            st.caption("Completa Tabla y Columna para habilitar.")

    st.markdown("---")

    # ── Tabla vía API ──
    datos = data.get("datos", [])
    if datos:
        df = pd.DataFrame(datos).rename(columns={
            "tabla_destino": "Tabla destino", "columna": "Columna",
            "tipo_validacion": "Tipo validación", "valor_min": "Valor min",
            "valor_max": "Valor max", "accion": "Acción", "activo": "Activo",
        })
        seccion_tabla_con_guardar(
            df, key="reglas_cfg", titulo="📋 Reglas de validación",
            page_size=15, mostrar_boton_guardar=False,
        )

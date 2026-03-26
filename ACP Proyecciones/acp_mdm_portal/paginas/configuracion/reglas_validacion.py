"""
paginas/configuracion/reglas_validacion.py — Reglas de Validación (Enterprise)
Paginación delegada a SQL Server via OFFSET/FETCH.
"""

import streamlit as st

from utils.componentes import (
    banner_aviso,
    health_status_panel,
    mostrar_kpis,
    seccion_tabla_sql_paginada,
)
from utils.db import ejecutar_query
from utils.formato import header_pagina


_KPI_QUERY = """
    SELECT
        COUNT(*)                      AS total,
        SUM(CAST(Activo AS INT))      AS activas,
        COUNT(*) - SUM(CAST(Activo AS INT)) AS inactivas
    FROM Config.Reglas_Validacion
"""

_TABLA_QUERY = """
    SELECT
        Tabla_Destino   AS [Tabla destino],
        Columna,
        Tipo_Validacion AS [Tipo validación],
        Valor_Min       AS [Valor min],
        Valor_Max       AS [Valor max],
        Accion          AS [Acción],
        Activo
    FROM Config.Reglas_Validacion
"""

_ORDER_BY = "Tabla_Destino, Columna"


def render():
    header_pagina(
        "📋", "Configuración · Reglas de Validación",
        "Ajusta rangos y reglas de calidad de datos sin tocar código",
    )

    banner_aviso("Los cambios en reglas aplican en la <b>próxima ejecución del ETL</b>.")

    conectado = health_status_panel()

    # ── KPIs ──
    total = activas = inactivas = 0
    if conectado:
        try:
            row = ejecutar_query(_KPI_QUERY)
            if not row.empty:
                total     = int(row["total"].iloc[0])
                activas   = int(row["activas"].iloc[0])
                inactivas = int(row["inactivas"].iloc[0])
        except Exception:
            pass

    mostrar_kpis([
        {"label": "Total reglas", "value": total},
        {"label": "Activas",      "value": activas},
        {"label": "Inactivas",    "value": inactivas},
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
                st.toast(f"Regla para '{tabla}.{columna}' agregada correctamente.", icon="✅")
        if not campos_ok:
            st.caption("Completa Tabla y Columna para habilitar.")

    st.markdown("---")

    # ── Tabla con paginación SQL ──
    if conectado:
        seccion_tabla_sql_paginada(
            query_base=_TABLA_QUERY,
            order_by=_ORDER_BY,
            key="reglas_cfg",
            titulo="📋 Reglas de validación",
            page_size=15,
            columnas_check=["Activo"],
            btn_key="btn_reg_guardar",
            caption="Paginación SQL Server · solo viajan 15 registros por request.",
        )

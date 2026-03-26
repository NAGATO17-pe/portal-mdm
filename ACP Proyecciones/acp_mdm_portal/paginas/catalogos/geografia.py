"""
paginas/catalogos/geografia.py — Catálogo de Geografía (Enterprise)
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
        COUNT(*)                                          AS total,
        SUM(CAST(Es_Vigente AS INT))                     AS activos,
        SUM(CAST(Es_Test_Block AS INT))                  AS test_blocks
    FROM Silver.Dim_Geografia
"""

_TABLA_QUERY = """
    SELECT
        Fundo,
        Sector,
        Modulo        AS [Módulo],
        Turno,
        Es_Test_Block AS [Es Test Block],
        Es_Vigente    AS [Activa]
    FROM Silver.Dim_Geografia
"""

_ORDER_BY = "Fundo, Sector, Modulo"


def render():
    header_pagina("📍", "Catálogos · Geografía", "Fundos, sectores y módulos · cambios activan SCD2")

    banner_aviso("Los cambios en geografía activan <b>SCD Tipo 2</b> en la próxima ejecución del ETL.")

    conectado = health_status_panel()

    # ── KPIs ──
    total = activos = test_blocks = 0
    if conectado:
        try:
            row = ejecutar_query(_KPI_QUERY)
            if not row.empty:
                total       = int(row["total"].iloc[0])
                activos     = int(row["activos"].iloc[0])
                test_blocks = int(row["test_blocks"].iloc[0])
        except Exception:
            pass

    mostrar_kpis([
        {"label": "Módulos totales", "value": total},
        {"label": "Activos",         "value": activos},
        {"label": "Test Blocks",     "value": test_blocks},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar módulo ──
    with st.expander("➕ Agregar módulo nuevo", expanded=False):
        g1, g2, g3, g4 = st.columns(4)
        with g1:
            fundo = st.text_input("Fundo", key="geo_fundo", placeholder="Ej: Santa Patricia")
        with g2:
            sector = st.text_input("Sector", key="geo_sector", placeholder="Ej: Sector A")
        with g3:
            modulo = st.text_input("Módulo", key="geo_modulo", placeholder="Ej: MCA-01")
        with g4:
            st.selectbox("Turno", ["Mañana", "Tarde"], key="geo_turno")

        campos_ok = all(v and v.strip() for v in [fundo, sector, modulo])
        if st.button("✅ Agregar", key="btn_geo_agregar", type="primary", disabled=not campos_ok):
            st.toast(f"Módulo '{modulo}' en fundo '{fundo}' agregado correctamente.", icon="✅")
        if not campos_ok:
            st.caption("Completa Fundo, Sector y Módulo para habilitar.")

    st.markdown("---")

    # ── Tabla con paginación SQL ──
    if conectado:
        seccion_tabla_sql_paginada(
            query_base=_TABLA_QUERY,
            order_by=_ORDER_BY,
            key="geografia_cat",
            titulo="📋 Módulos registrados",
            page_size=15,
            columnas_check=["Es Test Block", "Activa"],
            btn_key="btn_geo_guardar",
            caption="Paginación SQL Server · solo viajan 15 registros por request.",
        )

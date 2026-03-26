"""
paginas/catalogos/personal.py — Catálogo de Personal (Enterprise)
Paginación delegada a SQL Server via OFFSET/FETCH.
"""

import streamlit as st

from utils.componentes import (
    health_status_panel,
    mostrar_kpis,
    seccion_tabla_sql_paginada,
)
from utils.db import ejecutar_query
from utils.formato import header_pagina


_KPI_QUERY = """
    SELECT
        COUNT(*)                     AS total,
        SUM(CASE WHEN Rol <> 'Administrativo' THEN 1 ELSE 0 END) AS campo,
        ROUND(AVG(Pct_Asertividad), 1) AS prod_avg
    FROM Silver.Dim_Personal
"""

_TABLA_QUERY = """
    SELECT
        DNI,
        Nombre_Completo AS [Nombre],
        Rol,
        Sexo,
        ID_Planilla     AS [Sede],
        Pct_Asertividad AS [Productividad],
        Dias_Ausentismo AS [Faltas]
    FROM Silver.Dim_Personal
"""

_ORDER_BY = "Nombre_Completo"


def render():
    header_pagina("👤", "Catálogos · Personal", "Gestión de trabajadores y roles")

    conectado = health_status_panel()

    # ── KPIs ──
    total = campo = 0
    prod_avg = 0.0
    if conectado:
        try:
            row = ejecutar_query(_KPI_QUERY)
            if not row.empty:
                total    = int(row["total"].iloc[0])
                campo    = int(row["campo"].iloc[0])
                prod_avg = float(row["prod_avg"].iloc[0] or 0)
        except Exception:
            pass

    mostrar_kpis([
        {"label": "Personal total",     "value": total},
        {"label": "En campo",           "value": campo},
        {"label": "Productividad avg",  "value": f"{prod_avg}%"},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Tabla con paginación SQL ──
    if conectado:
        seccion_tabla_sql_paginada(
            query_base=_TABLA_QUERY,
            order_by=_ORDER_BY,
            key="personal_cat",
            titulo="📋 Personal registrado",
            page_size=15,
            btn_key="btn_per_guardar",
            caption="Paginación SQL Server · solo viajan 15 registros por request.",
        )

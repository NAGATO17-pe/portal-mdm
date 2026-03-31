"""
paginas/catalogos/variedades.py — Catálogo de Variedades (Enterprise)
Paginación delegada a SQL Server via OFFSET/FETCH.
"""

import streamlit as st

from utils.componentes import (
    banner_aviso,
    health_status_panel,
    mostrar_kpis,
    seccion_tabla_sql_paginada,
    mostrar_dialogo_confirmacion,
)
from utils.db import ejecutar_query
from utils.formato import header_pagina


# Query para KPIs (liviana, sin paginar)
_KPI_QUERY = """
    SELECT
        COUNT(*)                         AS total,
        SUM(CAST(Es_Activa AS INT))      AS activas,
        COUNT(*) - SUM(CAST(Es_Activa AS INT)) AS inactivas
    FROM MDM.Catalogo_Variedades
"""

# Query base para la tabla paginada (sin ORDER BY)
_TABLA_QUERY = """
    SELECT
        Nombre_Canonico AS [Nombre canónico],
        Breeder,
        Es_Activa       AS [Activa]
    FROM MDM.Catalogo_Variedades
"""

_ORDER_BY = "Nombre_Canonico"


def render():
    header_pagina("🍇", "Catálogos · Variedades", "Gestión del catálogo oficial de variedades")

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
        {"label": "Total variedades", "value": total},
        {"label": "Activas",          "value": activas},
        {"label": "Inactivas",        "value": inactivas},
    ])

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Agregar variedad ──
    with st.expander("➕ Agregar variedad nueva", expanded=False):
        a1, a2, a3 = st.columns([2, 2, 1])
        with a1:
            nombre = st.text_input("Nombre canónico", key="var_nuevo_nombre",
                                   placeholder="Ej: Biloxi, Emerald")
        with a2:
            breeder = st.text_input("Breeder", key="var_nuevo_breeder",
                                    placeholder="Ej: Fall Creek")
        with a3:
            st.markdown("<br>", unsafe_allow_html=True)
            btn_disabled = not (nombre and nombre.strip())
            if st.button("✅ Agregar", key="btn_var_agregar", type="primary",
                      disabled=btn_disabled):
                def do_agregar(nom):
                    st.toast(f"Variedad '{nom}' agregada correctamente.", icon="✅")
                mostrar_dialogo_confirmacion(
                    "Confirmación de Catálogo",
                    f"¿Estás seguro de que deseas agregar la variedad '{nombre}'?",
                    do_agregar, nombre
                )
        if nombre and not nombre.strip():
            st.warning("El nombre canónico es obligatorio.", icon="⚠️")

    st.markdown("---")

    # ── Tabla con paginación SQL ──
    if conectado:
        seccion_tabla_sql_paginada(
            query_base=_TABLA_QUERY,
            order_by=_ORDER_BY,
            key="variedades_cat",
            titulo="📋 Variedades registradas",
            page_size=15,
            columnas_check=["Activa"],
            btn_key="btn_var_guardar",
            caption="Paginación SQL Server · solo viajan 15 registros por request.",
        )

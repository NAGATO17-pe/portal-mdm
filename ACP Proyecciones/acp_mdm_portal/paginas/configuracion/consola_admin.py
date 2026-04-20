"""
consola_admin.py — Herramienta interna de diagnóstico para administradores.
===========================================================================
Acceso directo a SQL Server via utils.db para debugging operativo sin abrir SSMS.
Uso intencional: esta página mantiene acceso SQL directo por diseño.
TODO (Sprint futuro): migrar las consultas rápidas a endpoints /admin/query
y reemplazar la consola libre por un explorador de tablas vía API.
"""
import streamlit as st
import pandas as pd
from utils.db import ejecutar_query
from utils.auth import tiene_permiso
from utils.formato import header_pagina

# Diccionarios de tablas y comandos amigables
_TABLAS_AMIGABLES = {
    "🍇 Catálogo de Variedades": "MDM.Catalogo_Variedades",
    "📍 Geografía y Módulos": "Silver.Dim_Geografia",
    "📋 Reglas de Validación": "Config.Reglas_Validacion",
    "⚙️ Parámetros del Pipeline": "Config.Parametros_Pipeline",
    "⌛ Log de Tiempos (Ejecuciones)": "Config.Log_Ejecucion",
    "🚨 Registros en Cuarentena": "Auditoria.Cuarentena_MDM",
    "🗂️ Hechos: Tareo Diario": "Silver.Fact_Tareo"
}

_COMANDOS_AMIGABLES = {
    "👀 Ver los primeros 50 registros": "SELECT TOP 50 * FROM {tabla}",
    "🧮 Contar el total de registros en la tabla": "SELECT COUNT(*) AS [Total de Registros] FROM {tabla}",
    "🔍 Ver registros recientes (Últimos 20)": "SELECT TOP 20 * FROM {tabla} ORDER BY 1 DESC",
}

def render():
    """Renderiza la página de pruebas libres de Base de Datos para administradores."""
    header_pagina("🛠️", "Pruebas de Base de Datos", "Entorno para consultar y evaluar los datos alojados en SQL Server de manera amigable.")

    # Protección rígida
    if not tiene_permiso("configurar"):
        st.error("Acceso denegado. Se requieren privilegios de Administrador para usar esta consola.")
        st.stop()

    # Rediseño UX: Usar pestañas para separar lo amigable de lo crudo
    tab1, tab2 = st.tabs(["🚀 Consultas Rápidas (Amigables)", "⌨️ Consola SQL Avanzada"])

    query_a_ejecutar = ""

    # ── Pestaña 1: Consultas Amigables ──────────────────────────────────────────
    with tab1:
        st.markdown("### Analizador Visual de Tablas")
        st.markdown("Elige el módulo y el tipo de información que quieres extraer sin escribir código.")
        
        c1, c2 = st.columns(2)
        with c1:
            tabla_seleccionada = st.selectbox(
                "1. Área o Tabla a revisar:",
                options=list(_TABLAS_AMIGABLES.keys())
            )
        with c2:
            comando_seleccionado = st.selectbox(
                "2. Acción a consultar:",
                options=list(_COMANDOS_AMIGABLES.keys())
            )
        
        # Armar la query basada en la selección
        tabla_db = _TABLAS_AMIGABLES[tabla_seleccionada]
        query_rapida = _COMANDOS_AMIGABLES[comando_seleccionado].format(tabla=tabla_db)
        
        st.info(f"**Query generada en lenguaje BD:** `{query_rapida}`")
        
        if st.button("▶️ Ejecutar Consulta Visual", type="primary", key="btn_visual", width='stretch'):
            query_a_ejecutar = query_rapida


    # ── Pestaña 2: Sentencias Libres ───────────────────────────────────────────
    with tab2:
        st.markdown("### 🖥️ Consola SQL T-SQL")
        st.warning("⚠️  Ejecuta sentencias personalizadas bajo tu propia responsabilidad.")

        query_libre = st.text_area(
            "Ingresa tu consulta (T-SQL):",
            height=150,
            placeholder="SELECT TOP 100 * FROM Schema.Table WHERE Condicion = 1",
            label_visibility="collapsed"
        )
        
        btn_col1, btn_col2, _ = st.columns([2, 1, 3])
        with btn_col1:
            if st.button("🚀 Ejecutar T-SQL", type="primary", key="btn_tsql", width='stretch'):
                query_a_ejecutar = query_libre
        with btn_col2:
            if st.button("🧹 Limpiar Texto", width='stretch'):
                st.rerun()

    st.markdown("---")

    # ── Ejecución Común ────────────────────────────────────────────────────────
    if query_a_ejecutar:
        if not query_a_ejecutar.strip():
            st.warning("⚠️ La consulta está vacía. Escribe algo válido para ejecutar.")
        else:
            with st.spinner("⏳ Conectando y extrayendo información de SQL Server..."):
                try:
                    df = ejecutar_query(query_a_ejecutar)
                    
                    filas = len(df)
                    cols = len(df.columns)
                    
                    st.success(f"✅ ¡Datos recuperados! Se obtuvieron **{filas} filas** distribuidas en **{cols} columnas**.")
                    
                    if filas > 0:
                        st.dataframe(df, width='stretch', hide_index=True)
                        
                        csv = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Descargar Resultado (CSV)",
                            data=csv,
                            file_name='resultado_prueba_bd.csv',
                            mime='text/csv',
                        )
                    else:
                        st.info("La operación culminó exitosamente, pero no devolvió ningún registro de datos (tabla o cruce vacío).")
                        
                except Exception as e:
                    st.error("❌ Ocurrió un error al procesar la solicitud en SQL Server:")
                    st.code(str(e), language="sql")

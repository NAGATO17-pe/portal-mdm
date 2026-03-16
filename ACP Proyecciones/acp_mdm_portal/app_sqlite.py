"""
app_sqlite.py — Portal MDM conectado a SQLite de desarrollo.
Versión de prueba real: lee los datos que escribe pipeline_dev.py

Ejecutar con:
    .\.venv\Scripts\streamlit.exe run app_sqlite.py
"""
import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
from utils.formato import aplicar_css, header_pagina, colorear_severidad, colorear_estado, score_a_color

DB_PATH = Path(__file__).parent.parent / "ETL" / "data" / "acp_dev.db"

st.set_page_config(
    page_title="ACP MDM Portal [DEV-SQLite]",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

aplicar_css()


def check_db():
    if not DB_PATH.exists():
        st.error(f"""
            ❌ **No se encontró la base de datos de desarrollo.**

            Ejecuta primero en la terminal:
            ```
            python ETL/dev/setup_sqlite.py
            ```
            Luego carga datos con:
            ```
            python ETL/dev/pipeline_dev.py
            ```
        """)
        st.stop()


def query(sql: str, params=()) -> pd.DataFrame:
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def execute(sql: str, params=()):
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(sql, params)
    conn.commit()
    conn.close()


# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div class="sidebar-logo">
            <div style="font-size:2rem;">🧪</div>
            <h2>ACP MDM Portal</h2>
            <p>Modo DEV · SQLite Local</p>
        </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <div style="background:rgba(205,127,50,0.15); border:1px solid #CD7F32;
             border-radius:8px; padding:8px 12px; margin-bottom:12px; font-size:0.78rem; color:#FFD700;">
            🗄️ BD: <code>acp_dev.db</code><br>
            📁 {DB_PATH.name}
        </div>
    """, unsafe_allow_html=True)

    pagina = st.radio(
        "Navegación",
        options=[
            "🏠  Inicio",
            "🔴  Cuarentena",
            "🔗  Homologación",
            "📚  Catálogos › Variedades",
            "⚙️   Config › Parámetros",
        ],
        label_visibility="collapsed",
    )

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Actualizar datos", use_container_width=True):
        st.rerun()


check_db()

# ─────────────────────────────────────────────────────────────────────────
# INICIO
# ─────────────────────────────────────────────────────────────────────────
if pagina == "🏠  Inicio":
    header_pagina("🏠", "Inicio [SQLite DEV]", "Estado real del pipeline de desarrollo")

    df_log = query('SELECT * FROM "Auditoria.Log_Carga" ORDER BY ID_Log DESC LIMIT 5')
    df_cuar = query('SELECT * FROM "MDM.Cuarentena"')
    df_homo = query('SELECT * FROM "MDM.Homologacion" WHERE Aprobado = 0')

    total_ins = df_log["Filas_Insertadas"].sum() if not df_log.empty else 0
    total_rec = df_log["Filas_Rechazadas"].sum() if not df_log.empty else 0
    ultima = df_log.iloc[0]["Inicio_Carga"][:16] if not df_log.empty else "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⏱️ Última carga", ultima)
    c2.metric("✅ Filas OK (acum.)", f"{int(total_ins):,}")
    c3.metric("🔴 En cuarentena", len(df_cuar))
    c4.metric("🔗 Homologaciones pendientes", len(df_homo))

    st.markdown("<br>", unsafe_allow_html=True)

    if len(df_cuar[df_cuar["Severidad"] == "CRÍTICO"]) > 0:
        n = len(df_cuar[df_cuar["Severidad"] == "CRÍTICO"])
        st.error(f"🚨 **{n} filas CRÍTICAS** en cuarentena. Ir a **Cuarentena** para resolver.")
    if len(df_homo) > 0:
        st.warning(f"⚠️ **{len(df_homo)} variedades** sin homologar. Ir a **Homologación**.")
    if df_log.empty:
        st.info("📭 No hay cargas registradas todavía. Ejecuta `pipeline_dev.py` con tus archivos Excel.")

    if not df_log.empty:
        st.markdown("### 📋 Log de últimas cargas")
        st.dataframe(df_log, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────
# CUARENTENA
# ─────────────────────────────────────────────────────────────────────────
elif pagina == "🔴  Cuarentena":
    header_pagina("🔴", "Cuarentena [SQLite DEV]", "Filas rechazadas reales de tu ETL")

    df = query('SELECT * FROM "MDM.Cuarentena" ORDER BY ID_Cuarentena DESC')

    if df.empty:
        st.success("✅ Sin registros en cuarentena — todo limpio.")
    else:
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            tablas = ["Todas"] + df["Tabla_Origen"].unique().tolist()
            tabla_sel = st.selectbox("Tabla", tablas, key="cq_tabla")
        with fc2:
            sevs = ["Todas"] + df["Severidad"].unique().tolist()
            sev_sel = st.selectbox("Severidad", sevs, key="cq_sev")
        with fc3:
            estados = ["Todos"] + df["Estado"].unique().tolist()
            estado_sel = st.selectbox("Estado", estados, key="cq_estado")

        if tabla_sel != "Todas":
            df = df[df["Tabla_Origen"] == tabla_sel]
        if sev_sel != "Todas":
            df = df[df["Severidad"] == sev_sel]
        if estado_sel != "Todos":
            df = df[df["Estado"] == estado_sel]

        st.markdown(f"**{len(df)} registros**")
        st.dataframe(
            df[["ID_Cuarentena","Tabla_Origen","Columna_Origen","Valor_Raw","Motivo","Severidad","Estado","Fecha_Ingreso"]]
              .style.applymap(colorear_severidad, subset=["Severidad"]),
            use_container_width=True, hide_index=True, height=300
        )

        st.markdown("---")
        st.markdown("### Resolver registro")
        id_sel = st.selectbox("ID a resolver", df["ID_Cuarentena"].tolist(), key="cq_id")
        fila = df[df["ID_Cuarentena"] == id_sel].iloc[0]

        st.info(f"**Valor:** `{fila['Valor_Raw']}` | **Columna:** {fila['Columna_Origen']} | **Motivo:** {fila['Motivo']}")

        opcion = st.radio("Acción", ["✅ Marcar como resuelto", "🗑️ Descartar"], key=f"cq_op_{id_sel}")
        if st.button("Aplicar", type="primary", key=f"cq_btn_{id_sel}"):
            decision = "Resuelto" if "resuelto" in opcion else "Descartado"
            execute(
                '''UPDATE "MDM.Cuarentena" SET Estado = ?, Decision = ?, Resuelto_En = datetime('now')
                   WHERE ID_Cuarentena = ?''',
                (decision, decision, int(id_sel))
            )
            st.success(f"✅ Registro {id_sel} → {decision}")
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────
# HOMOLOGACIÓN
# ─────────────────────────────────────────────────────────────────────────
elif pagina == "🔗  Homologación":
    header_pagina("🔗", "Homologación [SQLite DEV]", "Sugerencias generadas por el ETL real")

    tab1, tab2 = st.tabs(["📬 Pendientes", "📚 Aprobadas"])

    with tab1:
        df = query('SELECT * FROM "MDM.Homologacion" WHERE Aprobado = 0 ORDER BY Veces_Visto DESC')
        if df.empty:
            st.success("✅ No hay sugerencias pendientes.")
        else:
            for _, row in df.iterrows():
                icono = score_a_color(row["Score"])
                st.markdown(f"""
                    <div style="background:white; border:1px solid #D5E0D8; border-left:4px solid #1E6B35;
                         border-radius:8px; padding:10px 16px; margin-bottom:8px;">
                        {icono} &nbsp;<b>"{row['Texto_Crudo']}"</b> → <b style="color:#1E6B35;">"{row['Valor_Canonico_Sugerido']}"</b>
                        &nbsp; Score: <b>{row['Score']:.2f}</b> · {row['Veces_Visto']}x visto
                    </div>
                """, unsafe_allow_html=True)

                a1, a2, a3 = st.columns([1, 2, 1])
                with a1:
                    if st.button("✅ Aprobar", key=f"ap_{row['ID_Homologacion']}", type="primary"):
                        execute(
                            """UPDATE "MDM.Homologacion" SET Aprobado=1, Aprobado_Por='Usuario',
                               Fecha_Aprobacion=datetime('now') WHERE ID_Homologacion=?""",
                            (int(row["ID_Homologacion"]),)
                        )
                        st.success("Aprobado")
                        st.rerun()
                with a2:
                    nuevo = st.text_input("Corregir:", value=row["Valor_Canonico_Sugerido"],
                                          key=f"co_{row['ID_Homologacion']}", label_visibility="collapsed")
                    if st.button("✏️ Aprobar con corrección", key=f"ac_{row['ID_Homologacion']}"):
                        execute(
                            """UPDATE "MDM.Homologacion" SET Aprobado=1, Valor_Canonico_Sugerido=?,
                               Aprobado_Por='Usuario', Fecha_Aprobacion=datetime('now')
                               WHERE ID_Homologacion=?""",
                            (nuevo, int(row["ID_Homologacion"]))
                        )
                        st.success(f"Aprobado con corrección: {nuevo}")
                        st.rerun()
                with a3:
                    if st.button("❌ Rechazar", key=f"re_{row['ID_Homologacion']}"):
                        execute(
                            'DELETE FROM "MDM.Homologacion" WHERE ID_Homologacion=?',
                            (int(row["ID_Homologacion"]),)
                        )
                        st.warning("Rechazado y eliminado.")
                        st.rerun()
                st.markdown("---")

    with tab2:
        df_hist = query('SELECT * FROM "MDM.Homologacion" WHERE Aprobado = 1 ORDER BY Fecha_Aprobacion DESC')
        if df_hist.empty:
            st.info("Sin aprobadas todavía.")
        else:
            st.dataframe(df_hist, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────────────────
# CATÁLOGO VARIEDADES
# ─────────────────────────────────────────────────────────────────────────
elif pagina == "📚  Catálogos › Variedades":
    header_pagina("📚", "Catálogos · Variedades [SQLite DEV]", "Catálogo real en acp_dev.db")

    df = query('SELECT * FROM "MDM.Catalogo_Variedades" ORDER BY Nombre_Canonico')
    c1, c2 = st.columns(2)
    c1.metric("Total", len(df))
    c2.metric("Activas", int(df["Es_Activa"].sum()))

    st.markdown("<br>", unsafe_allow_html=True)

    with st.expander("➕ Agregar variedad"):
        n1, n2 = st.columns(2)
        with n1:
            nv = st.text_input("Nombre canónico", key="sv_n")
        with n2:
            nb = st.text_input("Breeder", key="sv_b")
        if st.button("✅ Agregar", type="primary", key="sv_btn"):
            if nv and nb:
                execute('INSERT INTO "MDM.Catalogo_Variedades" (Nombre_Canonico, Breeder) VALUES (?, ?)', (nv, nb))
                st.success(f"✅ {nv} agregada.")
                st.rerun()

    edited = st.data_editor(
        df[["ID_Variedad", "Nombre_Canonico", "Breeder", "Es_Activa"]],
        use_container_width=True, hide_index=True,
        column_config={"Es_Activa": st.column_config.CheckboxColumn("Activa")},
        disabled=["ID_Variedad", "Nombre_Canonico", "Breeder"]
    )

    if st.button("💾 Guardar cambios", type="primary", key="sv_save"):
        for _, row in edited.iterrows():
            execute(
                'UPDATE "MDM.Catalogo_Variedades" SET Es_Activa=? WHERE ID_Variedad=?',
                (int(row["Es_Activa"]), int(row["ID_Variedad"]))
            )
        st.success("✅ Guardado.")
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────
# PARÁMETROS
# ─────────────────────────────────────────────────────────────────────────
elif pagina == "⚙️   Config › Parámetros":
    header_pagina("⚙️", "Parámetros Pipeline [SQLite DEV]", "Valores reales en acp_dev.db")

    df = query('SELECT * FROM "Config.Parametros_Pipeline" ORDER BY Nombre_Parametro')

    cambios = {}
    for _, row in df.iterrows():
        p1, p2, p3 = st.columns([2.5, 2, 4])
        with p1:
            st.markdown(f"**`{row['Nombre_Parametro']}`**")
        with p2:
            nuevo = st.text_input("Valor", value=str(row["Valor"]),
                                  key=f"pp_{row['Nombre_Parametro']}", label_visibility="collapsed")
            cambios[row["Nombre_Parametro"]] = nuevo
        with p3:
            st.caption(str(row["Descripcion"]))
        st.markdown("---")

    if st.button("💾 Guardar parámetros", type="primary"):
        for nombre, valor in cambios.items():
            execute(
                """UPDATE "Config.Parametros_Pipeline"
                   SET Valor=?, Fecha_Modificacion=datetime('now')
                   WHERE Nombre_Parametro=?""",
                (valor, nombre)
            )
        st.success("✅ Parámetros guardados en acp_dev.db.")
        st.rerun()

import sqlalchemy
from sqlalchemy import text

engine = sqlalchemy.create_engine('mssql+pyodbc://LCP-PAG-PRACTIC/ACP_DataWarehose_Proyecciones?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes')

queries = {
    "Indices": "SELECT i.name, i.type_desc, is_unique FROM sys.indexes i WHERE i.object_id = OBJECT_ID('Silver.Fact_Tasa_Crecimiento_Brotes')",
    "Count": "SELECT COUNT(*) FROM Silver.Fact_Tasa_Crecimiento_Brotes",
    "Running Queries": """
        SELECT r.start_time, r.status, r.command, r.wait_type, r.wait_time, r.last_wait_type, t.text
        FROM sys.dm_exec_requests r
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
        WHERE r.session_id != @@SPID
    """
}

try:
    with engine.connect() as conn:
        for name, query in queries.items():
            print(f"--- {name} ---")
            try:
                res = conn.execute(text(query)).fetchall()
                for row in res:
                    print(row)
            except Exception as e:
                print(f"Error en {name}: {e}")
except Exception as e:
    print(f"Error de conexion: {e}")

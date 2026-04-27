import urllib
from sqlalchemy import create_engine, text

def inspeccionar_silver():
    s='LCP-PAG-PRACTIC'
    b='ACP_DataWarehose_Proyecciones'
    d='ODBC Driver 17 for SQL Server'
    c=f'DRIVER={{{d}}};SERVER={s};DATABASE={b};Trusted_Connection=yes;TrustServerCertificate=yes;'
    u='mssql+pyodbc:///?odbc_connect='+urllib.parse.quote_plus(c)
    
    engine = create_engine(u)
    with engine.connect() as conn:
        print("Tablas en Silver:")
        res = conn.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Silver' AND TABLE_NAME LIKE 'Fact%'"))
        for r in res:
            print(f"- {r[0]}")

if __name__ == "__main__":
    inspeccionar_silver()

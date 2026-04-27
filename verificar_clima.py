import urllib
from sqlalchemy import create_engine, text

def buscar_clima():
    s='LCP-PAG-PRACTIC'
    b='ACP_DataWarehose_Proyecciones'
    d='ODBC Driver 17 for SQL Server'
    c=f'DRIVER={{{d}}};SERVER={s};DATABASE={b};Trusted_Connection=yes;TrustServerCertificate=yes;'
    u='mssql+pyodbc:///?odbc_connect='+urllib.parse.quote_plus(c)
    
    engine = create_engine(u)
    with engine.connect() as conn:
        print("Buscando tablas relacionadas con 'Clima'...")
        res = conn.execute(text("SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%Clima%'"))
        for r in res:
            print(f"- {r[0]}")
            # Ver cuántas filas tiene
            count = conn.execute(text(f"SELECT COUNT(*) FROM {r[0]}")).scalar()
            print(f"  (Filas: {count})")

if __name__ == "__main__":
    buscar_clima()

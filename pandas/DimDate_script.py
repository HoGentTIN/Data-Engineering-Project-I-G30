import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Stap 1: Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
table_name = "DimDate"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 2: DimDate DataFrame genereren
date_range = pd.date_range(start="2010-01-01", end="2025-12-31")
df_dim_date = pd.DataFrame({
    "DateKey": [int(d.strftime("%Y%m%d")) for d in date_range],
    "FullDate": date_range.strftime("%Y-%m-%d"),
    "NameDay": date_range.strftime("%A"),
    "NameMonthEN": date_range.strftime("%B"),
    "NumberQuarter": ((date_range.month - 1) // 3 + 1),
})

# 📌 Stap 3: Maak de tabel correct aan in SQL Server
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('DimDate', 'U') IS NOT NULL
            DROP TABLE DimDate;

        CREATE TABLE DimDate (
            DateKey BIGINT NOT NULL PRIMARY KEY,
            FullDate DATE NOT NULL,
            NameDay NVARCHAR(20) NOT NULL,
            NameMonthEN NVARCHAR(20) NOT NULL,
            NumberQuarter INT NOT NULL
        );
    """))
    conn.commit()

# 📌 Stap 4: Data invoegen met een BULK INSERT (betere prestatie)
df_dim_date.to_sql(table_name, engine, if_exists="append", index=False, dtype={
    "DateKey": sqlalchemy.types.BigInteger,
    "FullDate": sqlalchemy.types.Date,
    "NameDay": sqlalchemy.types.NVARCHAR(20),
    "NameMonthEN": sqlalchemy.types.NVARCHAR(20),
    "NumberQuarter": sqlalchemy.types.Integer,
})

print(f"✅ DimDate met {len(df_dim_date)} rijen succesvol weggeschreven naar {database}.{table_name}")

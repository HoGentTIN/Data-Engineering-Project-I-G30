import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Stap 1: Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
table_name = "DimTime"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 2: DimTime DataFrame genereren
time_range = pd.date_range(start="00:00:00", end="23:59:59", freq="s").time
df_dim_time = pd.DataFrame({
    "TimeKey": [int(f"{t.hour:02}{t.minute:02}{t.second:02}") for t in time_range],
    "FullTime": [t.strftime("%H:%M:%S") for t in time_range],
    "Hour": [t.hour for t in time_range],
    "Minutes": [t.minute for t in time_range],
    "Seconds": [t.second for t in time_range],
})

# 📌 Stap 3: Maak de tabel correct aan in SQL Server
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('DimTime', 'U') IS NOT NULL
            DROP TABLE DimTime;

        CREATE TABLE DimTime (
            TimeKey INT NOT NULL PRIMARY KEY,
            FullTime TIME NOT NULL,
            Hour INT NOT NULL,
            Minutes INT NOT NULL,
            Seconds INT NOT NULL
        );
    """))
    conn.commit()

# 📌 Stap 4: Data invoegen
df_dim_time.to_sql(table_name, engine, if_exists="append", index=False)

print(f"✅ DimTime met {len(df_dim_time)} rijen succesvol weggeschreven naar {database}.{table_name}")

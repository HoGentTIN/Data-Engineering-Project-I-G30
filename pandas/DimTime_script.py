import pandas as pd
import sqlalchemy

# 📌 Databaseverbinding instellen
server = "localhost"  # Of je SQL Server-instance
database = "EnergyDWH"
table_name = "DimTime"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# ⏰ DimTime DataFrame genereren (elke minuut van een dag)
time_range = pd.date_range(start="00:00:00", end="23:59:00", freq="T").time  # 🔥 'T' = elke minuut

df_dim_time = pd.DataFrame({
    "TimeKey": [int(f"{t.hour:02}{t.minute:02}") for t in time_range],  # 🔥 Per minuut (HHMM)
    "FullTime": [t.strftime("%H:%M:%S") for t in time_range],
    "Hour": [t.hour for t in time_range],
    "Minutes": [t.minute for t in time_range],
    "TimeAM_PM": ["AM" if t.hour < 12 else "PM" for t in time_range],
})

# 📌 Data wegschrijven naar SQL Server (tabel wordt vervangen)
df_dim_time.to_sql(table_name, engine, if_exists="replace", index=False)

print(f"✅ DimTime met {len(df_dim_time)} rijen (per minuut) succesvol weggeschreven naar {database}.{table_name}")

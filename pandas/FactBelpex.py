import pandas as pd
import sqlalchemy
import re
from sqlalchemy import text

# 📌 Stap 1: Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
table_name = "FactBelpex"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 2: CSV inlezen met juiste encoding en scheidingsteken
csv_path = "Data-Engineering-Project-I-G30\data\input\BelpexFilter.csv"
df_belpex = pd.read_csv(csv_path, encoding="ISO-8859-1", sep=";", dtype=str)  # ✅ Lees als string om fouten te voorkomen

# 📌 Debugging - Controleer of de kolommen correct zijn
print("📌 Kolommen in CSV:", df_belpex.columns)

# ✅ Stap 3: Datum en tijd splitsen naar DateKey en TimeKey
df_belpex["Date"] = pd.to_datetime(df_belpex["Date"].str.split(" ").str[0], format="%d/%m/%Y", errors="coerce")

# ✅ Tijd correct verwerken en TimeKey aanmaken in HHMM formaat
df_belpex["Time"] = df_belpex["Date"].astype(str).str.split(" ").str[1].fillna("00:00:00")
df_belpex["TimeKey"] = pd.to_datetime(df_belpex["Time"], format="%H:%M:%S", errors="coerce").dt.strftime("%H%M").astype(int)

# ✅ Maak DateKey in YYYYMMDD formaat
df_belpex["DateKey"] = df_belpex["Date"].dt.strftime("%Y%m%d").astype(int)

# 📌 Stap 4: Speciale tekens verwijderen en komma vervangen door punt
def clean_currency(value):
    if pd.isna(value) or value.strip() == "":
        return 0.0  # ✅ Voorkom fouten bij lege waarden
    value = re.sub(r"[^\d,]", "", value)  # Verwijder alle niet-numerieke tekens behalve komma
    return float(value.replace(",", "."))  # Vervang komma door punt en zet om naar float

df_belpex["Euro"] = df_belpex["Euro"].apply(clean_currency)

# 📌 Stap 5: Selecteer relevante kolommen
columns = ["DateKey", "TimeKey", "Euro"]
df_belpex = df_belpex[columns].rename(columns={"Euro": "BelpexPrice"})

# 📌 Stap 6: Maak de tabel correct aan in SQL Server
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('FactBelpex', 'U') IS NOT NULL
            DROP TABLE FactBelpex;

        CREATE TABLE FactBelpex (
            DateKey BIGINT NOT NULL,
            TimeKey INT NOT NULL,
            BelpexPrice FLOAT NOT NULL,
            FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
            FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey)
        );
    """))
    conn.commit()

print("✅ Tabel FactBelpex succesvol aangemaakt.")

# 📌 Stap 7: **Efficiënte validatie van DateKey en TimeKey**
with engine.connect() as conn:
    valid_keys = pd.read_sql("""
        SELECT DateKey, TimeKey FROM DimDate
        CROSS JOIN DimTime
    """, conn)

valid_dates = set(valid_keys["DateKey"])
valid_times = set(valid_keys["TimeKey"])

df_belpex = df_belpex[df_belpex["DateKey"].isin(valid_dates)]
df_belpex = df_belpex[df_belpex["TimeKey"].isin(valid_times)]

# 📌 Stap 8: Data wegschrijven naar SQL Server
df_belpex.to_sql(table_name, engine, if_exists="append", index=False)

# 📌 Stap 9: Controle uitvoeren
unique_dates_count = df_belpex["DateKey"].nunique()
print(f"✅ FactBelpex met {len(df_belpex)} records succesvol opgeslagen in {database}.{table_name}")
print(f"📊 Aantal unieke datums in dataset: {unique_dates_count}")

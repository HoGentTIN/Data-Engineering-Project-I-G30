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
csv_path = "BelpexFilter (1).csv"  # Pas aan naar jouw bestandspad
df_belpex = pd.read_csv(csv_path, encoding="ISO-8859-1", sep=";", dtype=str)  # Lees alles als string om fouten te voorkomen

# 📌 Debugging - Controleer of de kolommen correct zijn
print("📌 Kolommen in CSV:", df_belpex.columns)

# 📌 Stap 3: Datum en tijd splitsen naar DateKey en TimeKey
df_belpex["Date"] = pd.to_datetime(df_belpex["Date"].str.split(" ").str[0], format="%d/%m/%Y", errors="coerce")
df_belpex["Time"] = df_belpex["Date"].astype(str).str.split(" ").str[1]
df_belpex["Time"] = df_belpex["Time"].fillna("00:00:00")  # Vul lege tijdwaarden met "00:00:00"

# **Maak DateKey en TimeKey in het juiste formaat (integer)**
df_belpex["DateKey"] = df_belpex["Date"].dt.strftime("%Y%m%d").astype(int)
df_belpex["TimeKey"] = pd.to_datetime(df_belpex["Time"], format="%H:%M:%S", errors="coerce").dt.strftime("%H%M%S").astype(int)

# 📌 Stap 4: Speciale tekens verwijderen en komma vervangen door punt
def clean_currency(value):
    if isinstance(value, str):
        value = re.sub(r"[^\d,]", "", value)  # Verwijder alle niet-numerieke tekens behalve komma
        return value.replace(",", ".")  # Vervang komma door punt
    return value

df_belpex["Euro"] = df_belpex["Euro"].apply(clean_currency).astype(float)  # Converteer naar float

# 📌 Debugging - Controleer of Euro-kolom correct is geconverteerd
print("🔍 Unieke waarden in Euro-kolom:\n", df_belpex["Euro"].unique())

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

# 📌 Stap 7: Controleer of DateKey en TimeKey waarden geldig zijn
valid_dates = pd.read_sql("SELECT DateKey FROM DimDate", engine)["DateKey"].tolist()
valid_times = pd.read_sql("SELECT TimeKey FROM DimTime", engine)["TimeKey"].tolist()

df_belpex = df_belpex[df_belpex["DateKey"].isin(valid_dates)]
df_belpex = df_belpex[df_belpex["TimeKey"].isin(valid_times)]

# 📌 Stap 8: Data wegschrijven naar SQL Server
df_belpex.to_sql(table_name, engine, if_exists="append", index=False)

# 📌 Stap 9: Controle uitvoeren
unique_dates_count = df_belpex["DateKey"].nunique()
print(f"✅ FactBelpex met {len(df_belpex)} records succesvol opgeslagen in {database}.{table_name}")
print(f"📊 Aantal unieke datums in dataset: {unique_dates_count}")

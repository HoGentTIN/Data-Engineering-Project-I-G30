import os
import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
csv_path = r"C:\Users\smets\OneDrive\Documenten\Hogent\Jaar 2\semester2\DEP1\P6269_1_50_DMK_Sample_Elek.csv"
bulk_csv_path = "C:/Users/smets/OneDrive/Documenten/Hogent/Jaar 2/semester2/DEP1/uitgepakte_data/FactUser_Bulk.csv"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Tabellen opnieuw aanmaken
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('FactUser', 'U') IS NOT NULL
            DROP TABLE FactUser;

        IF OBJECT_ID('DimUser', 'U') IS NOT NULL
            DROP TABLE DimUser;

        CREATE TABLE DimUser (
            UserKey INT IDENTITY(1,1) PRIMARY KEY,
            EAN_ID INT NOT NULL UNIQUE,
            Warmtepomp_Indicator BIT NOT NULL,
            Elektrisch_Voertuig_Indicator BIT NOT NULL,
            PVInstallatieIndicator BIT NOT NULL,  -- ✅ SQL Server-naam gefixt
            Contract_Categorie NVARCHAR(50) NOT NULL
        );

        CREATE TABLE FactUser (
            DateKey BIGINT NOT NULL,
            TimeKey INT NOT NULL,
            UserKey INT NOT NULL,
            Volume_Afname_kWh FLOAT,
            Volume_Injectie_kWh FLOAT,
            FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
            FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey),
            FOREIGN KEY (UserKey) REFERENCES DimUser(UserKey)
        );
    """))
    conn.commit()

print("✅ Tabellen DimUser en FactUser succesvol aangemaakt.")

# 📌 Stap 1: Lees de CSV correct in
df_users = pd.read_csv(csv_path, sep=";", encoding="ISO-8859-1")

# 📌 Stap 2: Controleer en zoek de juiste kolomnamen automatisch
print("📂 Kolomnamen in de CSV:", df_users.columns.tolist())

date_col = [col for col in df_users.columns if "Datum" in col][0]
time_col = [col for col in df_users.columns if "Tijd" in col or "Startuur" in col][0]

# ✅ Fix de kolomnaam voor PV-Installatie_Indicator
df_users = df_users.rename(columns={"PV-Installatie_Indicator": "PVInstallatieIndicator"})

# 📌 Stap 3: Controleer en fix lege waarden in Datum_Startuur
print("🔍 Controleer eerste 10 waarden in Datum_Startuur:")
print(df_users[time_col].head(10))

# ✅ Datumformaat correct instellen
df_users[time_col] = pd.to_datetime(df_users[time_col], format="%Y-%m-%dT%H:%M:%S.%fZ", errors="coerce")

print(f"🔍 Aantal NaN in {time_col} na conversie: {df_users[time_col].isna().sum()}")  # Debugging

# Vervang NaN door standaardwaarden om crash te voorkomen
df_users[time_col] = df_users[time_col].fillna(pd.Timestamp("2000-01-01 00:00:00"))
df_users["DateKey"] = df_users[time_col].dt.strftime("%Y%m%d").astype(int)
df_users["TimeKey"] = df_users[time_col].dt.strftime("%H%M").astype(int)

# 📌 Stap 4: DimUser vullen en opslaan
dim_user_columns = [
    "EAN_ID",
    "Warmtepomp_Indicator",
    "Elektrisch_Voertuig_Indicator",
    "PVInstallatieIndicator",  # ✅ Gebruik correcte naam
    "Contract_Categorie"
]

df_dim_user = df_users[dim_user_columns].drop_duplicates()

df_dim_user.to_sql("DimUser", engine, if_exists="append", index=False)

# 📌 Stap 5: UserKey ophalen en koppelen
with engine.connect() as conn:
    user_map = pd.read_sql("SELECT UserKey, EAN_ID FROM DimUser", conn)

df_users = df_users.merge(user_map, on="EAN_ID", how="left")

# 📌 Stap 6: Opslaan in tijdelijk CSV-bestand voor BULK INSERT
fact_user_columns = ["DateKey", "TimeKey", "UserKey", "Volume_Afname_kWh", "Volume_Injectie_kWh"]
df_users[fact_user_columns].to_csv(bulk_csv_path, index=False, header=True)

print(f"✅ FactUser dataset voorbereid voor BULK INSERT. Start BULK INSERT...")

# 📌 Stap 7: BULK INSERT uitvoeren in SQL Server
bulk_insert_sql = f"""
    BULK INSERT FactUser
    FROM '{bulk_csv_path}'
    WITH (
        FORMAT = 'CSV',
        FIRSTROW = 2,  -- Skip de header
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\\n',
        TABLOCK
    );
"""

with engine.connect() as conn:
    conn.execute(text(bulk_insert_sql))
    conn.commit()

print(f"✅ BULK INSERT succesvol uitgevoerd voor FactUser vanuit {bulk_csv_path}")

# 📌 Stap 8: Controle uitvoeren
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM FactUser")).fetchone()
    print(f"📊 Aantal rijen in FactUser: {result[0]}")

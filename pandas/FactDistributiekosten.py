import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
table_name = "FactDistributiekosten"

# ✅ SQLAlchemy verbinding (zoals in andere scripts)
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 1: Maak de SQL-tabel FactDistributiekosten aan met een relatie naar DimDate
with engine.connect() as conn:
    conn.execute(text(f"""
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
            DROP TABLE {table_name};

        CREATE TABLE {table_name} (
            DateKey BIGINT NOT NULL,
            Intercommunal NVARCHAR(255),
            CapacityRate_DigitalMeter FLOAT,
            ConsumptionRate_DigitalMeter_Normal FLOAT,
            ConsumptionRate_DigitalMeter_ExclusiveNight FLOAT,
            CapacityRate_ClassicMeter FLOAT,
            ConsumptionRate_ClassicMeter_Normal FLOAT,
            ConsumptionRate_ClassicMeter_ExclusiveNight FLOAT,
            ProsumerRate FLOAT,
            DataManagementRate_YearAndMonthReadMeters FLOAT,
            DataManagementRate_QuarterReadMeters FLOAT,
            CONSTRAINT FK_FactDistributiekosten_DateKey FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
        );
    """))
    conn.commit()

print(f"✅ SQL-tabel {table_name} succesvol aangemaakt met relatie naar DimDate.")

# 📌 Stap 2: CSV-bestand inlezen
csv_file = "Data-Engineering-Project-I-G30\data\input\Distributiekosten.csv"
df_distributiekosten = pd.read_csv(csv_file, delimiter=",")

# ✅ Print de kolomnamen om te controleren
print("📂 Kolomnamen in de CSV:", df_distributiekosten.columns.tolist())

# 📌 Stap 3: Kolomnamen aanpassen aan SQL Server
df_distributiekosten = df_distributiekosten.rename(columns={
    "Intercommunale": "Intercommunal",
    "Capaciteitstarief_Digitale_meter": "CapacityRate_DigitalMeter",
    "Afnametarief_Digitale_meter_Normaal": "ConsumptionRate_DigitalMeter_Normal",
    "Afnametarief_Digitale_meter_Exclusief_nacht": "ConsumptionRate_DigitalMeter_ExclusiveNight",
    "Capaciteitstarief_Klassieke_meter": "CapacityRate_ClassicMeter",
    "Afnametarief_Klassieke_meter_Normaal": "ConsumptionRate_ClassicMeter_Normal",
    "Afnametarief_Klassieke_meter_Exclusief_nacht": "ConsumptionRate_ClassicMeter_ExclusiveNight",
    "Prosumententarief": "ProsumerRate",
    "Tarief_databeheer_Jaar_en_maandgelezen_meters": "DataManagementRate_YearAndMonthReadMeters",
    "Tarief_databeheer_Kwartiergelezen_meters": "DataManagementRate_QuarterReadMeters"
})

# 📌 Stap 4: Datumkolommen converteren naar DATE en DateKey genereren
df_distributiekosten["Van"] = pd.to_datetime(df_distributiekosten["Van"], format="%d/%m/%Y", errors="coerce")
df_distributiekosten["DateKey"] = df_distributiekosten["Van"].dt.strftime("%Y%m%d").astype(int)

# 📌 Stap 5: Relevante kolommen selecteren
df_distributiekosten = df_distributiekosten[[
    "DateKey", "Intercommunal", "CapacityRate_DigitalMeter",
    "ConsumptionRate_DigitalMeter_Normal", "ConsumptionRate_DigitalMeter_ExclusiveNight",
    "CapacityRate_ClassicMeter", "ConsumptionRate_ClassicMeter_Normal",
    "ConsumptionRate_ClassicMeter_ExclusiveNight", "ProsumerRate",
    "DataManagementRate_YearAndMonthReadMeters", "DataManagementRate_QuarterReadMeters"
]]

# 📌 Stap 6: Data naar SQL Server schrijven
df_distributiekosten.to_sql(table_name, engine, if_exists="append", index=False)

print(f"✅ {len(df_distributiekosten)} records succesvol ingeladen in {table_name}.")

# 📌 Stap 7: Controle uitvoeren
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
    print(f"📊 Aantal rijen in {table_name}: {result[0]}")

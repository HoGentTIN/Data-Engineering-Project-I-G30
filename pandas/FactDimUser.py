import pandas as pd
import zipfile
import sqlalchemy
from sqlalchemy import text

# 📌 Stap 1: Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 2: ZIP-bestand openen en CSV inlezen in chunks
zip_path = "P6269_1_50_DMK_Sample_Elek.zip"

with zipfile.ZipFile(zip_path, 'r') as z:
    file_name = z.namelist()[0]  # Pak de eerste file in de ZIP
    print(f"📂 Gevonden bestand in ZIP: {file_name}")

    # 📌 Stap 3: Foreign Keys verwijderen vóór het droppen van tabellen
    with engine.connect() as conn:
        conn.execute(text("""
            IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactUser_UserKey')
                ALTER TABLE FactUser DROP CONSTRAINT FK_FactUser_UserKey;

            IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactUser_DateKey')
                ALTER TABLE FactUser DROP CONSTRAINT FK_FactUser_DateKey;

            IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactUser_TimeKey')
                ALTER TABLE FactUser DROP CONSTRAINT FK_FactUser_TimeKey;

            IF OBJECT_ID('FactUser', 'U') IS NOT NULL
                DROP TABLE FactUser;

            IF OBJECT_ID('DimUser', 'U') IS NOT NULL
                DROP TABLE DimUser;

            CREATE TABLE DimUser (
                UserKey INT IDENTITY(1,1) PRIMARY KEY,
                EAN_ID INT NOT NULL UNIQUE,
                Warmtepomp_Indicator BIT NOT NULL,
                Elektrisch_Voertuig_Indicator BIT NOT NULL,
                PV_Installatie_Indicator BIT NOT NULL,
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

    # 📌 Stap 4: CSV inlezen in chunks en verwerken
    chunk_size = 100000
    chunk_count = 0

    with z.open(file_name) as file:
        for chunk in pd.read_csv(file, sep=",", encoding="ISO-8859-1", chunksize=chunk_size):
            chunk_count += 1
            print(f"📊 Verwerk chunk {chunk_count} met {len(chunk)} rijen...")

            # 📌 Stap 5: DateKey & TimeKey genereren
            chunk["Datum_Startuur"] = pd.to_datetime(chunk["Datum_Startuur"], format="%d/%m/%Y %H:%M:%S", errors="coerce")
            chunk["DateKey"] = chunk["Datum_Startuur"].dt.strftime("%Y%m%d").astype(int)
            chunk["TimeKey"] = chunk["Datum_Startuur"].dt.strftime("%H%M%S").astype(int)

            # 📌 Stap 6: DimUser vullen met unieke gebruikers
            dim_user_columns = ["EAN_ID", "Warmtepomp_Indicator", "Elektrisch_Voertuig_Indicator", "PV_Installatie_Indicator", "Contract_Categorie"]
            df_dim_user = chunk[dim_user_columns].drop_duplicates()

            df_dim_user.to_sql("DimUser", engine, if_exists="append", index=False)

            # 📌 Stap 7: UserKey ophalen uit DimUser
            with engine.connect() as conn:
                user_map = pd.read_sql("SELECT UserKey, EAN_ID FROM DimUser", conn)

            # 📌 Stap 8: UserKey koppelen aan FactUser
            chunk = chunk.merge(user_map, on="EAN_ID", how="left")

            # 📌 Stap 9: FactUser vullen en wegschrijven naar SQL Server
            fact_user_columns = ["DateKey", "TimeKey", "UserKey", "Volume_Afname_kWh", "Volume_Injectie_kWh"]
            df_fact_user = chunk[fact_user_columns]

            df_fact_user.to_sql("FactUser", engine, if_exists="append", index=False)

    print(f"✅ Alle {chunk_count} chunks succesvol verwerkt en opgeslagen in {database}.FactUser")

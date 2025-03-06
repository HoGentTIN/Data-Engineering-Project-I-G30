import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
csv_path = "Data-Engineering-Project-I-G30\data\input\Bijdragen.csv"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 1: Maak de SQL-tabel FactHeffingen aan en voeg een foreign key naar DimDate toe
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('FactHeffingen', 'U') IS NOT NULL
            DROP TABLE FactHeffingen;

        CREATE TABLE FactHeffingen (
            DateKey BIGINT NOT NULL,
            Energiebijdrage FLOAT NOT NULL,
            Federale_bijdrage_0_3000 FLOAT NOT NULL,
            Federale_bijdrage_3000_20000 FLOAT NOT NULL,
            Federale_bijdrage_20000_50000 FLOAT NOT NULL,
            Federale_bijdrage_50000_100000 FLOAT NOT NULL,
            Bijdrage_energiefonds_residentiele_gebruiker FLOAT NOT NULL,
            Bijdrage_energiefonds_niet_residentiele_gebruiker FLOAT NOT NULL,
            CONSTRAINT FK_FactHeffingen_DateKey FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
        );
    """))
    conn.commit()

print("✅ SQL-tabel FactHeffingen succesvol aangemaakt met relatie naar DimDate.")

# 📌 Stap 2: CSV inlezen
df_heffingen = pd.read_csv(csv_path, sep=",", encoding="ISO-8859-1")

# 📌 Stap 3: Controleer kolomnamen en fix encodingproblemen
print("📂 Kolomnamen in de CSV:", df_heffingen.columns.tolist())

df_heffingen = df_heffingen.rename(columns={
    "Bijdrage_energiefonds_residentiÃ«le_gebruiker": "Bijdrage_energiefonds_residentiele_gebruiker",
    "Bijdrage_energiefonds_niet_residentiÃ«le_gebruiker": "Bijdrage_energiefonds_niet_residentiele_gebruiker"
})

# 📌 Stap 4: Datumkolommen omzetten naar DATE en DateKey aanmaken
df_heffingen["Van"] = pd.to_datetime(df_heffingen["Van"], format="%d/%m/%Y")
df_heffingen["DateKey"] = df_heffingen["Van"].dt.strftime("%Y%m%d").astype(int)

# 📌 Stap 5: Relevante kolommen selecteren
df_heffingen = df_heffingen[[
    "DateKey", "Energiebijdrage", "Federale_bijdrage_0_3000", "Federale_bijdrage_3000_20000",
    "Federale_bijdrage_20000_50000", "Federale_bijdrage_50000_100000",
    "Bijdrage_energiefonds_residentiele_gebruiker", "Bijdrage_energiefonds_niet_residentiele_gebruiker"
]]

# 📌 Stap 6: Data naar SQL Server schrijven
df_heffingen.to_sql("FactHeffingen", engine, if_exists="append", index=False)

print(f"✅ {len(df_heffingen)} records succesvol ingeladen in FactHeffingen.")

# 📌

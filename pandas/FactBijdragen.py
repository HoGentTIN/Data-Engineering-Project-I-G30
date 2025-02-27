import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
csv_path = r"C:\Users\smets\OneDrive\Documenten\Hogent\Jaar 2\semester2\DEP1\Bijdragen.csv"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 1: Maak de SQL-tabel aan als deze nog niet bestaat
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('FactHeffingen', 'U') IS NOT NULL
            DROP TABLE FactHeffingen;

        CREATE TABLE FactHeffingen (
            Van DATE NOT NULL,
            Tot DATE NOT NULL,
            Energiebijdrage FLOAT NOT NULL,
            Federale_bijdrage_0_3000 FLOAT NOT NULL,
            Federale_bijdrage_3000_20000 FLOAT NOT NULL,
            Federale_bijdrage_20000_50000 FLOAT NOT NULL,
            Federale_bijdrage_50000_100000 FLOAT NOT NULL,
            Bijdrage_energiefonds_residentiele_gebruiker FLOAT NOT NULL,
            Bijdrage_energiefonds_niet_residentiele_gebruiker FLOAT NOT NULL
        );
    """))
    conn.commit()

print("✅ SQL-tabel FactHeffingen succesvol aangemaakt.")

# 📌 Stap 2: CSV inlezen
df_heffingen = pd.read_csv(csv_path, sep=",", encoding="ISO-8859-1")

# 📌 Stap 3: Controleer kolomnamen en fix encodingproblemen
print("📂 Kolomnamen in de CSV:", df_heffingen.columns.tolist())

df_heffingen = df_heffingen.rename(columns={
    "Bijdrage_energiefonds_residentiÃ«le_gebruiker": "Bijdrage_energiefonds_residentiele_gebruiker",
    "Bijdrage_energiefonds_niet_residentiÃ«le_gebruiker": "Bijdrage_energiefonds_niet_residentiele_gebruiker"
})

# 📌 Stap 4: Datumkolommen omzetten naar DATE
df_heffingen["Van"] = pd.to_datetime(df_heffingen["Van"], format="%d/%m/%Y")
df_heffingen["Tot"] = pd.to_datetime(df_heffingen["Tot"], format="%d/%m/%Y")

# 📌 Stap 5: Data naar SQL Server schrijven
df_heffingen.to_sql("FactHeffingen", engine, if_exists="append", index=False)

print(f"✅ {len(df_heffingen)} records succesvol ingeladen in FactHeffingen.")

# 📌 Stap 6: Controle uitvoeren
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM FactHeffingen")).fetchone()
    print(f"📊 Aantal rijen in FactHeffingen: {result[0]}")

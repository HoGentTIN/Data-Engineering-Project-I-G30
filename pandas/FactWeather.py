import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Stap 1: Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
table_name = "FactWeather"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 2: CSV inlezen (Pas dit pad aan als nodig)
csv_path = "aws_1day.csv"
df_weather = pd.read_csv(csv_path)

# 📌 Stap 3: Kolomnamen controleren
print("📌 Kolommen in CSV:", df_weather.columns)

# 📌 Stap 4: Foreign Keys genereren
df_weather["DateKey"] = pd.to_datetime(df_weather["timestamp"]).dt.strftime("%Y%m%d").astype(int)
df_weather["TimeKey"] = pd.to_datetime(df_weather["timestamp"]).dt.strftime("%H%M%S").astype(int)
df_weather = df_weather.rename(columns={"code": "WeatherStationKey"})

# 📌 Stap 5: Alleen de correcte kolommen selecteren
columns = ["DateKey", "TimeKey", "WeatherStationKey", "precip_quantity", "temp_avg", "temp_max", "temp_min"]
df_weather = df_weather[columns]

# 📌 Stap 6: Controleer welke WeatherStationKeys in DimWeatherStation bestaan
with engine.connect() as conn:
    existing_stations = pd.read_sql("SELECT WeatherStationID FROM DimWeatherStation", conn)["WeatherStationID"].tolist()

# 📌 Stap 7: Zet NULL voor ontbrekende WeatherStationKeys
df_weather.loc[~df_weather["WeatherStationKey"].isin(existing_stations), "WeatherStationKey"] = None

# 📌 Debugging: Controleer hoeveel waarden NULL zijn
missing_count = df_weather["WeatherStationKey"].isna().sum()
print(f"🔍 Aantal records met NULL in WeatherStationKey: {missing_count}")

# 📌 Stap 8: Maak de tabel correct aan in SQL Server
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('FactWeather', 'U') IS NOT NULL
            DROP TABLE FactWeather;

        CREATE TABLE FactWeather (
            DateKey BIGINT NOT NULL,
            TimeKey INT NOT NULL,
            WeatherStationKey INT NULL,
            precip_quantity FLOAT,
            temp_avg FLOAT,
            temp_max FLOAT,
            temp_min FLOAT,
            FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
            FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey),
            FOREIGN KEY (WeatherStationKey) REFERENCES DimWeatherStation(WeatherStationID) ON DELETE SET NULL
        );
    """))
    conn.commit()

# 📌 Stap 9: Data invoegen
df_weather.to_sql(table_name, engine, if_exists="append", index=False)

print(f"✅ FactWeather met {len(df_weather)} records succesvol opgeslagen in {database}.{table_name}")

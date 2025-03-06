import pandas as pd
import sqlalchemy
from sqlalchemy import text

# 📌 Stap 1: Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
table_name = "FactWeather"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 2: CSV inlezen (Pas dit pad aan als nodig)
csv_path = "Data-Engineering-Project-I-G30\data\input\aws_1day.csv"
df_weather = pd.read_csv(csv_path)

# 📌 Stap 3: Kolomnamen controleren
print("📌 Kolommen in CSV:", df_weather.columns)

# 📌 Stap 4: Foreign Keys genereren
df_weather["DateKey"] = pd.to_datetime(df_weather["timestamp"]).dt.strftime("%Y%m%d").astype(int)
df_weather["TimeKey"] = pd.to_datetime(df_weather["timestamp"]).dt.strftime("%H%M").astype(int)  # ✅ HHMM formaat
df_weather = df_weather.rename(columns={"code": "WeatherStationKey"})

# 📌 Stap 5: Controleer welke `WeatherStationKeys`, `DateKey`, en `TimeKey` geldig zijn
with engine.connect() as conn:
    valid_keys = pd.read_sql("""
        SELECT DateKey, TimeKey, WeatherStationID FROM DimDate
        CROSS JOIN DimTime
        LEFT JOIN DimWeatherStation ON 1=1
    """, conn)

valid_dates = set(valid_keys["DateKey"].dropna())
valid_times = set(valid_keys["TimeKey"].dropna())
valid_stations = set(valid_keys["WeatherStationID"].dropna())

# 📌 Stap 6: Ongeldige waarden verwijderen (in plaats van pas later NULL zetten)
df_weather = df_weather[df_weather["DateKey"].isin(valid_dates)]
df_weather = df_weather[df_weather["TimeKey"].isin(valid_times)]
df_weather["WeatherStationKey"] = df_weather["WeatherStationKey"].apply(lambda x: x if x in valid_stations else None)

# 📌 Stap 7: NULL-waarden in numerieke kolommen vervangen door standaardwaarde (0.0)
numeric_cols = ["precip_quantity", "temp_avg", "temp_max", "temp_min"]
df_weather[numeric_cols] = df_weather[numeric_cols].fillna(0.0)

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
            precip_quantity FLOAT DEFAULT 0.0,
            temp_avg FLOAT DEFAULT 0.0,
            temp_max FLOAT DEFAULT 0.0,
            temp_min FLOAT DEFAULT 0.0,
            FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
            FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey),
            FOREIGN KEY (WeatherStationKey) REFERENCES DimWeatherStation(WeatherStationID)
        );
    """))
    conn.commit()

# 📌 Stap 9: Data invoegen
df_weather.to_sql(table_name, engine, if_exists="append", index=False)

print(f"✅ FactWeather met {len(df_weather)} records succesvol opgeslagen in {database}.{table_name}")

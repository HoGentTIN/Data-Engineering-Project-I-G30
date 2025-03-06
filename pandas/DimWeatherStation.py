import pandas as pd
import sqlalchemy
import re
from sqlalchemy import text

# 📌 Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
table_name = "DimWeatherStation"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 CSV inlezen
csv_path = "Data-Engineering-Project-I-G30\data\input\aws_station.csv"
df_weather = pd.read_csv(csv_path)

# 📌 Latitude & Longitude extraheren
def extract_lat_lon(point):
    match = re.findall(r"[-+]?\d*\.\d+|\d+", point)
    return float(match[0]), float(match[1]) if len(match) == 2 else (None, None)

df_weather[["Latitude", "Longitude"]] = df_weather["the_geom"].apply(lambda x: pd.Series(extract_lat_lon(x)))

# 📌 Kolommen hernoemen
df_weather = df_weather.rename(columns={"code": "WeatherStationID", "name": "WeatherStationName", "altitude": "Altitude"})
df_weather = df_weather[["WeatherStationID", "WeatherStationName", "Latitude", "Longitude", "Altitude"]]

# 📌 Maak de tabel correct aan in SQL Server
with engine.connect() as conn:
    conn.execute(text("""
        IF OBJECT_ID('DimWeatherStation', 'U') IS NOT NULL
            DROP TABLE DimWeatherStation;

        CREATE TABLE DimWeatherStation (
            WeatherStationID INT NOT NULL PRIMARY KEY,
            WeatherStationName NVARCHAR(100) NOT NULL,
            Latitude FLOAT NOT NULL,
            Longitude FLOAT NOT NULL,
            Altitude FLOAT NOT NULL
        );
    """))
    conn.commit()

# 📌 Data invoegen
df_weather.to_sql(table_name, engine, if_exists="append", index=False)

print(f"✅ DimWeatherStation met {len(df_weather)} records succesvol weggeschreven naar {database}.{table_name}")

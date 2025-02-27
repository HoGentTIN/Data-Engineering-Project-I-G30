import sqlalchemy
from sqlalchemy import text

# 📌 Stap 1: Databaseverbinding instellen
server = "localhost"
database = "EnergyDWH"
engine = sqlalchemy.create_engine(f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server")

# 📌 Stap 2: BELPEX-RLP-M waarde voor de maand instellen
belpex_rlp_m_value = 85.34  # Pas dit aan naar de correcte maandelijkse waarde
start_date_key = 20250101  # Begin van de maand
end_date_key = 20250131    # Einde van de maand

# 📌 Stap 3: UPDATE uitvoeren in SQL Server haldzdlo
with engine.connect() as conn:
    conn.execute(text(""" 
        UPDATE FactBelpex
        SET BELPEX_RLP_MPrice = :value
        WHERE DateKey BETWEEN :start_date AND :end_date;
    """), {"value": belpex_rlp_m_value, "start_date": start_date_key, "end_date": end_date_key})

print(f"✅ BELPEX_RLP_MPrice geüpdatet naar {belpex_rlp_m_value} voor {start_date_key} - {end_date_key}")

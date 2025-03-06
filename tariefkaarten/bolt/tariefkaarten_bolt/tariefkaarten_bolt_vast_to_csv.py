import os
import re
import csv
import PyPDF2

# Paden die generiek ingevuld kunnen worden
pathName = "tariefkaarten/bolt/tariefkaarten_bolt/tariefkaarten_bolt_vast"
csv_file = "tariefkaarten/bolt/tariefkaarten_bolt/tariefkaarten_bolt_vast.csv"

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9, ]+', '', text)

def extract_info_from_pdf(pdf_path):
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())

    month_year_match = re.search(r'([A-Za-z]+)\s*(\d{4})\s*-\s*residentieel', text, re.IGNORECASE)

    # Prijzen zoeken
    price_regex = r'c€\s*([\d,]+)\s*/kWh'
    price_matches = re.findall(price_regex, text, re.S)

    # Vaste vergoeding zoeken
    fixed_cost_match = re.search(r'€\s*([\d,]+)\s*/maand\s*Platformkost', text)
    fixed_cost = float(fixed_cost_match.group(1).replace(',', '.')) * 12 if fixed_cost_match else "Onbekend"

    # Belpex-formule zoeken
    belpex_match = re.search(r'Belpex\s*\*\s*([\d.,]+)\s*-\s*([\d.,]+)', text)
    meterfactor = belpex_match.group(1) if belpex_match else "Onbekend"
    balanseringskost = belpex_match.group(2) if belpex_match else "Onbekend"

    data = {
        "Maand": clean_text(month_year_match.group(1)) if month_year_match else "Onbekend",
        "Jaar": clean_text(month_year_match.group(2)) if month_year_match else "Onbekend",
        "Leverancier": "Bolt",
        "Enkelvoudig": price_matches[0] if len(price_matches) > 0 else "Onbekend",
        "Dag": price_matches[1] if len(price_matches) > 1 else "Onbekend",
        "Nacht": price_matches[2] if len(price_matches) > 2 else "Onbekend",
        "Excl_nacht": price_matches[3] if len(price_matches) > 3 else "Onbekend",
        "Vaste_vergoeding": fixed_cost,
        "Meterfactor": meterfactor,
        "Balanseringskost": balanseringskost
    }
    return data


def process_pdfs(directory, output_csv):
    directory = os.path.expanduser(directory)
    if not os.path.exists(directory):
        print("Directory bestaat niet.")
        return
    
    with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
        # Pas de delimiter aan naar puntkomma
        writer = csv.DictWriter(file, fieldnames=["Maand", "Jaar", "Leverancier", "Enkelvoudig", "Dag", "Nacht", "Excl_nacht", "Vaste_vergoeding", "Meterfactor", "Balanseringskost"], delimiter=';')
        writer.writeheader()
        
        for filename in os.listdir(directory):
            if filename.endswith(".pdf"):
                pdf_path = os.path.join(directory, filename)
                data = extract_info_from_pdf(pdf_path)
                writer.writerow(data)
                print(f"Gegevens opgeslagen voor {filename}")

if __name__ == "__main__":
    process_pdfs(pathName, csv_file)

import os
import re
import csv
import PyPDF2

pathName = "tariefkaarten/bolt/tariefkaarten_bolt/tariefkaarten_bolt_elektriciteit"
csv_file = "tariefkaarten/bolt/tariefkaarten_bolt/tariefkaarten_bolt_elektriciteit.csv"

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9, ]+', '', text)

def extract_info_from_pdf(pdf_path):
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())
    
    # Zoek naar de maand en het jaar in de tekst
    month_year_match = re.search(r'([A-Za-z]+)\s*(\d{4})\s*-\s*residentieel', text, re.IGNORECASE)
    
    # Regex voor de Meterfactor en Balanseringskost
    meterfactor_regex = r"Belpex \* ([0-9]{1,3}(?:,\d{3})*(?:\.\d+)?)"
    balanseringskost_regex = r"\+ ([0-9]{1,3}(?:,\d{3})*(?:\.\d+)?)"

    # Zoek naar de Meterfactor en Balanseringskost
    meterfactor_match = re.search(meterfactor_regex, text)
    balanseringskost_match = re.search(balanseringskost_regex, text)

    data = {
        "Maand": clean_text(month_year_match.group(1)) if month_year_match else "Onbekend",
        "Jaar": clean_text(month_year_match.group(2)) if month_year_match else "Onbekend",
        "Leverancier": "Bolt",
        "Meterfactor": meterfactor_match.group(1) if meterfactor_match else "Onbekend",
        "Balanseringskost": balanseringskost_match.group(1) if balanseringskost_match else "Onbekend",
    }
    return data

def process_pdfs(directory, output_csv):
    directory = os.path.expanduser(directory)
    if not os.path.exists(directory):
        print("Directory bestaat niet.")
        return
    
    with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
        # Pas de delimiter aan naar puntkomma
        writer = csv.DictWriter(file, fieldnames=["Maand", "Jaar", "Leverancier", "Meterfactor", "Balanseringskost"], delimiter=';')
        writer.writeheader()
        
        for filename in os.listdir(directory):
            if filename.endswith(".pdf"):
                pdf_path = os.path.join(directory, filename)
                data = extract_info_from_pdf(pdf_path)
                writer.writerow(data)
                print(f"Gegevens opgeslagen voor {filename}")

if __name__ == "__main__":
    process_pdfs(pathName, csv_file)

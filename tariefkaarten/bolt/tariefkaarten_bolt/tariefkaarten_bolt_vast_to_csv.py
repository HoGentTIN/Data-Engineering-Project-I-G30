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
    
    # Zoek naar de maand en het jaar in de tekst
    month_year_match = re.search(r'([A-Za-z]+)\s*(\d{4})\s*-\s*residentieel', text, re.IGNORECASE)
    
    # Verbeterde regex voor het "vast bedrag"
    price_regex = r'c€([0-9]{1,3}(?:[\.,][0-9]{2}))\/kWh'
    price_match = re.search(price_regex, text)
    
    # Zorg ervoor dat alleen de eerste prijs wordt gebruikt
    if price_match:
        print(f"Gevonden prijs: {price_match.group(1)}")  # Debugging om te zien wat er wordt gevonden
    
    data = {
        "Maand": clean_text(month_year_match.group(1)) if month_year_match else "Onbekend",
        "Jaar": clean_text(month_year_match.group(2)) if month_year_match else "Onbekend",
        "Leverancier": "Bolt",
        "Vast Bedrag": price_match.group(1) if price_match else "Onbekend",  # Dit is het getal na 'c€' en voor '/kWh'
    }
    return data

def process_pdfs(directory, output_csv):
    directory = os.path.expanduser(directory)
    if not os.path.exists(directory):
        print("Directory bestaat niet.")
        return
    
    with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
        # Pas de delimiter aan naar puntkomma
        writer = csv.DictWriter(file, fieldnames=["Maand", "Jaar", "Leverancier", "Vast Bedrag"], delimiter=';')
        writer.writeheader()
        
        for filename in os.listdir(directory):
            if filename.endswith(".pdf"):
                pdf_path = os.path.join(directory, filename)
                data = extract_info_from_pdf(pdf_path)
                writer.writerow(data)
                print(f"Gegevens opgeslagen voor {filename}")

if __name__ == "__main__":
    process_pdfs(pathName, csv_file)

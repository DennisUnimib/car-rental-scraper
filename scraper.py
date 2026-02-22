import os
import re
from datetime import datetime
from playwright.sync_api import sync_playwright
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv()

def scrape_sixt():
    url = os.environ.get("SIXT_SCRAPE_URL")

    with sync_playwright() as p:
        print("Avvio browser...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print(f"Navigazione verso: {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            print(f"Errore durante la navigazione: {e}")
            # Proseguiamo comunque, magari il DOM è carico abbastanza

        # Gestione Cookie Banner
        try:
            print("Controllo banner cookie...")
            # Cerchiamo il bottone in modo più flessibile
            page.wait_for_selector('button:has-text("ACCETTA")', timeout=10000)
            accept_button = page.get_by_role("button", name=re.compile("ACCETTA TUTTO|ACCEPT ALL|ACCETTA", re.IGNORECASE))
            if accept_button.is_visible():
                accept_button.click()
                print("Cookie accettati.")
        except Exception as e:
            print(f"Nessun banner cookie trovato o già accettato.")

        # Attesa caricamento offerte
        print("In attesa del caricamento delle offerte...")
        try:
            # Aspettiamo che appaia almeno un h4 (categoria) o un bottone di offerta
            page.wait_for_selector('h4', timeout=45000)
        except Exception as e:
            print(f"Offerte non caricate entro il timeout. Provo a catturare uno screenshot.")
            page.screenshot(path="debug_state.png")
            browser.close()
            return

        # Scrolling per caricare tutte le offerte (se necessario)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Estrazione dati
        offers_data = []
        # Troviamo tutti i bottoni che rappresentano una card di offerta
        cards = page.query_selector_all('[data-testid="rent-offer-list-tile"]')
        print(f"Trovate {len(cards)} offerte.")

        for card in cards:
            try:
                # Categoria (es. STANDARD BERLINA)
                category_el = card.query_selector('h4')
                category = category_el.inner_text().strip() if category_el else "N/D"

                # Modello (solitamente il primo tag p all'interno della card)
                model_el = card.query_selector('p')
                model = model_el.inner_text().strip() if model_el else "N/D"

                # Prezzi 
                price_text = card.inner_text()
                
                # Regex migliorata per estrarre i prezzi (es. 32,32 o 32.32)
                # Al giorno
                day_price_match = re.search(r'(\d+[,\.]\d+)\s*€\s*/\s*giorno', price_text)
                day_price = day_price_match.group(1) if day_price_match else "N/D"

                # Prezzo Totale
                total_price_match = re.search(r'(\d+[,\.]\d+)\s*€\s*totale', price_text)
                total_price = total_price_match.group(1) if total_price_match else "N/D"

                offers_data.append({
                    'Categoria': category,
                    'Modello': model,
                    'Prezzo al Giorno (€)': day_price,
                    'Prezzo Totale (€)': total_price,
                    'Data Scraping': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            except Exception as e:
                print(f"Errore nell'estrarre dati da una card: {e}")

        browser.close()

        if offers_data:
            # 1. Salvataggio in CSV (Locale)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sixt_prices_{timestamp}.csv"
            
            df = pd.DataFrame(offers_data)
            df.to_csv(filename, index=False, encoding='utf-8-sig', sep=';')
            print(f"Scraping completato. Salvati {len(offers_data)} record in: {filename}")

            # 2. Salvataggio su Supabase
            save_to_supabase(offers_data)
        else:
            print("Nessun dato recuperato.")

def save_to_supabase(offers_data):
    """Invia i dati estratti a una tabella su Supabase."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        print("ATTENZIONE: SUPABASE_URL o SUPABASE_KEY non configurati nel file .env. Salto il salvataggio su DB.")
        return

    try:
        supabase: Client = create_client(url, key)
        
        # Prepariamo i dati per il DB (pulizia prezzi)
        db_data = []
        for offer in offers_data:
            # Converte "32,50" in 32.50 float
            def clean_price(p):
                if p == "N/D": return None
                try:
                    return float(p.replace(',', '.'))
                except:
                    return None

            db_data.append({
                "categoria": offer['Categoria'],
                "modello": offer['Modello'],
                "prezzo_giorno": clean_price(offer['Prezzo al Giorno (€)']),
                "prezzo_totale": clean_price(offer['Prezzo Totale (€)']),
                "data_scraping": offer['Data Scraping']
            })

        # Inserimento nella tabella 'sixt_car_prices'
        response = supabase.table("sixt_car_prices").insert(db_data).execute()
        print(f"Dati inviati con successo a Supabase (tabella 'sixt_car_prices').")
    except Exception as e:
        print(f"Errore durante il salvataggio su Supabase: {e}")

if __name__ == "__main__":
    scrape_sixt()

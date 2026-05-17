"""
scraper.py - Autonomous data download from the internet

This script makes the pipeline fully autonomous:
no files need to be downloaded manually by the user.

It checks every CHECK_INTERVAL_HOURS whether updated versions
of each dataset exist online. It only downloads if something new is found.

Data sources:
  1. Inside Airbnb — listings + calendar NYC
  2. Zillow ZORI — monthly rent index by ZIP code
  3. US Census ACS5 — median income + population by ZIP code

Persistent state: data/raw/.scraper_state.json
  Tracks what has already been downloaded to avoid re-downloading.
"""
import os
import re
import sys
import json
import gzip
import time
import logging
import requests
import csv
from datetime import datetime

sys.path.insert(0, "/app") 
#sys.path.insert(0, "/app") aggiunge /app come prima cartella dove cercare, path di docker non path fisico

from pipeline.config import (
    DATA_RAW_PATH, FILES, KAFKA_BROKER, KAFKA_TOPIC,
    CHECK_INTERVAL_HOURS
)

#configura il sistema di logging
logging.basicConfig(
    level= logging.INFO, #mostra i messaggi di livello INFO e superiori (INFO, WARNING, ERROR)
    format = "%(asctime)s [SCRAPER] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)
#crea un logger specifico per questo file. __name__ è il nome del modulo corrente (es. pipeline.scraper).

#Path of the state file (scraper memory)
STATE_FILE = os.path.join(DATA_RAW_PATH, ".scraper_state.json")
#Source URLs
AIRBNB_PAGE_URL = "https://insideairbnb.com/get-the-data/"
ZILLOW_URL = "https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"
CENSUS_BASE_URL = "https://api.census.gov/data"

def load_state() -> dict:
    """
    Reads .scraper_state.json from data/raw/.
    If the file does not exist (first run), returns an empty dict.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE,"r") as f:
            return json.load(f)
    return {}

def save_state(state: dict):
    """
    Saves the updated state to .scraper_state.json.
    The file lives in data/raw/ which is a Docker volume —
    it survives container restarts.
    """
    os.makedirs(DATA_RAW_PATH, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent= 2)
        log.info(f"State saved: {state}")
    
def download_file(url, dest_path):
    """
    Downloads a file in streaming mode with 1MB chunks.
    Logs progress every 10MB to show activity on large files
    """
    log.info(f"Downloading: {url}")
    log.info(f"  -> {dest_path}")

    response = requests.get(url, stream= True, timeout=120) 
    #stream=True → non scaricare tutto il file subito in memoria, 
    # dammi i dati a pezzi man mano. Senza questo, un file da 30MB 
    # verrebbe caricato tutto in RAM prima di poterlo scrivere su disco
    response.raise_for_status() #Controlla se la risposta HTTP è un errore 

    downloaded = 0
    chunk_size = 1024 * 1024 #1Mb per chunk

    with open(dest_path, "wb") as f: #wb scrittura binaria perchè stiamo scrivendo file compressi
        for chunk in response.iter_content(chunk_size= chunk_size): #Legge la risposta HTTP un pezzo alla volta da 1MB. iter_content restituisce i dati a pezzi invece di tutto in una volta 
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded % (10*1024*1024) == 0: #stampa ogni 10 mb
                    log.info(f"  Downloaded {downloaded // (1024*1024)} MB..")
    
    log.info(f"  Download complete ({downloaded // (1024*1024)} MB)")


def get_available_inside_airbnb_nyc_dates() -> list:
    """
    Downloads the HTML page of insideairbnb.com/get-the-data/ and
    finds all links containing 'new-york-city'.
    Extracts available dates with regex and sorts them newest first.

    Returns a list of date strings: ["2026-02-13", "2025-12-10", ...]
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.error("BeautifulSoup4 not installed.")
        return []
    log.info("Checking available versions on Inside Airbnb..")
    response = requests.get(AIRBNB_PAGE_URL,timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    
    dates = set()
    for a in soup.find_all("a", href = True):
        href = a["href"]
        if "new-york-city" in href:
            # Extract date from URL path: .../new-york-city/2026-02-13/...
            match = re.search(r"new-york-city/(\d{4}-\d{2}-\d{2})/",href)
            if match:
                dates.add(match.group(1)) #solo il contenuto della prima coppia di parentesi
    
    sorted_dates = sorted(dates, reverse=True)
    log.info(f"Available dates: {sorted_dates[:5]}..")
    return sorted_dates


def validate_airbnb_join_from_files(listings_path: str, calendar_path: str) -> bool:
    """
    Validates that listings and calendar can be joined by sampling 200 listing IDs.

    Known Inside Airbnb issue: listings and calendar files are generated
    at different times so IDs may not match between the two files.

    Strategy:
    1. Read the first 200 listing IDs from listings.gz
    2. Scan the first 10,000 rows of calendar.gz
    3. If at least 1 ID matches -> join is valid
    """
    log.info("Validating listings <-> calendar join...")

    # Read first 200 listing IDs from listings
    listings_ids = set()
    with gzip.open(listings_path, "rt", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)  # DictReader reads the CSV and returns dicts per row
        for i, row in enumerate(reader):
            if i >= 200:
                break
            listings_ids.add(str(row.get("id", "")).strip())

    if not listings_ids:
        log.warning("No listings IDs found in listings file")
        return False

    log.info(f"Sampled {len(listings_ids)} listings IDs from listings")

    # Search for these IDs in the first 10,000 rows of calendar
    matches = 0
    with gzip.open(calendar_path, "rt", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 10000:
                break
            if str(row.get("listing_id", "")).strip() in listings_ids:
                matches += 1

    log.info(f"Matches found: {matches}")

    if matches >= 1:
        log.info("Join is valid - listings and calendar share IDs")
        return True
    else:
        log.warning("Join is NOT valid - IDs do not match")
        return False
    
def scrape_inside_airbnb(state: dict) -> bool:
    """
    Checks and downloads the most recent joinable Inside Airbnb NYC files.
    Iterates dates from newest to oldest.
    For each date:
      - Downloads listings and calendar to temporary files (.tmp)
      - Validates that the join is possible
      - If valid: renames to final files
      - If not valid: deletes .tmp and tries the previous date

    Returns True if new files were downloaded, False otherwise.
    """
    dates = get_available_inside_airbnb_nyc_dates()
    if not dates:
        log.error("No dates found on Inside Airbnb")
        return False
        
    current_date = state.get("inside_airbnb_date")
    latest_date = dates[0]

    if current_date == latest_date:
        log.info(f"Inside Airbnb already up to date ({current_date}) - skip")
        return False
    
    log.info(f"New version available: {latest_date} (current: {current_date})")

    for date in dates:
        log.info(f"Trying version {date}...")

        base = f"https://data.insideairbnb.com/united-states/ny/new-york-city/{date}/data"
        listings_url = f"{base}/listings.csv.gz"
        calendar_url = f"{base}/calendar.csv.gz"

        listings_tmp = os.path.join(DATA_RAW_PATH, FILES["listings"] + ".tmp")
        calendar_tmp = os.path.join(DATA_RAW_PATH, FILES["calendar"] + ".tmp")

        try:
            download_file(listings_url,listings_tmp)
            download_file(calendar_url,calendar_tmp)
        except Exception as e:
            log.warning(f"Download error for {date}:{e} - trying previous date")
            for f_tmp in [listings_tmp, calendar_tmp]:
                if os.path.exists(f_tmp):
                    os.remove(f_tmp)
            continue

        if validate_airbnb_join_from_files(listings_tmp, calendar_tmp):
            listings_final = os.path.join(DATA_RAW_PATH, FILES["listings"])
            calendar_final = os.path.join(DATA_RAW_PATH, FILES["calendar"])
            os.replace(listings_tmp,listings_final)
            os.replace(calendar_tmp,calendar_final)
            state["inside_airbnb_date"] = date
            log.info(f"Inside Airbnb updated to version {date}")
            return True
        else:
            log.warning(f"Version {date} not joinable - trying previous date")
            for f_tmp in [listings_tmp, calendar_tmp]:
                if os.path.exists(f_tmp):
                    os.remove(f_tmp)

        log.error("No joinable version found on Inside Airbnb")
        return False
    
def get_zillow_latest_column() -> str:
    """
    Reads ONLY the first row of the Zillow CSV in streaming mode
    to extract the latest date column without downloading the entire file.

    The Zillow CSV header looks like:
    RegionID, ... , 2025-09-30, 2025-12-31, 2026-03-31

    Returns the last column matching a date format (YYYY-MM-DD).
    """
    log.info("Checking latest Zillow column (streaming header)...")
    response = requests.get(ZILLOW_URL, stream=True, timeout=30)
    response.raise_for_status()

    #Read only the first 2KB - enough for the header row
    first_chunk = b""
    for chunk in response.iter_content(chunk_size=1024):
        first_chunk += chunk
        if b"\n" in first_chunk:#appena vado a capo fermatii abbiamo letto la prima riga
            break
        response.close()
    #l'header (prima riga) contiene i nomi delle colonne — tra cui tutte le date disponibili.
        first_line = first_chunk.split(b"\n")[0].decode("utf-8", errors="replace")
        columns = [c.strip().strip('"') for c in first_line.split(",")]

        #Find the last column matching YYYY-MM-DD
        date_cols = [c for c in columns if re.match(r"^\d{4}-\d{2}-\d{2}$", c)] #"Per ogni c in columns, se c matcha il pattern, metti c nel risultato"
        if not date_cols:
            return ""
        latest = sorted(date_cols)[-1]
        log.info(f"Latest Zillow column found: {latest}")
        return latest

def scrape_zillow(state:dict) -> bool:
    """
    Downloads the Zillow CSV only if a newer date column exists.
    Returns True if a new file was downloaded, False otherwise.
    """
    latest_col = get_zillow_latest_column()
    if not latest_col:
        log.error("Could not determine the latest Zillow column")
        return False

    current_col = state.get("zillow_last_column","")

    if current_col >= latest_col:
        log.info(f"Zillow already up to date ({current_col}) - skip")
        return False 

    log.info(f"New Zillow version: {latest_col} (current: {current_col})")

    dest = os.path.join(DATA_RAW_PATH, FILES["zillow"])
    download_file(ZILLOW_URL, dest)   
    
    state["zillow_last_column"] = latest_col
    log.info(f"Zillow updated to column {latest_col}")
    return True

def get_latest_census_year() -> int:
    """
    Finds the most recent year available in the Census ACS5 API.
    ACS5 data is released with a one-year delay.
    Tries years from most recent to oldest until one returns HTTP 200.
    Uses ZIP code 10001 (Midtown Manhattan) as a test query.
    """
    log.info("Finding most recent Census year...")
    for year in [2024, 2023, 2022, 2021, 2020, 2019]:
        try:
            url = f"{CENSUS_BASE_URL}/{year}/acs/acs5"
            params = {
                "get": "B19013_001E",
                "for": "zip code tabulation area:10001",
            }
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                log.info(f"Census year available: {year}")
                return year
        except Exception:
            pass
    log.error("No Census year available")
    return 0
def download_census_variable(year: int, variable: str, label: str) -> list:
    """
    Downloads a Census variable for all US ZIP codes.

    Parameters:
      year:     data year (e.g. 2024)
      variable: Census variable code (e.g. "B19013_001E")
      label:    descriptive name for logs (e.g. "median_income")

    Returns a list of dicts with keys:
      GEO_ID:     e.g. "860Z200US10001"
      <variable>: numeric value
    """
    log.info(f"Downloading Census {label} (year {year})...")
    url = f"{CENSUS_BASE_URL}/{year}/acs/acs5"
    params = {
        "get": f"GEO_ID,{variable}",
        "for": "zip code tabulation area:*"
    }

    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()

    data = response.json()
    header = data[0]
    rows = data[1:]

    result = []
    for row in rows:
        record = dict(zip(header, row))
        result.append({
            "GEO_ID":  record.get("GEO_ID", ""),
            variable:  record.get(variable, ""),
        })

    log.info(f"  {len(result)} ZIP codes downloaded for {label}")
    return result

def scrape_census(state: dict) -> bool:
    """
    Downloads Census ACS5 data (income + population) for all ZIP codes
    if a more recent year is available.

    Returns True if new files were downloaded, False otherwise.
    """
    year = get_latest_census_year()
    if not year:
        return False

    current_year = state.get("census_year", 0)
    if current_year >= year:
        log.info(f"Census already up to date (year {current_year}) - skip")
        return False

    log.info(f"New Census version: {year} (current: {current_year})")

        # Download median income
    income_data = download_census_variable(year, "B19013_001E", "median_income")
    income_path = os.path.join(DATA_RAW_PATH, FILES["census_income"])
    with open(income_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["GEO_ID", "B19013_001E"])
        writer.writeheader()
        writer.writerows(income_data)
    log.info(f"census_income.csv saved ({len(income_data)} rows)")

    # Download total population
    pop_data = download_census_variable(year, "B01003_001E", "population")
    pop_path = os.path.join(DATA_RAW_PATH, FILES["census_population"])
    with open(pop_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["GEO_ID", "B01003_001E"])
        writer.writeheader()
        writer.writerows(pop_data)
    log.info(f"census_population.csv saved ({len(pop_data)} rows)")

    state["census_year"] = year
    return True

def publish_kafka_event(sources: list):
    """
    Publishes an event to Kafka to notify the consumer that
    new data is available and the pipeline should run.

    Message format:
    {
      "event": "data_updated",
      "sources": ["inside_airbnb", "zillow"],
      "timestamp": "2026-02-13T10:23:00"
    }
    """
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers = KAFKA_BROKER,
            value_serializer = lambda v: json.dumps(v).encode("utf-8")#prima di inviare ogni messaggio, convertilo in JSON e poi in bytes,
        )

        message = {
            "event": "data_updated",
            "sources": sources,
            "timestamp": datetime.now().isoformat(),
        }

        producer.send(KAFKA_TOPIC, value=message)
        producer.flush()# forza l'invio immediato di tutto quello che è nel buffer
        producer.close()

        log.info(f"Kafka event published: {message}")
    
    except Exception as e:
        log.error(f"Kafka publish error: {e}")
        log.warning("Pipeline will not start automatically - run it manually")

def run_once():
    """
    Runs a single check-and-download cycle for all data sources.
    If at least one source has new data, publishes a Kafka event
    to trigger the ETL pipeline.
    """
    log.info("=" * 55)
    log.info("Scraper cycle started")
    log.info("=" * 55)

    os.makedirs(DATA_RAW_PATH, exist_ok=True)
    state = load_state()
    updated_sources = []

    # 1. Inside Airbnb
    try:
        if scrape_inside_airbnb(state):
            updated_sources.append("inside_airbnb")
    except Exception as e:
        log.error(f"Inside Airbnb error: {e}")

    # 2. Zillow
    try:
        if scrape_zillow(state):
            updated_sources.append("zillow")
    except Exception as e:
        log.error(f"Zillow error: {e}")

    # 3. Census
    try:
        if scrape_census(state):
            updated_sources.append("census")
    except Exception as e:
        log.error(f"Census error: {e}")

    # Update last check timestamp
    state["last_check"] = datetime.now().isoformat()
    save_state(state)

    if updated_sources:
        log.info(f"Updated sources: {updated_sources}")
        publish_kafka_event(updated_sources)
    else:
        log.info("No updates found - pipeline not triggered")

    log.info("Scraper cycle complete")
  
def main():
    """
    Main scraper loop.
    Runs run_once() immediately on first start, then sleeps
    CHECK_INTERVAL_HOURS hours and repeats indefinitely.
    """
    log.info(f"Scraper started - checking every {CHECK_INTERVAL_HOURS}h")
    while True:
        run_once()
        log.info(f"Next check in {CHECK_INTERVAL_HOURS}h")
        time.sleep(CHECK_INTERVAL_HOURS * 3600)


if __name__ == "__main__": #Esegue main() solo se lo script viene lanciato direttamente
    main()
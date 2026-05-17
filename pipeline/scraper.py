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
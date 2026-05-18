"""
config.py - Centralized pipeline configuration

All settings are read from environment variables set in docker-compose.yml.
If a variable is not set, a default value is used.

Centralizing configuration here means that to change a parameter
(e.g. the MinIO bucket name) you only need to modify it in one place.
"""
import os
#variabili d'ambiente del servizio pipeline runner

#MinIO
MINIO_HOST = os.getenv("MINIO_HOST","minio:9000")
MINIO_ENDPOINT = f"http://{MINIO_HOST}"
MINIO_USER = os.getenv("MINIO_USER","admin")
MINIO_PASS = os.getenv("MINIO_PASS","password123")
BUCKET = "rental-observatory"

#PostgreSQL
PG_HOST = os.getenv("POSTGRES_HOST","postgres")
PG_PORT = os.getenv("POSTGRES_PORT","5432")
PG_DB = os.getenv("POSTGRES_DB","rental_observatory")
PG_USER = os.getenv("POSTGRES_USER","bdt_admin")
PG_PASS = os.getenv("POSTGRES_PASS","bdt_password")

#JDBC URL used by Spark to connect to PostgreSQL
# Spark è scritto in Java/Scala e gira sulla JVM. PostgreSQL è un database esterno. 
# Per farli parlare serve un protocollo standard chiamato JDBC (Java Database Connectivity).
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
#JDBC properties passed to Spark ( credentials + driver class)
PG_JDBC_PROPS = {
    "user": PG_USER,
    "password":PG_PASS,
    "driver":"org.postgresql.Driver"
}
#Il driver dice a Spark quale JAR usare per parlare con PostgreSQL. 
# È per questo che scarichiamo postgresql-42.6.0.jar nella cartella spark_jars/ 
# — senza quel file Java, Spark non sa come comunicare con PostgreSQL.

#KAFKA
KAFKA_BROKER = os.getenv("KAFKA_BROKER","kafka:9092") #indirizzo server kafka
KAFKA_TOPIC = "pipeline.file-events" #il canale in qui viaggano i messaggi
KAFKA_GROUP = "pipeline-consumer-group" #Kafka usa i consumer group per tenere traccia di fino a dove hai letto i messaggi. È come un segnalibro

#PATHS
#Where the scraper downloads CSV files inside the container
DATA_RAW_PATH = os.getenv("DATA_RAW_PATH","/app/data/raw")
#/app/                        ← root del progetto dentro il container
#├── docker-compose.yml
#├── pipeline/
#├── data/
#│   └── raw/                 ← /app/data/raw
#└── app/
#È la stessa cartella, vista da due prospettive diverse. 
# Quando lo scraper scrive in /app/data/raw/ dentro il container, 
# i file appaiono automaticamente in data/raw/ sul tuo PC 
# — perché è lo stesso posto fisico su disco.

#TIMING
#How often the scraper checks for updated data online
CHECK_INTERVAL_HOURS = int(os.getenv("CHECK_INTERVAL_HOURS","24"))#perchè se no da stringa

# Seconds to wait after the last Kafka event before launching the pipeline.
# The debounce prevents multiple pipeline runs if several events arrive
# in quick succession (e.g. scraper downloads 5 files = 5 potential events)
DEBOUNCE_SECONDS = 60
#non in docker compose perchè dettaglio interno della pipeline, non una configurazione dell'infrastruttura.

#STANDARD FILE NAMES
# Maps logical name → physical filename in data/raw/.
# The scraper downloads and saves files using these standard names.
# Using FILES["listings"] instead of the raw filename means you only
# need to update this dict if filenames ever change.
FILES = {
    "listings":           "listings_NY.csv.gz",
    "calendar":           "calendar_NY.csv.gz",
    "zillow":             "zillow_rent.csv",
    "census_income":      "census_income.csv",
    "census_population":  "census_population.csv",
} 
# dizionario che mappa un nome logico al nome fisico del file.
#Quando lo scraper scarica il file, lo salva con il nome standardizzato 
# che abbiamo definito in FILES, non con il nome originale

#da creare start.py, per ora testiamo su pyspark
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")

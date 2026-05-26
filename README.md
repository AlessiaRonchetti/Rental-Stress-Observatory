NYC Rental Stress Observatory
Description
The NYC Rental Stress Observatory is a fully autonomous Big Data system that monitors the impact of short-term rentals (Airbnb) on housing affordability in New York City. With a single docker-compose up, the system downloads data, processes it through a medallion architecture, and serves an interactive Streamlit dashboard — with no manual intervention.
Abstract
The system combines four public data sources (Inside Airbnb, Zillow, US Census ACS, NYC Open Data) to quantify how the proliferation of Airbnb listings reduces the supply of long-term housing and pushes rents up. Data is landed in an S3-compatible data lake (MinIO) and refined through a Bronze → Silver → Gold pipeline using Apache Spark and Apache Sedona (for geospatial joins between Airbnb coordinates and ZIP-code polygons). The aggregated Gold metrics are written to PostgreSQL, which acts as the serving layer for the dashboard. The whole pipeline is event-driven: a scraper publishes Kafka events when new data appears, and a debounced Kafka consumer triggers the ETL automatically. All seven services are orchestrated with Docker Compose.
The final output is a set of rental stress metrics per ZIP code and borough — rent burden %, occupancy rate, and an Airbnb Pressure Index — visualized in a real-time web dashboard.
Technologies Used

Docker + Docker Compose: Orchestrates all seven services with a single command.
Apache Kafka (Confluent 7.5.0): Event-driven trigger that decouples the scraper from the pipeline; messages are retained for 7 days to allow replay after a crash.
Apache Zookeeper: Cluster coordinator required by Kafka (broker registry, leader election, metadata).
Apache Spark 3.5.0 (PySpark): Distributed processing engine for the Bronze → Silver → Gold transformations.
Apache Sedona 1.7.0: Extends Spark with geospatial functions (ST_Point, ST_Within) to spatially join Airbnb coordinates to ZIP-code polygons.
MinIO: S3-compatible object storage used as the data lake (Bronze / Silver / Gold layers).
PostgreSQL 15: Relational serving layer holding the aggregated tables queried by the dashboard.
Streamlit: Interactive web dashboard (KPIs, choropleth map, charts, tables) that auto-refreshes every 5 minutes.

Project Structure
Rental-Stress-Observatory/
├── docker-compose.yml          # Defines all 7 services
├── .env                        # Secret keys (not committed)
├── pipeline/                   # Autonomous data pipeline
│   ├── config.py               # Environment variables and constants
│   ├── scraper.py              # Downloads data, publishes Kafka events
│   ├── ingestion.py            # Uploads raw files to MinIO Bronze
│   ├── processing.py           # Spark: Bronze → Silver → Gold
│   ├── serving.py              # Spark: Gold Parquet → PostgreSQL
│   ├── run_pipeline.py         # Orchestrates ingestion + processing + serving
│   ├── pipeline_consumer.py    # Kafka consumer with debounce logic
│   └── start.py                # Entry point: launches scraper + consumer threads
├── app/
│   └── dashboard.py            # Streamlit dashboard
├── notebooks/                  # JupyterLab notebooks for exploration
└── images/                     # Dashboard screenshots used in this README
Setup & Configuration
Prerequisites

Docker Desktop installed and running
A free US Census API key (sign up here)

How to Run

Clone the repository:

sh    git clone https://github.com/AlessiaRonchetti/Rental-Stress-Observatory.git
    cd Rental-Stress-Observatory

Create your .env file with the Census API key:

sh    echo "CENSUS_API_KEY=your_key_here" > .env

Start all services:

sh    docker-compose up -d

Watch the pipeline (first run takes ~15–30 min: it downloads JARs and processes the data):

sh    docker logs -f pipeline-runner

Open the dashboard at http://localhost:8501. It shows "Data not available yet" until the pipeline completes, then auto-refreshes.

Other endpoints: MinIO console http://localhost:9001 (admin / password123) · JupyterLab http://localhost:8888 (token bigdata123) · Spark UI http://localhost:4040.
To reset everything (stops services and deletes all data):
shdocker-compose down -v && rm -rf minio_data/ data/raw/ spark_jars/
Data Sources
SourceWhat it providesFormatURLInside AirbnbListings (lat/lon, price, type) + Calendar (daily availability)CSV (.gz)data.insideairbnb.comZillow ZORIMarket rent estimate per ZIP code, one column per monthCSVfiles.zillowstatic.comUS Census ACSMedian household income + population per ZIP code (5-year estimates)JSON APIapi.census.govNYC Open DataGeoJSON polygons of NYC ZIP code boundaries (MODZCTA)GeoJSONdata.cityofnewyork.us
Medallion Architecture
Data flows through three layers stored in MinIO, in increasing quality. The key advantage is the immutability of Bronze: raw data is never modified, so the pipeline can be re-run from Bronze after a bug fix without re-downloading anything.
LayerFormatContentWritten byBronze (raw)CSV (.gz)Files exactly as downloaded, no transformationingestion.pySilver (cleaned)ParquetTyped, deduplicated, nulls handled, GEO_ID → ZIPprocessing.pyGold (aggregated)ParquetBusiness metrics ready for the dashboardprocessing.py
The Gold layer produces five tables, also written to PostgreSQL: market_rental_stress, airbnb_borough_summary, airbnb_pressure, airbnb_listings_with_zip, and zip_airbnb_stress_summary.
Indices and Methodology
1. Rent Burden % (HUD standard) — share of annual income spent on rent:
rent_burden_pct = (market_rent × 12 / median_income) × 100
CategoryThresholdAffordable< 30%Stressed30% – 49.9%Severely Stressed≥ 50%
2. Occupancy Rate (per listing) — days_blocked / total_days, requiring ≥ 300 calendar days. (available = "f" means the day is blocked — booked or host-blocked.)
3. Airbnb Pressure Index (per borough) — combines volume and utilization, normalized to a 0–1 scale:
pressure_score = (num_listings / max_listings_across_boroughs) × avg_occupancy_pct
4. Entire Home % — (entire_home_listings / total_listings) × 100. Shows what share of a borough's Airbnb supply is fully removed from the long-term rental market.
Components Description
Pipeline (autonomous ETL)

config.py: Central configuration read from environment variables (Kafka/MinIO hosts, topic, 60s debounce, Census key, JDBC properties).
scraper.py: Checks each source every 24h and downloads only changed files; resolves the dynamic Zillow URL via BeautifulSoup and publishes a Kafka event on pipeline.file-events. Keeps scraper_state.json for idempotency.
ingestion.py: Uploads raw files from data/raw/ to MinIO under bronze/.
processing.py: Spark job for Silver and Gold. Cleans and types the data, runs the Sedona spatial join (ST_Within) to assign each listing a ZIP code, and computes the Gold metrics. Downloads the required JARs (hadoop-aws, sedona, geotools, postgresql) at runtime.
serving.py: Reads Gold Parquet from MinIO and writes the five tables to PostgreSQL via Spark JDBC (.mode("overwrite")).
run_pipeline.py: Orchestrates ingestion → processing → serving; aborts on the first failure to avoid partial writes.
pipeline_consumer.py: Kafka consumer that triggers the pipeline after 60s of silence (debounce), batching bursts of events into a single run.
start.py: Container entry point; launches the scraper and consumer as daemon threads.

Docker Services (7 containers)
ServiceImagePortRolezookeepercp-zookeeper:7.5.02181Kafka coordinatorkafkacp-kafka:7.5.09092Message broker (pipeline.file-events)miniominio/minio:latest9000 / 9001S3-compatible data lakepostgrespostgres:155432Serving layer for the dashboardpysparkjupyter/pyspark-notebook8888 / 4040Interactive Spark / JupyterLabpipeline-runnerjupyter/pyspark-notebook—Runs start.py (scraper + consumer)frontendpython:3.9-slim8501Streamlit dashboard
Dashboard
The Streamlit dashboard (app/dashboard.py) reads exclusively from PostgreSQL and auto-refreshes every 5 minutes via @st.cache_data(ttl=300). If the database is still empty, it shows a friendly "Data not available yet" message instead of crashing (graceful degradation). It contains six sections: KPI cards, an interactive rental-stress heatmap, two borough bar charts, a top-10 stressed ZIP table, and a scatter plot.
<p align="center">
  <img src="borough_bar_charts.png" width="800" alt="Borough Analysis">
  <br>
  <em>Figure 1: Airbnb listings per borough and Airbnb Pressure Index.</em>
</p>

<p align="center">
  <img src="top10_stressed_zips.png" width="800" alt="Top 10 Stressed ZIPs">
  <br>
  <em>Figure 2: Top 10 ZIP codes under the most severe rental stress.</em>
</p>

Inside Airbnb join mismatch: listings and calendar are generated at different times, so not all listing_ids match. A left join keeps every listing (null occupancy where calendar is missing); avg() in Gold ignores nulls.
Manual trigger → Kafka automation: Steps were originally run by hand. Now the scraper publishes Kafka events and a debounced consumer runs the pipeline automatically. Kafka was chosen over cron because it decouples components, persists events for 7 days (replay after crash), and is extensible.
Zillow dynamic URL + column bug: The CSV URL carries a changing timestamp (hardcoding → 403), resolved dynamically with BeautifulSoup. A separate indentation bug had the header-parsing inside the download loop, so break skipped it; moving the parsing outside the loop fixed the stale-date result.
Census API key not passed: The key was defined in config.py but not added to the request params, so the API returned HTML instead of JSON. Fix: add "key": CENSUS_API_KEY to the params; the key is supplied via .env → Docker → os.getenv.
PYSPARK_SUBMIT_ARGS before SparkSession: JARs added with .config("spark.jars", ...) are ignored because the JVM classpath is fixed at getOrCreate(). Set os.environ["PYSPARK_SUBMIT_ARGS"] before importing SparkSession.
Sedona import after SparkSession: Importing Sedona at the top of the file fails (it looks for an active Spark context). Move the import inside the function, after the session is built.

Authors
This project was developed for the Big Data Technologies course by:

Giorgia Mazzarello
Amine Elhani
Alessia Ronchetti

License
For educational purposes. Data sources keep their own terms of use: Inside Airbnb (CC0), Zillow (Zillow Research Terms), US Census (public domain), NYC Open Data (NYC Open Data Terms).

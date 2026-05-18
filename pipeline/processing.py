"""
processing.py - Silver and Gold layer processing

Reads raw files from MinIO Bronze layer, cleans and transforms them
into Silver (clean Parquet), then joins and aggregates into Gold (Parquet).

Bronze → Silver: clean types, drop nulls, rename columns
Silver → Gold:   join all datasets by ZIP code, compute rental stress index
Spatial:         assign ZIP code to each Airbnb listing via Sedona spatial join
"""
import sys
sys.path.insert(0, "/app")

import os
import re
import logging
import urllib.request

from pipeline.config import (
    MINIO_ENDPOINT, MINIO_USER, MINIO_PASS, BUCKET, FILES
)

log = logging.getLogger(__name__)

# ── MinIO layer paths ──────────────────────────────────────────────────────────
BRONZE_PATH = f"s3a://{BUCKET}/bronze/"
SILVER_PATH = f"s3a://{BUCKET}/silver/"
GOLD_PATH   = f"s3a://{BUCKET}/gold/"

# ── JARs required by Spark ────────────────────────────────────────────────────
# hadoop-aws + aws-sdk  → let Spark read/write MinIO via S3A protocol
# sedona + geotools     → spatial join (assign ZIP code to each Airbnb listing)
JAR_DIR = "/tmp/spark_jars"
JARS = {
    "hadoop-aws-3.3.4.jar":
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar":
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
    "sedona-spark-shaded-3.5_2.12-1.7.0.jar":
        "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.7.0/sedona-spark-shaded-3.5_2.12-1.7.0.jar",
    "geotools-wrapper-1.6.1-28.2.jar":
        "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.6.1-28.2/geotools-wrapper-1.6.1-28.2.jar",
}

#JAR download

def ensure_jars():
    """
    Downloads required JARs into JAR_DIR if not already present.
    JARs persist across container restarts because JAR_DIR is a Docker volume.
    """
    os.makedirs(JAR_DIR, exist_ok=True)
    for filename, url in JARS.items():
        dest = os.path.join(JAR_DIR, filename)
        if not os.path.exists(dest):
            log.info(f"Downloading JAR: {filename}...")
            urllib.request.urlretrieve(url, dest)
            log.info(f"Downloaded: {filename}")
        else:
            log.info(f"JAR already present: {filename}")

#Spark session
def create_spark_session():
    """
    Creates a SparkSession configured for MinIO (S3A) and Sedona (spatial join).
    PYSPARK_SUBMIT_ARGS must be set BEFORE importing SparkSession,
    otherwise the JARs are not loaded into the JVM.
    """
    jar_list = ",".join([os.path.join(JAR_DIR, f) for f in JARS])
    #/tmp/spark_jars/hadoop-aws-3.3.4.jar,/tmp/spark_jars/aws-java-sdk-bundle-1.12.262.jar,...
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {jar_list} pyspark-shell"

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("NYC_Rental_Stress")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true") #dice a Spark come costruire gli URL per accedere a MinIO, path style
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") #dice a Spark quale classe Java usare quando vede un path che inizia con s3a://
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN") #Spark di default stampa tantissimi log di livello INFO che rendono l'output illeggibile. Con WARN vediamo solo avvisi ed errori, non tutti i dettagli interni di Spark.
    log.info(f"Spark {spark.version} ready")
    return spark


def load_bronze(spark):
    """
    Loads all raw files from MinIO Bronze layer into Spark DataFrames.
    """
    log.info("=" * 55)
    log.info("STEP 1 — Loading Bronze layer...")
    log.info("=" * 55)

    pop_df = spark.read.csv(
        BRONZE_PATH + FILES["census_population"],
        header=True, inferSchema=True)

    income_df = spark.read.csv(
        BRONZE_PATH + FILES["census_income"],
        header=True, inferSchema=True)

    zillow_df = spark.read.csv(
        BRONZE_PATH + FILES["zillow"],
        header=True, inferSchema=True)

    listings_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true") #una singola cella può contenere \n
        .option("quote", '"') #perché i file Airbnb contengono testo libero nelle colonne
        .option("escape", '"')
        .load(BRONZE_PATH + FILES["listings"])
    )

    calendar_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .load(BRONZE_PATH + FILES["calendar"])
    )

    log.info(f"  Population:  {pop_df.count():,} rows")
    log.info(f"  Income:      {income_df.count():,} rows")
    log.info(f"  Zillow:      {zillow_df.count():,} rows")
    log.info(f"  Listings:    {listings_df.count():,} rows")
    log.info(f"  Calendar:    {calendar_df.count():,} rows")
    log.info("Bronze loaded.")

    return pop_df, income_df, zillow_df, listings_df, calendar_df

def process_census_population(pop_df):
    """
    Cleans Census population data.
    Extracts ZIP code from GEO_ID and casts population to int.

    Adaptation from notebook: our CSV has no 'Geography' header row
    and no extra columns (NAME, margin of error) since we downloaded
    via API instead of the Census bulk download.
    """
    from pyspark.sql.functions import col, substring, regexp_replace, when

    return (
        pop_df
        .withColumn("zip_code", substring(col("GEO_ID"), -5, 5)) #substring prende 3 parametri: (colonna, posizione_di_inizio, lunghezza)
        .withColumn(
            "total_population",
            when(
                regexp_replace(col("B01003_001E"), "[^0-9]", "") == "", None
            ).otherwise(
                regexp_replace(col("B01003_001E"), "[^0-9]", "").cast("int")
            )
            #SE (il valore dopo aver rimosso i non-numeri è una stringa vuota)
            # metti None
            # ALTRIMENTI  metti il valore pulito convertito a int
        )
        .select("zip_code", "total_population")
        .dropna()
    )


def process_census_income(income_df):
    """
    Cleans Census income data.
    Extracts ZIP code from GEO_ID and casts median income to int.
    """
    from pyspark.sql.functions import col, substring, regexp_replace, when

    return (
        income_df
        .withColumn("zip_code", substring(col("GEO_ID"), -5, 5))
        .withColumn(
            "median_income",
            when(
                regexp_replace(col("B19013_001E"), "[^0-9]", "") == "", None
            ).otherwise(
                regexp_replace(col("B19013_001E"), "[^0-9]", "").cast("int")
            )
        )
        .select("zip_code", "median_income")
        .dropna()
    )

def process_zillow(zillow_df):
    """
    Cleans Zillow ZORI data.
    Filters for New York state and picks the latest available date column
    dynamically instead of hardcoding a specific date.
    """
    from pyspark.sql.functions import col

    # Find the latest date column dynamically (format YYYY-MM-DD)
    date_cols = sorted([
        c for c in zillow_df.columns
        if re.match(r"^\d{4}-\d{2}-\d{2}$", c)
    ])
    latest_col = date_cols[-1]
    log.info(f"Using Zillow column: {latest_col}")

    return (
        zillow_df
        .filter(col("State") == "NY")
        .select(
            col("RegionName").cast("string").alias("zip_code"),
            col(latest_col).cast("float").alias("market_rent")
        )
        .dropna()
    )

def process_listings(listings_df):
    """
    Cleans Airbnb listings data.
    Keeps only geographic and demographic columns needed for the spatial join.
    Note: price column is NULL in the entire NYC dataset — we use
    Zillow ZORI as market rent proxy instead.
    """
    from pyspark.sql.functions import col

    return (
        listings_df
        # Remove rows with invalid ID
        .filter(col("id").cast("bigint").isNotNull())
        # Remove listings without borough (cannot be geolocated)
        .filter(col("neighbourhood_group_cleansed").isNotNull())
        # Remove listings without coordinates (cannot be mapped)
        .filter(col("latitude").isNotNull())
        .filter(col("longitude").isNotNull())
        .select(
            col("id").cast("bigint").alias("listing_id"),
            col("neighbourhood_group_cleansed"),
            col("neighbourhood_cleansed"),
            col("latitude").cast("float"),
            col("longitude").cast("float"),
            col("room_type"),
            col("accommodates").cast("int"),
            col("calculated_host_listings_count").cast("int"),
        )
    )
def process_calendar(calendar_df):
    from pyspark.sql.functions import col, when, sum as spark_sum, count, round as spark_round
    from pyspark.sql.functions import to_date

    # Step 1 — Cast types and parse date
    df = (
        calendar_df
        .filter(col("listing_id").cast("bigint").isNotNull())
        .withColumn("listing_id", col("listing_id").cast("bigint"))
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    )

    # Step 2 — Convert available column to numeric flags
    df = (
        df
        .withColumn("is_available", when(col("available") == "t", 1).otherwise(0))
        .withColumn("is_blocked",   when(col("available") == "f", 1).otherwise(0))
    )

    # Step 3 — Aggregate per listing: reduces 13M rows to ~36k
    df = (
        df
        .groupBy("listing_id")
        .agg(
            spark_sum("is_available").alias("days_available"),
            spark_sum("is_blocked").alias("days_blocked"),
            count("date").alias("total_days")
        )
    )

    # Step 4 — Keep only listings with enough data and compute occupancy rate
    df = (
        df
        .filter(col("total_days") >= 300)
        .withColumn(
            "cal_occupancy_rate",
            spark_round(col("days_blocked") / col("total_days"), 4)
        )
    )

    # Step 5 — Convert 0.0 to NULL (listings never booked have no reliable data)
    df = (
        df
        .withColumn(
            "cal_occupancy_rate",
            when(col("cal_occupancy_rate") == 0, None)
            .otherwise(col("cal_occupancy_rate"))
        )
    )

    return df
def build_airbnb_enriched(listings_df, calendar_df):
    """
    Joins listings and calendar on listing_id.
    Uses cal_occupancy_rate (real data from calendar) as final_occupancy_rate.
    Inner join: keeps only listings present in both datasets.
    """
    from pyspark.sql.functions import col

    listings_silver = process_listings(listings_df)
    calendar_silver = process_calendar(calendar_df)

    return (
        calendar_silver
        .join(listings_silver, "listing_id", "inner")
        .select(
            "listing_id",
            "neighbourhood_group_cleansed",
            "neighbourhood_cleansed",
            "latitude",
            "longitude",
            "room_type",
            "accommodates",
            "calculated_host_listings_count",
            col("cal_occupancy_rate").alias("final_occupancy_rate"),
            "days_available",
            "days_blocked",
            "total_days",
        )
    )

#Save silver
def save_silver(census_pop, census_inc, zillow, airbnb_enriched):
    """
    Saves all Silver DataFrames to MinIO in Parquet format.
    Parquet is columnar — faster reads and better compression than CSV.
    """
    log.info("Saving Silver layer to MinIO...")

    census_pop.write.mode("overwrite").parquet(SILVER_PATH + "census_population/")
    log.info("census_population saved")

    census_inc.write.mode("overwrite").parquet(SILVER_PATH + "census_income/")
    log.info("census_income saved")

    zillow.write.mode("overwrite").parquet(SILVER_PATH + "zillow_rent/")
    log.info("zillow_rent saved")

    airbnb_enriched.write.mode("overwrite").parquet(SILVER_PATH + "airbnb_listings_enriched/")
    log.info("airbnb_listings_enriched saved")

    log.info("Silver layer saved.")

#Silver to Gold
def build_gold_economic_profile(census_pop, census_inc, zillow):
    """
    Joins Census population, Census income and Zillow rent by ZIP code.
    Computes the Rental Stress Index (rent burden) per ZIP code.

    rent_burden_pct = (monthly_rent * 12 / median_income) * 100

    HUD standard thresholds:
      < 30%  → Affordable
      30-50% → Stressed
      >= 50% → Severely Stressed
    """
    from pyspark.sql.functions import col, when, round as spark_round

    # Join all three datasets by ZIP code
    # Inner join: keep only ZIP codes present in ALL three datasets
    economic_profile = (
        census_pop
        .join(census_inc, "zip_code", "inner")
        .join(zillow,     "zip_code", "inner")
    )

    # Compute rent burden percentage
    gold = (
        economic_profile
        .withColumn(
            "rent_burden_pct",
            when(
                col("median_income") > 0,
                spark_round(
                    (col("market_rent") * 12 / col("median_income")) * 100, 2
                )
            ).otherwise(None)
        )
        .withColumn(
            "stress_category",
            when(col("rent_burden_pct") >= 50, "Severely Stressed")
            .when(col("rent_burden_pct") >= 30, "Stressed")
            .when(col("rent_burden_pct").isNotNull(), "Affordable")
            .otherwise("No Data")
        )
    )

    log.info(f"Economic profile: {gold.count():,} ZIP codes")
    return gold

def build_gold_borough_summary(airbnb_enriched):
    """
    Aggregates Airbnb listings by borough.
    Computes listing concentration, occupancy pressure,
    and Airbnb pressure index per borough.
    """
    from pyspark.sql.functions import (
        col, count, avg, round as spark_round,
        sum as spark_sum, when, max as spark_max
    )

    # Aggregate per borough
    borough_summary = (
        airbnb_enriched
        .filter(col("final_occupancy_rate").isNotNull())
        .groupBy("neighbourhood_group_cleansed")
        .agg(
            count("listing_id").alias("num_listings"),
            spark_round(avg("final_occupancy_rate") * 100, 1).alias("avg_occupancy_pct"),
            spark_round(avg("calculated_host_listings_count"), 1).alias("avg_host_listings"),
            spark_sum(
                when(col("room_type") == "Entire home/apt", 1).otherwise(0)
            ).alias("entire_home_count")
        )
        .withColumn(
            "entire_home_pct",
            spark_round(col("entire_home_count") / col("num_listings") * 100, 1)
        )
        .orderBy(col("num_listings").desc())
    )

    # Compute pressure score: normalized listing count * occupancy rate
    max_listings = borough_summary.agg(spark_max("num_listings")).collect()[0][0] #.collect() restituisce una lista di Row

    airbnb_pressure = (
        borough_summary
        .withColumn(
            "pressure_score",
            spark_round(
                (col("num_listings") / max_listings) * col("avg_occupancy_pct"), 2
            )
        )
        .orderBy(col("pressure_score").desc())
    )

    log.info(f"Borough summary: {airbnb_pressure.count()} boroughs")
    return borough_summary, airbnb_pressure

#Spatial Analysis
def download_nyc_geojson():
    """
    Downloads NYC ZIP code boundaries as GeoJSON from NYC Open Data.
    Saved to /tmp so it does not need to be re-downloaded on every run.
    """
    geojson_path = "/tmp/nyc_zip.geojson"
    if not os.path.exists(geojson_path):
        log.info("Downloading NYC ZIP GeoJSON...")
        import requests
        url = "https://data.cityofnewyork.us/resource/pri4-ifjk.geojson?$limit=5000"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(geojson_path, "wb") as f:
            f.write(r.content)
        log.info(f"GeoJSON downloaded ({os.path.getsize(geojson_path)//1024} KB)")
    else:
        log.info("GeoJSON already present")
    return geojson_path

def build_gold_spatial(spark, airbnb_enriched, gold_economic):
    """
    Assigns a ZIP code to each Airbnb listing using a spatial join (Sedona).

    Steps:
      1. Load NYC ZIP code boundaries (GeoJSON polygons)
      2. Create a geometry point from each listing lat/lon
      3. ST_Within: assign each point to the polygon it falls inside
      4. Join with economic stress index
      5. Aggregate per ZIP code
    """
    from pyspark.sql.functions import (
        col, count, avg, round as spark_round,
        sum as spark_sum, when, explode
    )
    from sedona.spark import SedonaContext
    from sedona.sql.st_constructors import ST_Point
    from sedona.sql.st_predicates import ST_Within

    # Register Sedona on the existing SparkSession
    sedona = SedonaContext.create(spark) #registra Sedona sulla SparkSession esistente 
    log.info("Sedona ready")

    geojson_path = download_nyc_geojson()

    gdf_raw = (
        spark.read
        .format("geojson")
        .option("multiLine","true")
        .load(geojson_path)
    )

    # The GeoJSON has a 'features' array — each element is one ZIP polygon
    # explode() splits the array into one row per element
    gdf_zip = (
        gdf_raw
        .select(explode(col("features")).alias("feature"))
        .select(
            col("feature.properties.modzcta").alias("zip_code"),
            col("feature.geometry").alias("geometry")
        )
        .filter(col("zip_code").isNotNull())
    )

    log.info(f"ZIP polygons loaded: {gdf_zip.count()}") 

    airbnb_geo = (
        airbnb_enriched
        .filter(col("latitude").isNotNull())
        .filter(col("longitude").isNotNull())
        .withColumn(
            "geometry",
            ST_Point(col("longitude"), col("latitude")) #Create a geometry point from each listing's coordinates, prima long e lat
        )
    )

    log.info(f"Listings with coordinates: {airbnb_geo.count():,}")

    #Spatial join: assign each listing point to its ZIP polygon
    # ST_Within(point, polygon) returns True if the point is inside the polygon
    airbnb_with_zip = (
        airbnb_geo.alias("a")
        .join(
            gdf_zip.alias("z"),
            ST_Within(col("a.geometry"), col("z.geometry")),
            "left"
        )
        .select(
            col("a.listing_id"),
            col("a.neighbourhood_group_cleansed"),
            col("a.latitude"),
            col("a.longitude"),
            col("a.final_occupancy_rate"),
            col("a.room_type"),
            col("a.calculated_host_listings_count"),
            col("z.zip_code"),
        )
    )

    matched = airbnb_with_zip.filter(col("zip_code").isNotNull()).count()
    log.info(f"Listings assigned to ZIP: {matched:,} / {airbnb_with_zip.count():,}")
    
    # Join each listing with the economic stress index by ZIP
    complete = (
        airbnb_with_zip
        .join(
            gold_economic.select(
                "zip_code", "median_income", "market_rent",
                "total_population", "rent_burden_pct", "stress_category"
            ),
            "zip_code", "left"
        )
    )
    #Aggregate per ZIP code
    zip_summary = (
        complete
        .groupBy(
            "zip_code",
            "neighbourhood_group_cleansed",
            "rent_burden_pct",
            "stress_category",
            "median_income",
            "market_rent",
            "total_population",
        )
        .agg(
            count("listing_id").alias("num_airbnb_listings"),
            spark_round(avg("final_occupancy_rate") * 100, 1).alias("avg_occupancy_pct"),
            spark_round(avg("calculated_host_listings_count"), 1).alias("avg_host_listings"),
            spark_round(
                spark_sum(
                    when(col("room_type") == "Entire home/apt", 1).otherwise(0)
                ) / count("listing_id") * 100, 1
            ).alias("entire_home_pct")
        )
        .orderBy(col("rent_burden_pct").desc())
    )

    log.info(f"ZIP summary: {zip_summary.count():,} ZIP codes")
    return airbnb_with_zip, zip_summary

#Save Gold
def save_gold(gold_economic, borough_summary, airbnb_pressure, airbnb_with_zip, zip_summary):
    """
    Saves all Gold DataFrames to MinIO in Parquet format.
    """
    log.info("Saving Gold layer to MinIO...")

    gold_economic.write.mode("overwrite").parquet(GOLD_PATH + "market_rental_stress/")
    log.info("market_rental_stress saved")

    borough_summary.write.mode("overwrite").parquet(GOLD_PATH + "airbnb_borough_summary/")
    log.info("airbnb_borough_summary saved")

    airbnb_pressure.write.mode("overwrite").parquet(GOLD_PATH + "airbnb_pressure/")
    log.info("airbnb_pressure saved")

    zip_summary.write.mode("overwrite").parquet(GOLD_PATH + "zip_airbnb_stress_summary/")
    log.info("zip_airbnb_stress_summary saved")

    airbnb_with_zip.write.mode("overwrite").parquet(GOLD_PATH + "airbnb_listings_with_zip/")
    log.info("airbnb_listings_with_zip saved")

    log.info("Gold layer saved.")

#Main
def run():
    """
    Orchestrates the full processing pipeline:
      Bronze → Silver → Gold
    """
    log.info("=== Processing started ===")

    # Step 0 — Download JARs if needed
    ensure_jars()

    # Step 1 — Create Spark session
    spark = create_spark_session()

    try:
        # Step 2 — Load Bronze layer
        pop_df, income_df, zillow_df, listings_df, calendar_df = load_bronze(spark)

        # Step 3 — Bronze → Silver
        log.info("Building Silver layer...")
        census_pop      = process_census_population(pop_df)
        census_inc      = process_census_income(income_df)
        zillow          = process_zillow(zillow_df)
        airbnb_enriched = build_airbnb_enriched(listings_df, calendar_df)

        # Step 4 — Save Silver
        save_silver(census_pop, census_inc, zillow, airbnb_enriched)

        # Step 5 — Silver → Gold
        log.info("Building Gold layer...")
        gold_economic                  = build_gold_economic_profile(census_pop, census_inc, zillow)
        borough_summary, airbnb_pressure = build_gold_borough_summary(airbnb_enriched)
        airbnb_with_zip, zip_summary   = build_gold_spatial(spark, airbnb_enriched, gold_economic)

        # Step 6 — Save Gold
        save_gold(gold_economic, borough_summary, airbnb_pressure, airbnb_with_zip, zip_summary)

        log.info("=== Processing complete ===")

    finally:
        # Il try/finally garantisce che spark.stop() venga sempre chiamato anche se un passo intermedio fallisce 
        # senza questo la JVM rimarrebbe appesa in memoria
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
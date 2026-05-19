"""
serving.py - Gold layer → PostgreSQL

Reads aggregated Gold Parquet files from MinIO and writes them
to PostgreSQL tables for the Streamlit dashboard to read.
"""
import sys
sys.path.insert(0, "/app")

import os
import logging

from pipeline.config import (
    MINIO_ENDPOINT, MINIO_USER, MINIO_PASS,
    BUCKET, PG_JDBC_URL, PG_JDBC_PROPS
)

log = logging.getLogger(__name__)

GOLD_PATH = f"s3a://{BUCKET}/gold/"

JAR_DIR = "/tmp/spark_jars"
JARS = {
    "hadoop-aws-3.3.4.jar":
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "aws-java-sdk-bundle-1.12.262.jar":
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
    "postgresql-42.6.0.jar":
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar",#Serve il driver PostgreSQL JDBC per scrivere su PostgreSQL
}

def create_spark_session():
    """
    Creates a SparkSession configured for MinIO (S3A) and PostgreSQL (JDBC).
    """
    import urllib.request
    os.makedirs(JAR_DIR, exist_ok=True)
    for filename, url in JARS.items():
        dest = os.path.join(JAR_DIR, filename)
        if not os.path.exists(dest):
            log.info(f"Downloading JAR: {filename}...")
            urllib.request.urlretrieve(url, dest)

    jar_list = ",".join([os.path.join(JAR_DIR, f) for f in JARS])
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {jar_list} pyspark-shell"

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("NYC_Rental_Stress_Serving")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"Spark {spark.version} ready")
    return spark

def write_to_postgres(df, table_name):
    """
    Writes a Spark DataFrame to a PostgreSQL table via JDBC.
    mode='overwrite' replaces the table completely on each run.
    """
    df.write \
        .format("jdbc") \
        .option("url", PG_JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", PG_JDBC_PROPS["user"]) \
        .option("password", PG_JDBC_PROPS["password"]) \
        .option("driver", PG_JDBC_PROPS["driver"]) \
        .mode("overwrite") \
        .save() #lazy
    log.info(f"Table written: {table_name} ({df.count():,} rows)")

def run():
    """
    Reads all Gold Parquet files from MinIO and writes them to PostgreSQL.
    """
    log.info("=== Serving started ===")
    spark = create_spark_session()

    try:
        # market_rental_stress — stress index per ZIP code
        df = spark.read.parquet(GOLD_PATH + "market_rental_stress/")
        write_to_postgres(df, "market_rental_stress")

        # airbnb_borough_summary — listing concentration per borough
        df = spark.read.parquet(GOLD_PATH + "airbnb_borough_summary/")
        write_to_postgres(df, "airbnb_borough_summary")

        # airbnb_pressure — pressure index per borough
        df = spark.read.parquet(GOLD_PATH + "airbnb_pressure/")
        write_to_postgres(df, "airbnb_pressure")

        # zip_airbnb_stress_summary — combined view per ZIP code
        df = spark.read.parquet(GOLD_PATH + "zip_airbnb_stress_summary/")
        write_to_postgres(df, "zip_airbnb_stress_summary")

        # airbnb_listings_with_zip — individual listings with ZIP (for heatmap)
        df = spark.read.parquet(GOLD_PATH + "airbnb_listings_with_zip/")
        write_to_postgres(df, "airbnb_listings_with_zip")

        log.info("=== Serving complete ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
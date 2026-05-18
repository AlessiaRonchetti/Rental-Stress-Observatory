"""
ingestion.py - Bronze layer ingestion

Uploads raw CSV files from data/raw/ to MinIO under the bronze/ prefix.
This is the first step of the medallion architecture:
  data/raw/ (local) → MinIO bronze/ (data lake)
"""
import os
import sys
sys.path.insert(0, "/app")

import logging
from minio import Minio
from minio.error import S3Error

from pipeline.config import (
    MINIO_HOST, MINIO_USER, MINIO_PASS,
    BUCKET, DATA_RAW_PATH, FILES
)

log = logging.getLogger(__name__)

def get_minio_client():
    """
    Creates and returns a MinIO client.
    secure=False because we are inside Docker (no HTTPS needed).
    """
    return Minio(
        MINIO_HOST,
        access_key=MINIO_USER,
        secret_key=MINIO_PASS,
        secure=False #usa http non https
    )

def ensure_bucket(client):
    """
    Creates the MinIO bucket if it does not exist yet.
    """
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info(f"Bucket created: {BUCKET}")
    else:
        log.info(f"Bucket already exists: {BUCKET}")


def upload_file(client, local_path, object_name):
    """
    Uploads a single file to MinIO.
    object_name is the path inside the bucket, e.g. bronze/listings_NY.csv.gz
    """
    client.fput_object(BUCKET, object_name, local_path)
    log.info(f"Uploaded: {local_path} → {BUCKET}/{object_name}")

def run():
    """
    Main ingestion function.
    Uploads all files defined in FILES to the bronze/ layer in MinIO.
    Skips files that are missing locally (for example not yet downloaded).
    """
    log.info("=== Ingestion started ===")

    client = get_minio_client()
    ensure_bucket(client)

    uploaded = 0
    skipped = 0

    for key, filename in FILES.items():
        local_path = os.path.join(DATA_RAW_PATH, filename)
        object_name = f"bronze/{filename}"

        if not os.path.exists(local_path):
            log.warning(f"File not found locally, skipping: {local_path}")
            skipped += 1
            continue

        try:
            upload_file(client, local_path, object_name)
            uploaded += 1
        except S3Error as e:
            log.error(f"MinIO error uploading {filename}: {e}")

    log.info(f"=== Ingestion complete: {uploaded} uploaded, {skipped} skipped ===")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
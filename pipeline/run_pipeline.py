"""
run_pipeline.py - Pipeline orchestrator

Runs the three pipeline steps in sequence:
  1. ingestion.py  — CSV local files → MinIO Bronze
  2. processing.py — Bronze → Silver → Gold via Spark
  3. serving.py    — Gold → PostgreSQL

Called by pipeline_consumer.py after receiving a Kafka event.
"""
import sys
sys.path.insert(0, "/app")

import logging
from pipeline import ingestion, processing, serving

log = logging.getLogger(__name__)

def run():
    """
    Runs the full pipeline: ingestion → processing → serving.
    Each step is independent — if one fails, the error is logged
    and the pipeline stops at that step.
    """
    log.info("=" * 55)
    log.info("PIPELINE RUN STARTED")
    log.info("=" * 55)

    try:
        log.info("Step 1/3 — Ingestion...")
        ingestion.run()
        log.info("Step 1/3 — Ingestion complete")
    except Exception as e:
        log.error(f"Ingestion failed: {e}")
        raise

    try:
        log.info("Step 2/3 — Processing...")
        processing.run()
        log.info("Step 2/3 — Processing complete")
    except Exception as e:
        log.error(f"Processing failed: {e}")
        raise

    try:
        log.info("Step 3/3 — Serving...")
        serving.run()
        log.info("Step 3/3 — Serving complete")
    except Exception as e:
        log.error(f"Serving failed: {e}")
        raise

    log.info("=" * 55)
    log.info("PIPELINE RUN COMPLETE")
    log.info("=" * 55)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
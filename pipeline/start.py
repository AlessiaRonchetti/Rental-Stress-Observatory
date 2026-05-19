"""
start.py - Pipeline entry point

Starts two components in parallel:
  1. scraper.main()            — checks for new data every CHECK_INTERVAL_HOURS
  2. pipeline_consumer.main()  — listens on Kafka and triggers the pipeline

Both run as daemon threads — if the main process dies, they die with it.
"""
import sys
sys.path.insert(0, "/app")

import logging
import threading
from pipeline import scraper, pipeline_consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def main():
    log.info("Starting pipeline runner...")

    # Thread 1 — Scraper: checks for new data periodically
    scraper_thread = threading.Thread(
        target=scraper.main,
        name="Scraper",
        daemon=True
    )

    # Thread 2 — Consumer: listens on Kafka and triggers the pipeline
    consumer_thread = threading.Thread(
        target=pipeline_consumer.main,
        name="Consumer",
        daemon=True
    )

    scraper_thread.start()
    log.info("Scraper thread started")

    consumer_thread.start()
    log.info("Consumer thread started")

    # Keep main thread alive — if it exits, daemon threads die too
    scraper_thread.join() #aspetta qui finché questo thread non finisce"
    #Siccome scraper.main() è un loop infinito, non finisce mai,il main rimane bloccato per sempre e il container rimane vivo.
    consumer_thread.join() # mai raggiunto, ma per completezza


if __name__ == "__main__":
    main()

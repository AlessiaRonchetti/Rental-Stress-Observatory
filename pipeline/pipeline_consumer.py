"""
pipeline_consumer.py - Kafka consumer

Listens on the Kafka topic 'pipeline.file-events'.
When a new event arrives, waits DEBOUNCE_SECONDS for more events
(to batch multiple source updates into a single pipeline run),
then triggers run_pipeline.run().
"""
"""
pipeline_consumer.py - Kafka consumer

Listens on the Kafka topic 'pipeline.file-events'.
When a new event arrives, waits DEBOUNCE_SECONDS for more events
(to batch multiple source updates into a single pipeline run),
then triggers run_pipeline.run().
"""
import sys
sys.path.insert(0, "/app")

import json
import time
import logging
from kafka import KafkaConsumer
from pipeline.config import (
    KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP, DEBOUNCE_SECONDS
)
from pipeline import run_pipeline

log = logging.getLogger(__name__)


def wait_for_kafka(retries=10, delay=10):
    """
    Waits for Kafka to be ready before starting the consumer.
    Retries up to 'retries' times with 'delay' seconds between attempts.
    """
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),#Kafka trasmette i messaggi come bytes — non come stringhe o dizionari Python. Questo parametro dice a Kafka come convertire ogni messaggio ricevuto
                auto_offset_reset="latest"#legge solo i messaggi nuovi da quando si è connesso,
                enable_auto_commit=True#Con True Kafka salva automaticamente l'offset ogni volta che leggiamo un messaggio. 
                #Questo significa che se il consumer crasha e si riavvia, 
                #ricomincia dal punto dove si era fermato invece di rileggere tutto dall'inizio,
            )
            log.info("Connected to Kafka")
            return consumer
        except Exception as e:
            log.warning(f"Kafka not ready (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries")

def main():
    """
    Main consumer loop.
    Implements a debounce pattern:
      - When a message arrives, records the time
      - Keeps polling for more messages
      - When no new message arrives for DEBOUNCE_SECONDS, runs the pipeline
    This batches multiple Kafka events into a single pipeline run.
    """
    log.info(f"Pipeline consumer started")
    log.info(f"Topic: {KAFKA_TOPIC} | Group: {KAFKA_GROUP} | Debounce: {DEBOUNCE_SECONDS}s")

    consumer = wait_for_kafka()

    last_event_time = None

    while True:
        # Poll Kafka for new messages (timeout 5 seconds)
        messages = consumer.poll(timeout_ms=5000)

        if messages:
            for topic_partition, records in messages.items():
                for record in records:
                    log.info(f"Event received: {record.value}")
                    last_event_time = time.time()

        # If we have a pending event and debounce time has passed → run pipeline
        if last_event_time is not None:
            elapsed = time.time() - last_event_time
            remaining = DEBOUNCE_SECONDS - elapsed

            if remaining <= 0:
                log.info("Debounce complete — triggering pipeline...")
                last_event_time = None
                try:
                    run_pipeline.run()
                except Exception as e:
                    log.error(f"Pipeline failed: {e}")
            else:
                log.info(f"Waiting for more events... ({remaining:.0f}s remaining)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
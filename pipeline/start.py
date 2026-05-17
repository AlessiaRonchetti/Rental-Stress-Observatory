# Temporary start.py - keeps the container alive for testing
import time
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

log.info("Container started - waiting for scripts...")

while True:
    time.sleep(60)
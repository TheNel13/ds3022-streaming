"""lander.py: Kafka topic bluesky_posts -> raw Parquet batches in data/raw/.

  python lander.py          # run until Ctrl+C
  python lander.py crash    # chaos card D1: write ONE file, then exit BEFORE storing offsets

Rules (checkpoint C):
  - a batch closes at 1,000 records or 60 seconds, whichever comes first
  - the batch is written to <name>.parquet.tmp, then renamed to <name>.parquet
  - offsets are stored only AFTER the file exists  ->  at-least-once (a crash re-lands, never loses)
  - every message is landed, even bad ones: invalid JSON, or JSON without a did, gets action = "malformed"
"""
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
from confluent_kafka import Consumer, KafkaError

TOPIC = "bluesky_posts"
GROUP = "bluesky_posts_lander"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
OUT_DIR = "data/raw"
PREFIX = "bluesky"
BATCH_SIZE = 1000
BATCH_SECONDS = 60

# kept from the class lander: kafka_partition, kafka_offset, kafka_key, action, landed_at, raw
# my columns: the id I dedupe on (did, rkey, operation), what kind of event it is, and the event time
SCHEMA = """
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    kafka_key       VARCHAR,
    action          VARCHAR,
    did             VARCHAR,
    rkey            VARCHAR,
    operation       VARCHAR,
    kind            VARCHAR,
    collection      VARCHAR,
    time_us         BIGINT,
    landed_at       TIMESTAMP,
    raw             VARCHAR
"""
N_COLUMNS = 12

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lander")


def to_row(msg, landed_at):
    """One Kafka message -> one row. Never raises: bad input becomes action = 'malformed'."""
    key = msg.key().decode("utf-8", "replace") if msg.key() else None
    value = msg.value()
    raw = value.decode("utf-8", "replace") if value is not None else None
    base = [msg.partition(), msg.offset(), key]
    try:
        env = json.loads(raw)
        data = env["data"]
        if not isinstance(data, dict) or not data.get("did"):
            raise ValueError("no did")
    except (TypeError, ValueError, KeyError):
        return base + ["malformed", None, None, None, None, None, None, landed_at, raw]
    commit = data.get("commit") or {}
    time_us = data.get("time_us")
    return base + [
        env.get("action"),
        data.get("did"),
        commit.get("rkey"),
        commit.get("operation"),
        data.get("kind"),
        commit.get("collection"),
        time_us if isinstance(time_us, int) else None,
        landed_at,
        raw,
    ]


def write_batch(rows):
    """Write rows to a .tmp file, then rename it: a half-written file is never seen as landed."""
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    final = f"{OUT_DIR}/{PREFIX}_{stamp}_{uuid.uuid4().hex[:8]}.parquet"
    tmp = final + ".tmp"
    con = duckdb.connect()
    con.execute(f"CREATE TABLE batch ({SCHEMA})")
    con.executemany(f"INSERT INTO batch VALUES ({', '.join(['?'] * N_COLUMNS)})", rows)
    con.execute(f"COPY batch TO '{tmp}' (FORMAT parquet)")
    con.close()
    os.replace(tmp, final)
    return final


def main():
    crash = len(sys.argv) > 1 and sys.argv[1] == "crash"
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,         # commits only what we stored ...
        "enable.auto.offset.store": False,  # ... and we store only after the file exists
    })
    consumer.subscribe([TOPIC])
    log.info("lander started: topic %s, group %s%s", TOPIC, GROUP, " (CRASH mode)" if crash else "")

    batch, started, total, malformed = [], time.monotonic(), 0, 0

    def flush():
        nonlocal batch, started, total, malformed
        if not batch:
            started = time.monotonic()
            return
        landed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = [to_row(m, landed_at) for m in batch]
        bad = sum(1 for r in rows if r[3] == "malformed")
        path = write_batch(rows)
        if crash:
            print(f"Landed {len(rows)} records to {path}")
            print("CRASH (on purpose): exiting before storing offsets")
            os._exit(1)
        for m in batch:                      # the file exists: now it is safe to store the offsets
            consumer.store_offsets(message=m)
        total += len(rows)
        malformed += bad
        log.info("Landed %d records (%d malformed) to %s | total %d, malformed %d",
                 len(rows), bad, path, total, malformed)
        batch, started = [], time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("Kafka error (skipped): %s", msg.error())
                else:
                    batch.append(msg)
            if len(batch) >= BATCH_SIZE or (batch and time.monotonic() - started >= BATCH_SECONDS):
                flush()
            elif not batch:
                started = time.monotonic()
    except KeyboardInterrupt:
        log.info("Ctrl+C: landing the last batch before stopping")
        flush()
    finally:
        consumer.close()  # commits the stored offsets
        log.info("lander stopped: %d records landed in this run", total)


if __name__ == "__main__":
    main()
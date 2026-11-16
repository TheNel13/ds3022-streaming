"""producer.py: Bluesky posts (Jetstream WebSocket) -> Kafka topic bluesky_posts.

  python producer.py                    # live, runs until Ctrl+C
  python producer.py --limit 60000      # stop after 60,000 messages
  python producer.py --replay 30        # start 30 minutes in the past (Jetstream cursor), then continue live

Every message goes to Kafka in the same envelope:
  {"action": "history" | "create" | "delete" | "update" | "identity" | "account", "data": <the Jetstream event>}
"history" = an event replayed from before the producer started (cursor), everything else is live.
Kafka key = did (the account): all events of one account land in one partition, in order.

Connection code adapted from probe.py (probe_ws) in the DP3 kit.
"""
import argparse
import asyncio
import json
import logging
import os
import time

from confluent_kafka import KafkaException, Producer
from tornado.httpclient import HTTPRequest
from tornado.websocket import websocket_connect

TOPIC = "bluesky_posts"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
USER_AGENT = "DS3022-Project3-producer/1.0 (UVA in Valencia student project)"
RECONNECT_PAUSE = 5      # seconds to wait before reconnecting after a dropped connection
LOG_EVERY = 1000         # one progress line every 1,000 messages

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

stats = {"sent": 0, "delivered": 0, "failed": 0, "no_key": 0, "not_json": 0, "reconnects": 0}


def on_delivery(err, msg):
    if err is not None:
        stats["failed"] += 1
        log.warning("delivery failed: %s", err)
    else:
        stats["delivered"] += 1


def envelope(event, live_since_us):
    """Same shape for every message: {"action": ..., "data": <original event>}."""
    time_us = event.get("time_us") or 0
    if time_us and time_us < live_since_us:
        action = "history"
    elif event.get("kind") == "commit":
        action = (event.get("commit") or {}).get("operation") or "commit"
    else:
        action = event.get("kind") or "unknown"
    return {"action": action, "data": event}


def send(producer, event, live_since_us):
    did = event.get("did")
    if not did:  # missing key: log it, send with no key, never crash
        stats["no_key"] += 1
        log.warning("event without did (sent with key=None)")
    value = json.dumps(envelope(event, live_since_us), ensure_ascii=False).encode("utf-8")
    key = did.encode("utf-8") if did else None
    while True:
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            break
        except BufferError:  # local queue full: let it drain, then try again
            producer.poll(1)
    producer.poll(0)
    stats["sent"] += 1
    if stats["sent"] % LOG_EVERY == 0:
        log.info("sent %d (delivered %d, failed %d, no key %d, not JSON %d, reconnects %d)",
                 stats["sent"], stats["delivered"], stats["failed"], stats["no_key"],
                 stats["not_json"], stats["reconnects"])


async def run(limit, replay_minutes):
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "linger.ms": 50})
    live_since_us = int(time.time() * 1_000_000)
    cursor = live_since_us - replay_minutes * 60 * 1_000_000 if replay_minutes else None
    if cursor:
        log.info("replaying from %d minutes ago (cursor=%d)", replay_minutes, cursor)

    try:
        while True:
            url = URL + (f"&cursor={cursor}" if cursor else "")
            try:
                ws = await websocket_connect(HTTPRequest(url, headers={"User-Agent": USER_AGENT}),
                                             ping_interval=15, ping_timeout=30)
                log.info("connected to Jetstream%s", f" (cursor {cursor})" if cursor else "")
                while True:
                    text = await ws.read_message()
                    if text is None:
                        raise ConnectionError("server closed the connection")
                    try:
                        event = json.loads(text)
                    except ValueError:
                        stats["not_json"] += 1
                        log.warning("skipped a message that is not JSON")
                        continue
                    if not isinstance(event, dict):
                        stats["not_json"] += 1
                        log.warning("skipped a JSON message that is not an object")
                        continue
                    send(producer, event, live_since_us)
                    # remember where we are: after a drop we resume from here (no gap)
                    if event.get("time_us"):
                        cursor = event["time_us"]
                    if limit and stats["sent"] >= limit:
                        log.info("limit of %d messages reached", limit)
                        ws.close()
                        return
            except Exception as e:  # noqa: BLE001  (network errors from tornado vary)
                stats["reconnects"] += 1
                log.warning("connection lost (%s: %s); reconnecting in %d s",
                            type(e).__name__, e, RECONNECT_PAUSE)
                await asyncio.sleep(RECONNECT_PAUSE)
    finally:
        left = producer.flush(30)
        log.info("done: sent %d, delivered %d, failed %d, still queued %d",
                 stats["sent"], stats["delivered"], stats["failed"], left)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--limit", type=int, default=0, help="stop after this many messages (0 = run forever)")
    p.add_argument("--replay", type=int, default=0, help="start this many minutes in the past (Jetstream cursor)")
    args = p.parse_args()
    try:
        asyncio.run(run(args.limit, args.replay))
    except KeyboardInterrupt:
        log.info("stopped by Ctrl+C")


if __name__ == "__main__":
    main()
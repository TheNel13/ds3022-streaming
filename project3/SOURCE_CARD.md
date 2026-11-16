# Source Design Card (checkpoint A)

**Name:** Nelson Lattimer
**Date:** Mon 16 Nov 2026 (I started the project late; the card was filled on this day with measured numbers)

## 1. The source
- Source and URL: Bluesky posts through Jetstream, `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post`
- Method (WebSocket push, REST polling or REST history pull): WebSocket push
- Account, key or User-Agent needed: none (I send a User-Agent header anyway)
- Limits I know about (rate limits, paging, reconnects): no rate limit, but it is fast; the connection can drop, so the producer reconnects after 5 s and resumes from the last `time_us` (cursor). Replay with `?cursor=` only goes back a limited window.

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "did": "did:plc:zptju672tr42xl5o7rvxv25l",
  "time_us": 1794865648752795,
  "kind": "commit",
  "commit": {
    "rev": "3n2hiwqhuwk22",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3n2hiwq4klk24",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-16T21:47:28.185Z",
      "langs": ["en"],
      "reply": {"parent": {"cid": "bafyrei...", "uri": "at://..."}, "root": {"cid": "bafyrei...", "uri": "at://..."}},
      "text": "Yes"
    },
    "cid": "bafyreiclcigs..."
  }
}
```

## 3. Volume maths
- Measured rate (from the probe): 50.16 messages per second (3,012 in 60 s, Mon 16 Nov, 22:47 CET)
- Live capture needed for 50,000: about 17 minutes
- History / backfill available? How many days? Yes, a limited replay with `?cursor=<time_us>` (a few hours is enough for me; I replayed 2 hours)
- My plan to reach 50,000 raw records: `python producer.py --replay 120` replays the last 2 hours (about 360,000 posts at 50/s) and then continues live. Reached 52,000 landed within minutes.

## 4. Topic design
- Topic name: `bluesky_posts` (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: `did` (the account id)
- Why this key (ordering, spread across partitions): all events of one account go to the same partition in order, so a post's create and its later delete stay in order. There are millions of accounts, so the keys spread evenly over the 3 partitions.

## 5. Duplicates
- What makes two records **the same Kafka record**? The same `kafka_partition` + `kafka_offset` (the lander landed it twice after a crash).
- What makes two records **the same thing**? The same post: `did` + `commit.rkey` (+ `operation`, because a post can be created and later deleted).
- If the same thing can arrive in several versions, which one do I keep? The `create` event (it has the text and language), with the lowest offset; a later `delete` only sets a `deleted_later` flag.

## 6. Research question (draft)
- When is each language community active on Bluesky? For every 15-minute window, what share of new posts is in each of the top languages, and do languages differ in how often they reply?

## 7. Self-check (checkpoint A)
- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
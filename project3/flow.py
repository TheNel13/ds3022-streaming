"""flow.py: Prefect + DuckDB processing for the Bluesky pipeline.

  python flow.py                 # full data: data/raw/*.parquet  -> bluesky.duckdb, charts/*.png
  python flow.py data/sample     # committed sample                -> bluesky_sample.duckdb, charts/*_sample.png

Tasks: load raw -> stage -> entities (posts) -> marts -> charts.
Every table is CREATE OR REPLACE, so running it twice gives the same numbers (idempotent).

Research question: when is each language community active on Bluesky? For every 15-minute window
(event time), what share of new posts is in each of the top languages, and how does the reply rate differ?
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from prefect import flow, get_run_logger, task  # noqa: E402

FOLDER = sys.argv[1].replace("\\", "/").rstrip("/") if len(sys.argv) > 1 else "data/raw"
RAW = FOLDER + "/*.parquet"
SUFFIX = "_sample" if "sample" in FOLDER else ""
DB = f"bluesky{SUFFIX}.duckdb"
CHART = f"charts/language_share{SUFFIX}.png"
WINDOW = "15 minutes"
TOP_LANGS = 5


def sql(query, fetch=False):
    with duckdb.connect(DB) as con:
        result = con.execute(query)
        return result.fetchall() if fetch else None


@task
def load_raw():
    """Everything the lander wrote, untouched (malformed rows and re-landed copies included)."""
    sql(f"CREATE OR REPLACE TABLE raw AS SELECT * FROM read_parquet('{RAW}')")
    n = sql("SELECT count(*) FROM raw", fetch=True)[0][0]
    get_run_logger().info("load_raw: %d rows from %s", n, RAW)


@task
def stage():
    """Kafka-record level: drop malformed rows, remove re-landed copies (same partition + offset),
    parse the nested fields from raw, cast types, trim text, keep only post events."""
    sql("""
        CREATE OR REPLACE TABLE stage AS
        WITH clean AS (
            SELECT *
            FROM raw
            WHERE action <> 'malformed'
            QUALIFY row_number() OVER (PARTITION BY kafka_partition, kafka_offset ORDER BY landed_at) = 1
        ), parsed AS (
            SELECT *, CAST(raw AS JSON) AS j FROM clean
        )
        SELECT
            kafka_partition, kafka_offset, kafka_key, action,
            did, rkey, operation,
            make_timestamp(time_us)                                     AS event_time,  -- UTC, Jetstream receive time
            TRY_CAST(j ->> '$.data.commit.record.createdAt' AS TIMESTAMP) AS created_at, -- set by the client
            nullif(trim(j ->> '$.data.commit.record.text'), '')         AS text,
            lower(j ->> '$.data.commit.record.langs[0]')                AS lang,
            (j -> '$.data.commit.record.reply') IS NOT NULL             AS is_reply,
            (j -> '$.data.commit.record.embed') IS NOT NULL             AS has_embed
        FROM parsed
        WHERE kind = 'commit'                       -- identity / account events are not posts
          AND collection = 'app.bsky.feed.post'
          AND did IS NOT NULL AND rkey IS NOT NULL
          AND time_us IS NOT NULL
    """)
    n = sql("SELECT count(*) FROM stage", fetch=True)[0][0]
    get_run_logger().info("stage: %d post events", n)


@task
def entities():
    """Thing level: one row per post (did + rkey). The same post sent twice (replay) is two Kafka
    records but one post. Keep the create event (it carries the text); flag posts deleted later."""
    sql("""
        CREATE OR REPLACE TABLE posts AS
        WITH creates AS (
            SELECT * FROM stage
            WHERE operation = 'create'
            QUALIFY row_number() OVER (PARTITION BY did, rkey ORDER BY kafka_partition, kafka_offset) = 1
        ), deletes AS (
            SELECT DISTINCT did, rkey FROM stage WHERE operation = 'delete'
        )
        SELECT c.*, (d.rkey IS NOT NULL) AS deleted_later
        FROM creates c
        LEFT JOIN deletes d ON c.did = d.did AND c.rkey = d.rkey
    """)
    n = sql("SELECT count(*) FROM posts", fetch=True)[0][0]
    get_run_logger().info("entities: %d unique posts", n)


@task
def marts():
    """Time windows + comparison: share of each top language in every 15-minute window."""
    sql(f"""
        CREATE OR REPLACE TABLE mart_top_langs AS
        SELECT lang, count(*) AS posts
        FROM posts WHERE lang IS NOT NULL
        GROUP BY lang ORDER BY posts DESC LIMIT {TOP_LANGS}
    """)
    sql(f"""
        CREATE OR REPLACE TABLE mart_lang_windows AS
        WITH labelled AS (
            SELECT time_bucket(INTERVAL '{WINDOW}', event_time) AS window_start,
                   CASE WHEN lang IN (SELECT lang FROM mart_top_langs) THEN lang
                        WHEN lang IS NULL THEN '(none)' ELSE 'other' END AS lang_group,
                   is_reply
            FROM posts
        )
        SELECT window_start, lang_group,
               count(*) AS posts,
               round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY window_start), 2)::DOUBLE AS pct_of_window,
               round(100.0 * avg(is_reply::INT), 2)::DOUBLE AS pct_replies
        FROM labelled
        GROUP BY window_start, lang_group
        ORDER BY window_start, lang_group
    """)
    sql("""
        CREATE OR REPLACE TABLE mart_lang_summary AS
        SELECT lang_group,
               sum(posts)                                        AS posts,
               round(min(pct_of_window), 1)                      AS min_pct_of_window,
               round(max(pct_of_window), 1)                      AS max_pct_of_window,
               arg_max(window_start, pct_of_window)              AS peak_window,
               round(sum(posts * pct_replies) / sum(posts), 1)   AS pct_replies
        FROM mart_lang_windows
        GROUP BY lang_group ORDER BY posts DESC
    """)
    log = get_run_logger()
    for row in sql("SELECT * FROM mart_lang_summary", fetch=True):
        log.info("summary: lang=%s posts=%s share %s%%-%s%% (peak %s) replies=%s%%", *row)


@task
def charts():
    rows = sql("""
        SELECT window_start, lang_group, pct_of_window FROM mart_lang_windows
        WHERE lang_group NOT IN ('other', '(none)') ORDER BY window_start
    """, fetch=True)
    if not rows:
        get_run_logger().warning("charts: no data to plot")
        return
    os.makedirs("charts", exist_ok=True)
    series = {}
    for window_start, lang, pct in rows:
        series.setdefault(lang, ([], []))
        series[lang][0].append(window_start)
        series[lang][1].append(pct)
    fig, ax = plt.subplots(figsize=(11, 5.5))
    for lang, (xs, ys) in sorted(series.items(), key=lambda kv: -sum(kv[1][1])):
        ax.plot(xs, ys, marker="o", markersize=3, linewidth=1.5, label=lang)
    ax.set_title(f"Bluesky: share of new posts by language, per {WINDOW} window"
                 + (" (sample)" if SUFFIX else ""))
    ax.set_xlabel("Window start (UTC, event time)")
    ax.set_ylabel("Share of posts in the window (%)")
    ax.grid(alpha=0.3)
    ax.legend(title="Language (record.langs[0])")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(CHART, dpi=130)
    plt.close(fig)
    get_run_logger().info("charts: wrote %s", CHART)


@task
def four_numbers():
    raw_n, malformed, unique_kafka = sql("""
        SELECT count(*),
               count(*) FILTER (WHERE action = 'malformed'),
               count(DISTINCT kafka_partition || '-' || kafka_offset) FILTER (WHERE action <> 'malformed')
        FROM raw
    """, fetch=True)[0]
    unique_posts = sql("SELECT count(*) FROM posts", fetch=True)[0][0]
    log = get_run_logger()
    log.info("FOUR NUMBERS (%s)", FOLDER)
    log.info("  raw records landed     : %d", raw_n)
    log.info("  malformed (kept in raw): %d", malformed)
    log.info("  unique Kafka records   : %d", unique_kafka)
    log.info("  unique posts           : %d", unique_posts)
    return raw_n, malformed, unique_kafka, unique_posts


@flow(name="bluesky-pipeline", log_prints=True)
def pipeline():
    load_raw()
    stage()
    entities()
    marts()
    charts()
    return four_numbers()


if __name__ == "__main__":
    pipeline()
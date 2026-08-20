# Architecture Decisions & Debugging Log

Running notes on *why* things were built the way they were, and real bugs
worth remembering. Meant to feed the final README / portfolio write-up in
Phase 7 — not polished, just accurate.

## Phase 1 — Data Quality & Validation

- Used Pydantic (`TradeData` model) as a "zero-trust" validation layer between
  Kafka and the database. Any message that doesn't match the expected shape
  (missing field, wrong type, invalid price) is rejected and logged, never
  written to the DB. The pipeline should never trust upstream data blindly.

## Phase 2 — Kafka Architecture

- Producers route messages with the trade symbol as the Kafka **key**
  (`producer.send(KAFKA_TOPIC, key=routing_key, value=log_data)`), enabling
  partition-based routing.
- Consumer uses a named consumer group (`market_data_team`) so multiple
  consumer instances could load-balance partitions in the future, rather than
  each one reading everything.

## Phase 3 — TimescaleDB Optimization

- `market_ticks` is a TimescaleDB **hypertable**, not a plain table — built
  for efficient time-series storage/queries at scale.
- Built `market_1m_candles` as a **continuous aggregate**
  (`WITH (timescaledb.continuous)`) instead of a plain materialized view, so
  1-minute OHLC candles are incrementally maintained by TimescaleDB itself
  rather than recomputed from scratch on every query.
- Used `WITH NO DATA` at creation time — populating a continuous aggregate
  immediately would force a full scan of historical data; better to control
  the first refresh explicitly.
- Refresh policy (`add_continuous_aggregate_policy`) tuned as:
  `start_offset = 1 hour`, `end_offset = 1 minute`, `schedule_interval = 1 minute`.
  `end_offset` deliberately excludes the current, still-in-progress minute so
  candles aren't reported before they're actually closed. `start_offset`
  gives a rolling window for late-arriving data without rescanning everything.
- Added `candle_readable`, a plain (non-continuous) view wrapping
  `market_1m_candles` with `to_char(bucket, 'YYYY-MM-DD HH24:MI')` for
  human-friendly `psql` output — kept separate from the raw aggregate because
  Grafana specifically needs a real `TIMESTAMPTZ` column, not a formatted
  string, to plot time series correctly.
- Set the database's default display timezone via
  `ALTER DATABASE marketdata SET timezone TO 'America/New_York'` — this
  changes only how existing UTC-stored timestamps *display*, not what's
  actually stored. Timestamps stay in UTC internally, which is the correct
  long-term choice for a system that might have producers/consumers/viewers
  in different timezones.
- `add_continuous_aggregate_policy` has no `IF NOT EXISTS` equivalent, unlike
  table/view creation — re-running it throws `DuplicateObject`. Worked around
  this by manually checking `timescaledb_information.jobs` before calling it,
  so `setup_aggregates()` is safely re-runnable like the rest of the setup
  functions.

## Bugs worth remembering

- **Kafka consumer group coordination failure (the big one).** Consumer kept
  timing out on `FIND_COORDINATOR` with no clear error pointing at the cause.
  Root cause: two single-character typos in `docker-compose.yml` environment
  variable *names* —  `KAFKA_INTER_BROKER_LISTERNER_NAME` (should be
  `LISTENER`) and `KAFKA_OFFSET_TOPIC_REPLICATION_FACTOR` (should be
  `OFFSETS`). Docker/Kafka silently ignores unrecognized env var names and
  falls back to defaults instead of erroring — so Kafka was quietly using a
  replication factor of 3 with only 1 broker available, which meant its
  internal `__consumer_offsets` topic could never be created, which meant
  every consumer group operation failed. Lesson: infrastructure config typos
  in variable *names* fail silently and can look exactly like an application
  bug from the outside.
- **Producer/consumer field-name mismatch after a rename.** Renamed
  `precentage` → `percentage` in the `TradeData` Pydantic model, the DB
  column, and the `INSERT` statement — but forgot the producer scripts
  (`log_crypto_mvp.py`, `log_ticks_mvp.py`) still built their Kafka payload
  with the old key. Every message got rejected by Pydantic validation
  (`Field required: percentage`) until both producers were updated too.
  Lesson: a field name crossing a producer/consumer boundary has to be
  updated on *every* side of that boundary, not just wherever you're
  currently looking.
- **TimescaleDB hypertable chunk duplicate-column issue.** During the same
  rename, running `ALTER TABLE ... ADD COLUMN IF NOT EXISTS percentage`
  before renaming the old column created a second, empty `percentage` column
  alongside the original `precentage` one — because hypertables propagate
  schema changes down into their underlying chunk tables. Had to `DROP` the
  empty duplicate before `RENAME COLUMN precentage TO percentage` would
  succeed.
- **Ephemeral container storage.** Force-recreating a container
  (`docker-compose up -d --force-recreate <service>`) destroys everything
  inside that container's filesystem unless a Docker volume is mounted.
  Learned this by losing the Grafana dashboard entirely after a
  force-recreate done to pick up a password change. Fixed by adding named
  volumes (`grafana-data:/var/lib/grafana`,
  `db-data:/var/lib/postgresql/data`) for both stateful services. Verified
  persistence by checking row counts before/after a real `docker-compose
  restart`, not just trusting that nothing had broken.
- **`.env` committed to git despite `.gitignore`.** A secret got committed
  and pushed to GitHub before the ignore rule was fully effective (or was
  force-added at some point, overriding `.gitignore`). Deleting the file
  locally afterward does *not* remove it from git history or undo the
  exposure — the only real fix is rotating the actual credential at the
  source (Alpaca dashboard), which was done. `.gitignore` correctly prevents
  it from happening again going forward, verified via `git status` staying
  clean after recreating `.env`.
- **PowerShell path/env gotchas** (worth remembering, less severe):
  `.env` needs `.\` prefix to run as a relative path, not just `.` or `\`;
  `$env:VAR` variables only exist for the current terminal session and are
  never written to disk — losing them is expected, not a bug.
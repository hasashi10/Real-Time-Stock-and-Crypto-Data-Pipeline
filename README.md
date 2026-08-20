# Real-Time Stock & Crypto Data Pipeline

An end-to-end streaming data pipeline that ingests live stock and
cryptocurrency trade data, validates it, stores it in a time-series
database, and visualizes it on live dashboards — fully containerized
with Docker.

**Data flow:** Alpaca API → Kafka → TimescaleDB → Grafana

## Why this project

Built to go deep on the "why" behind real data infrastructure decisions,
not just get a pipeline working — data validation, distributed messaging,
time-series optimization, observability, and security, in that order.
See [`DECISIONS.md`](./DECISIONS.md) for the full reasoning and debugging
log behind each choice.

## Architecture

```
Alpaca WebSocket (stocks + crypto)
        │
        ▼
  Kafka producers  ──▶  Kafka topic (market_ticks)
                              │
                              ▼
                     Kafka consumer group
                    (Pydantic validation)
                              │
                              ▼
                   TimescaleDB hypertable
                    (market_ticks)
                              │
                              ▼
              Continuous aggregate (market_1m_candles)
                    1-minute OHLC candles,
                    auto-refreshed on schedule
                              │
                              ▼
                    Grafana dashboards
              (candlestick charts + pipeline health)
```

## Components

- **`log_crypto_mvp.py`** — streams live crypto trades (BTC/USD, ETH/USD,
  DOGE/USD) from Alpaca's crypto WebSocket, produces to Kafka.
- **`log_ticks_mvp.py`** — streams live stock trades (AAPL, MSFT, GOOG,
  BLK) via Alpaca's IEX feed during market hours, produces to Kafka.
- **`consumer.py`** — consumes from Kafka, validates every message
  against a strict Pydantic schema (rejecting malformed data before it
  ever reaches the database), writes clean data to TimescaleDB, and sets
  up the database schema, continuous aggregate, refresh policy, and a
  human-readable view — all idempotently, so the whole schema rebuilds
  itself correctly on a fresh database.
- **`docker-compose.yml`** — Zookeeper, Kafka, TimescaleDB, and Grafana,
  wired together with persistent volumes and health checks.

## Key engineering decisions

- **Zero-trust data validation.** Every message is validated against a
  Pydantic model before it's written anywhere. Invalid data (missing
  fields, bad types, suspicious prices) is logged and dropped, never
  silently accepted.
- **Kafka partition routing + consumer groups.** Messages are keyed by
  symbol for partition-aware routing; the consumer runs under a named
  consumer group for load-balanced, fault-tolerant processing.
- **TimescaleDB continuous aggregates**, not application-level
  aggregation. 1-minute OHLC candles are computed and incrementally
  maintained by the database itself on a schedule, so dashboard queries
  never have to scan raw tick data.
- **Secrets management.** API keys and database passwords live in a
  git-ignored `.env` file and are injected via `python-dotenv` (app
  layer) and Docker Compose's native `${VAR}` substitution
  (infrastructure layer) — never hardcoded.
- **Persistent, health-checked infrastructure.** Both stateful services
  (TimescaleDB, Grafana) run on named Docker volumes so data and
  dashboards survive container recreation, not just restarts. Health
  checks (`pg_isready`, `kafka-topics --list`) let Grafana wait for a
  genuinely ready database instead of just a started container.

## Setup

1. Clone the repo and copy `.env.example` to `.env`, filling in your own
   Alpaca API key/secret and database passwords.
2. `docker-compose up -d` — starts Kafka, Zookeeper, TimescaleDB, and
   Grafana.
3. `python consumer.py` — connects to Kafka and sets up the full
   database schema (table, hypertable, continuous aggregate, refresh
   policy, readable view) automatically.
4. In a separate terminal, run `python log_crypto_mvp.py` (crypto trades
   24/7) and/or `python log_ticks_mvp.py` (stock trades, market hours
   only).
5. Open Grafana at `http://localhost:3000`, connect a PostgreSQL data
   source pointing at `db:5432` / database `marketdata`, and build
   candlestick panels against `market_1m_candles`.

## What I'd build next

- Automated tests for the Pydantic validation layer and the
  trend-detection logic in the producers.
- A Grafana dashboard template variable (`$symbol`) instead of one panel
  per symbol, so adding a new instrument doesn't require duplicating a
  panel by hand.
- Alerting on pipeline health (e.g., notify if tick volume drops to zero
  during expected trading hours).
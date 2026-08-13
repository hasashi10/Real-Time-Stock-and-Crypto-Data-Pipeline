# Real-Time Market Data & Crypto Pipeline

## 👋 The Story Behind the Code
Hi! I built this pipeline to bridge the gap between classroom theory and real-world system architecture. As I focus my career on Data Engineering and Cybersecurity, I wanted a hands-on environment to deeply learn Python scripting, handle messy live data, and understand how enterprise systems stay resilient under pressure. 

This isn't just a tutorial project—it's a live sandbox where I am actively learning how to build fault-tolerant data streams, secure containers, and optimize databases. 

## 📖 Project Overview
A resilient, real-time data engineering pipeline designed to ingest, process, and store live stock and cryptocurrency market ticks. By transforming raw WebSocket streams into highly optimized time-series data, this project demonstrates core concepts in data quality, decoupled architecture, and system security.

## 🛠️ Technology Stack
*   **Data Ingestion:** Alpaca API (WebSockets)
*   **Message Broker:** Apache Kafka (Decoupling, buffering, and stream processing)
*   **Database:** TimescaleDB / PostgreSQL (Hypertable-optimized time-series storage)
*   **Containerization:** Docker & Docker Compose
*   **Language:** Python (3.x)
*   **Visualization:** Grafana (Upcoming)

---

## ✅ Currently Implemented Features

### 1. Core Data Flow
*   Live WebSocket connection to Alpaca API filtering for specific Crypto/Stock tickers.
*   Kafka Producer script (`log_crypto_mvp.py`) publishing formatted JSON payloads.
*   Kafka Consumer script (`consumer.py`) reading streams and executing rapid database inserts.
*   Automated TimescaleDB schema generation (Hypertables).

### 2. Phase 1: Data Quality & Defensive Parsing
*   **Schema Validation:** Incoming Kafka JSON payloads are intercepted and validated against a strict schema before database insertion.
*   **Defensive Type Checking:** Verifies numeric types (`int`, `float`) for critical trading metrics (e.g., `price`) to prevent database type-mismatch crashes.
*   **Non-Blocking Control Flow:** Invalid payloads trigger structured warning logs and utilize Python `continue` statements to safely bypass execution without halting the consumer loop.
*   **Structured Logging:** Standardized Python `logging` capturing `timestamp`, `levelname`, and operational context for future containerized log aggregation.

---

## 🚀 Development Roadmap

*   [x] **Phase 1: Data Quality & Validation** (Defensive parsing, error handling, logging)
*   [ ] **Phase 2: Kafka Architecture** (Partitioning, keys, topic design, consumer groups)
*   [ ] **Phase 3: TimescaleDB Optimization** (Continuous aggregates, hypertables, compression)
*   [ ] **Phase 4: Pipeline Monitoring** (Grafana metrics for both market data & system health)
*   [ ] **Phase 5: Docker & Security** (Health checks, secrets management via `.env`)
*   [ ] **Phase 6: Automated Testing** (Unit testing parsing, validation, database writes)
*   [ ] **Phase 7: Documentation** (Architecture diagrams, final polish)

---

## 💻 How to Run Locally (MVP)
1. Start the infrastructure: `docker-compose up -d`
2. Activate Virtual Environment: `.\venv\Scripts\Activate.ps1`
3. Run Consumer: `python consumer.py`
4. Run Producer: `python log_crypto_mvp.py`
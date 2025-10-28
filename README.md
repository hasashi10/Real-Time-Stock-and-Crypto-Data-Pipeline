#Real-Time Stock and Crypto Data Pipeline

This is a complete, end-to-end data engineering project that captures real-time stock and cryptocurrency data, processes it, and stores it in a time-series database.

The pipeline is built on a modern, decoupled architecture using Python, Apache Kafka, and TimescaleDB, with all infrastructure managed by Docker.

Project Architecture

This project uses a producer-consumer model to create a resilient and scalable pipeline:

Producers: Two independent Python scripts (producer_stocks.py and producer_crypto.py) connect to the Alpaca WebSocket APIs to stream live trade/quote data.

Message Queue: When a price change is detected, the producers send the data to an Apache Kafka topic (market_ticks). This decouples the ingestion from the database, allowing the system to handle high volumes of data without data loss.

Consumer: A single consumer script (consumer.py) subscribes to the Kafka topic, reads the messages in real-time, and writes them to a TimescaleDB (PostgreSQL) database for permanent storage and analysis.

Local Backup: The producers also write a local .csv backup for redundancy.

Architecture Diagram

flowchart TD
    subgraph "Data Sources (Alpaca API)"
        A[Stock WebSocket]
        B[Crypto WebSocket]
    end

    subgraph "Python Producers"
        P1(producer_stocks.py)
        P2(producer_crypto.py)
    end

    subgraph "Messaging & Storage (Docker)"
        K[Apache Kafka <br> (Topic: market_ticks)]
        DB[(TimescaleDB <br> /Postgres)]
    end

    subgraph "Python Consumer"
        C(consumer.py)
    end

    A --> P1
    B --> P2

    P1 --"JSON Message"--> K
    P2 --"JSON Message"--> K

    K --"Reads Messages"--> C

    C --"Writes Data"--> DB


Technologies Used

Python: The core language for all scripts.

Alpaca API: Real-time WebSocket API for stock and crypto data.

Apache Kafka: High-throughput, distributed message queue.

PostgreSQL / TimescaleDB: Time-series database for efficient storage and querying of market data.

Docker / Docker Compose: For containerizing and running the backend infrastructure (Kafka, Zookeeper, and TimescaleDB).

Python Libraries: alpaca-py, kafka-python, psycopg2-binary.

How to Run This Project

Prerequisites:

Python 3.10+

Docker Desktop (must be running)

An Alpaca Paper Trading account (to get API keys)

Set Up Environment:

Create a Python virtual environment:

python -m venv venv
.\venv\Scripts\activate


Install the required libraries:

pip install -r requirements.txt


Add API Keys:

Create a file named config.py.

Add your Alpaca API keys to this file:

# config.py
API_KEY = "YOUR_ALPACA_KEY_ID"
SECRET_KEY = "YOUR_ALPACA_SECRET_KEY"


Start the Infrastructure:

This one command will start Kafka, Zookeeper, and the TimescaleDB database in the background.

docker-compose up -d


Run the Pipeline:

You need to run the consumer and producers in separate terminals.

Terminal 1 (Run the Consumer):

.\venv\Scripts\activate
python consumer.py


(This will connect and wait for messages...)

Terminal 2 (Run the Crypto Producer):

.\venv\Scripts\activate
python producer_crypto.py  # Or whatever you named your crypto script


Terminal 3 (Run the Stock Producer):

.\venv\Scripts\activate
python producer_stocks.py   # Or whatever you named your stock script


View Your Data:

You can now connect to the marketdata database (at localhost:5432) using any SQL client (like the VSCode PostgreSQL extension) to see your data flowing into the market_ticks table.

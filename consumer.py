import psycopg2
from kafka import KafkaConsumer
import json
import time
import logging
from pydantic import BaseModel, field_validator, ValidationError
from datetime import datetime

class TradeData(BaseModel):
    symbol: str
    price: float
    timestamp: datetime
    direction: str
    percentage: float

    @field_validator('price')
    @classmethod
    def check_valid_price(cls, value):
        if value <= 0:
            raise ValueError(f"suspicious price detected: {value}")
        return value

    
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%y-%m-%d %H:%M'
)
# kafka consumer setup---
def get_kafka_consumer():
    print("connecting to kafka . .")
    try:
        consumer = KafkaConsumer(
            'market_ticks',
            bootstrap_servers=['localhost:9092'],
            group_id='market_data_team',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        print("kafka consumer connected succesfully.")
        return consumer
    except Exception as e:
        print(f" FAILED TO CONNECT TO KAFKA !!!")
        print(f" Error: {e}")
        print("please make sure the Docker containers are running ( ' Docker-compose up -d').")
        exit()

# database connection-----
def get_db_connection():
    """
    connects to the progres/TimescaleDB database. we use the TimescaleDB extension because it
    stores thousands of timestamps and prices for this type of project
    this will keep trying until it succeds.
    """
    while True:
        try:
            conn = psycopg2.connect(
                #this matches my docker-compose.yml file
                host="localhost",
                database="marketdata",
                user="admin",
                password="password"
            )
            print("database connection succesful.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Database connection failed:{e}. retrying in 5 seconds.....")
            time.sleep(5)
def setup_database(conn):
    """
    here is where we create our main table, columns and converts it
    in to a timescaleDB Hypertable
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_ticks (
                time TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                direction TEXT,
                percentage DOUBLE PRECISION DEFAULT 0.0
            );
        """)
        conn.commit()
        print("table 'market_ticks' created.")

        cur.execute("""
            ALTER DATABASE marketdata SET timezone TO 'America/New_York';
        """)
        
        cur.execute("""
            ALTER TABLE market_ticks
            ADD COLUMN IF NOT EXISTS percentage DOUBLE PRECISION DEFAULT 0.0;
        """)
        cur.execute("""
            SELECT create_hypertable('market_ticks', 'time', if_not_exists => TRUE);
        """)
        conn.commit()
        print("table 'market_ticks' and hypertable are ready with 'percentage' column.")
def setup_aggregates(conn):
    """
    Creates the market_1m_candle continuous aggregate, 
    pre-computing 1-minute OHLC candle from raw ticks
    """
    with conn.cursor() as cur:
            cur.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS market_1m_candles
                with (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 minute', time) AS bucket,
                    symbol,
                    FIRST(price, time) AS open,
                    MAX(price) AS high,
                    MIN(price) AS low,
                    LAST(price, time) AS close,
                    COUNT(*) AS trade_count
                FROM market_ticks
                GROUP BY bucket, symbol
                WITH NO DATA;
             """)
            conn.commit()
            print("continuous aggregate 'market_1m_candles' is ready.")

            cur.execute("""
                SELECT COUNT(*) FROM timescaledb_information.jobs
                WHERE hypertable_name = (
                    SELECT materialization_hypertable_name
                    FROM timescaledb_information.continuous_aggregates
                    WHERE view_name = 'market_1m_candles');
            """)
            policy_exists = cur.fetchone()[0] > 0
            if not policy_exists:
                cur.execute("""
                    SELECT add_continuous_aggregate_policy('market_1m_candles',
                    start_offset => INTERVAL '1 hour',
                    end_offset => INTERVAL '1 minute',
                    schedule_interval => INTERVAL '1 minute');
                """ )
                conn.commit()
                print("refresh policy for 'market_1m_candles' is set")
            else:
                print("refresh policy for 'market_1m_candles' already exists, skipping.")
def setup_views(conn):
    """
    creates a human-readable view of market_1m_candles,
    formatting the timestamd as 'YYYY-MM-DD HH24:MI' for easier reading
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE VIEW candle_readable AS
            SELECT
                to_char(bucket, 'YYYY-MM-DD HH24:MI') AS time,
                symbol,
                open,
                high,
                low,
                close,
                trade_count
            FROM market_1m_candles
            ORDER BY bucket DESC;
        """)
        conn.commit()
        print("view 'candle_readable' is ready.")
    #---main consumer loop---
def consume_and_write():
    consumer = get_kafka_consumer()
    conn = get_db_connection()
    setup_database(conn)
    setup_aggregates(conn)
    setup_views(conn) 
    insert_sql = """
        INSERT INTO market_ticks(time, symbol, price, direction, percentage)
        VALUES (%s, %s, %s, %s, %s);
    """
    print("\nStarting kafka consumer loop... waiting for messages... ")
    try:
        for message in consumer:
            data = message.value
            try:
                clean_trade = TradeData.model_validate(data)
            except ValidationError as e:
                logging.warning(f"___invalid data Rejected____\n{e}")
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_sql, (
                        clean_trade.timestamp,
                        clean_trade.symbol,
                        clean_trade.price,
                        clean_trade.direction,
                        clean_trade.percentage
                    ))
                    conn.commit()
                    logging.info(f"--- wrote to DB: {clean_trade.symbol}@{clean_trade.price}({clean_trade.percentage}%)")

            except psycopg2.Error as e:
                logging.error(f"error writing to DB: {e}")
                conn.rollback()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        conn.close()
        consumer.close()
        print("database and kafka connections closed.")
if __name__ == "__main__":
    consume_and_write()


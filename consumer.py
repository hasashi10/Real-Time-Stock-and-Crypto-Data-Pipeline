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
    precentage: float

    @field_validator('price')
    @classmethod
    def check_valid_price(cls, value):
        if value <= 0:
            raise ValueError(f"suspicious price detectect: {value}")
        return value

    
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)
# kafka consumer setup---
KAFKA_TOPIC = 'market_ticks'

print("connecting to kafka. . .")
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = ['localhost:9092'],
        #this converts the json bytes into a python dictionary
        value_deserializer = lambda v:json.loads(v.decode('utf-8')),
        #this makes sure we read from the begin of the topic if we're a new consumer
        auto_offset_reset = 'earliest'
    )
    print("kafka consumer connected succesfully.")
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
            CREATE TABLE IF NOT EXISTS market_ticks(
                time TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                direction TEXT NOT NULL,
                precentage DOUBLE PRECISION NOT NULL
            );"""
        )
        cur.execute("""
            ALTER TABLE market_ticks
            ADD COLUMN IF NOT EXISTS precentage DOUBLE PRECISION DEFAULT 0.0;
        """)
        cur.execute("""
            SELECT create_hypertable('market_ticks', 'time', if_not_exists => TRUE);
        """)
        conn.commit()
        print("table 'market_ticks' and hypertable are ready with 'precentage' column.")
    #---main consumer loop---
def consume_and_write():
    conn = get_db_connection()
    setup_database(conn)
    insert_sql = """
        INSERT INTO market_ticks(time, symbol, price, direction, precentage)
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
                        clean_trade.precentage
                    ))
                    conn.commit()
                    logging.info(f"--- wrote to DB: {clean_trade.symbol}@{clean_trade.price}({clean_trade.precentage}%)")

            except psycopg2.Error as e:
                logging.error(f"error writing to DB: {e}")
                conn.rollback()
    except KeyboardInterrupt:
        print("\nStoppiing consumer...")
    finally:
        conn.close()
        consumer.close()
        print("database and kafka connections closed.")
if __name__ == "__main__":
    consume_and_write()


import psycopg2
from kafka import KafkaConsumer
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)

def validate_payload(payload: dict) -> bool:
    """
    checks if the incoming kafka message has the required fields and correct data types
    """
    required_fields =['symbol', 'price', 'timestamp', 'direction', 'precentage']
    #checks for missing fields
    for field in required_fields:
        if field not in payload:
            logging.error(f"validation failed. missing field: '{field}'. payload: {payload}")
            return False
    #checks data types (defensive parsing)
    if not isinstance(payload.get('price'), (int, float)):
        logging.error(f"validation failed 'price' must be a number, got {type(payload.get('price'))}.")
        return False
    return True


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
            if not validate_payload(data):
                logging.warning(f"skipped invalid message for symbol: {data.get('symbol', 'UNKNOWN')}")
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_sql, (
                        data['timestamp'],
                        data['symbol'],
                        data['price'],
                        data['direction'],
                        data['precentage'] if 'precentage' in data else 0.0
                    ))
                    conn.commit()
                    logging.info(f"--- wrote to DB: {data['symbol']}@{data['price']}({data.get('precentage', 0.0)}%)")
                    
            except(psycopg2.Error, json.JSONDecodeError) as e:
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


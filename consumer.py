import psycopg2
from kafka import KafkaConsumer
import json
import time

# --- Kafka Consumer Setup ---
KAFKA_TOPIC = 'market_ticks'

print("Connecting to Kafka...")
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=['localhost:9092'],
        # This deserializes the JSON bytes back into a Python dictionary
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        # This makes sure we read from the beginning of the topic if we're a new consumer
        auto_offset_reset='earliest'
    )
    print("Kafka Consumer connected successfully.")
except Exception as e:
    print(f"!!!!!!!! FAILED TO CONNECT TO KAFKA !!!!!!")
    print(f"Error: {e}")
    print("Please make sure your Docker containers are running ('docker-compose up -d').")
    exit()


# --- Database Connection ---
def get_db_connection():
    """
    Connects to the Postgres/TimescaleDB database.
    Will keep retrying until it succeeds.
    """
    while True:
        try:
            conn = psycopg2.connect(
                # This matches your docker-compose.yml file
                host="localhost",
                database="marketdata",
                user="admin",
                password="password"
            )
            print("Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Database connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def setup_database(conn):
    """
    Creates our main table and turns it into a TimescaleDB hypertable.
    """
    with conn.cursor() as cur:
        # 1. Create the table (if it doesn't exist)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_ticks (
                time TIMESTAMPTZ NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                price DOUBLE PRECISION,
                direction VARCHAR(4)
            );
        """)
        
        # 2. Turn it into a Hypertable (if it's not already)
        # This is the "magic" of TimescaleDB for fast time-series data
        cur.execute("""
            SELECT create_hypertable('market_ticks', 'time', if_not_exists => TRUE);
        """)
        
        conn.commit()
        print("Table 'market_ticks' and hypertable are ready.")

# --- Main Consumer Loop ---
def consume_and_write():
    conn = get_db_connection()
    setup_database(conn)
    
    insert_sql = """
        INSERT INTO market_ticks (time, symbol, price, direction)
        VALUES (%s, %s, %s, %s)
    """
    
    print("\nStarting Kafka consumer loop... Waiting for messages...")
    try:
        for message in consumer:
            # message.value is the dictionary we sent from the producer
            data = message.value
            
            try:
                with conn.cursor() as cur:
                    cur.execute(insert_sql, (
                        data['timestamp'],
                        data['symbol'],
                        data['price'],
                        data['direction']
                    ))
                    conn.commit()
                    print(f"--- Wrote to DB: {data['symbol']} @ {data['price']}")
            
            except (psycopg2.Error, json.JSONDecodeError) as e:
                print(f"Error writing to DB: {e}")
                conn.rollback() # Roll back the failed transaction
    
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        conn.close()
        consumer.close()
        print("Database and Kafka connections closed.")

if __name__ == "__main__":
    consume_and_write()
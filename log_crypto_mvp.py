import config
import csv
import json
from datetime import datetime
from alpaca.data.live import CryptoDataStream
#NEW: import the kafkaproducer
from kafka import KafkaProducer

#---Kafka producer setup ---
#this producer will connect to the kafka server running in docker
try:
    producer = KafkaProducer(
        #this is the address we exposed in docker-compose.yml
        bootstrap_servers=['localhost:9092'],
        #this serializes our data (dictionary) into JSON bytes
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("kafka producer connected succesful.")
except Exception as e:
    print(f"!!!!!! FAILED TO CONNECT TO KAFKA !!!!!!")
    print(f"Error: {e}")
    print("please make sure your docker containers are running ('docker-compose up -d)")
    exit()
KAFKA_TOPIC = 'market_ticks'

#____configuration______
API_KEY = config.API_KEY
SECRET_KEY = config.SECRET_KEY
SYMBOLS = ['BTC/USD', 'ETH/USD', 'DOGE/USD'] #tickers to watch
LOG_FILE = 'market_ticks.csv'

#this dictonary will hold the last price for each symbol
previous_prices = {}
#----file setup----
#open the csv file in 'append' mode and create a writer'
try:
    csv_file = open(LOG_FILE, 'a', newline='')
    fieldnames = [ 'timestamp', 'symbol' , 'price', 'direction']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    #write headers only if the file is new (empty)
    csv_file.seek(0, 2)#go to the end of the file
    if csv_file.tell()== 0:
        writer.writeheader()
except IOError as e:
    print(f"Error opening log file: {e}")
    exit()

#---- the core logic---
#this functions is called every time a new trade (tick) come in 
async def on_quote(data):
    """
    callback  function to process each quote 
    """
    print(f"---CRYPTO RECIEVED: {data.symbol} @(Ask: {data.ask_price})")
#------------------------------------------------
    symbol = data.symbol
    price = data.ask_price
    timestamp = data.timestamp.isoformat()

    direction = None #'up' or 'down'

    if symbol in previous_prices:
        if price > previous_prices[symbol]:
            direction = 'UP'
        elif price < previous_prices[symbol]:
            direction = 'DOWN'
#updates the previous price for the *next* tick
    previous_prices[symbol] = price

#Your REquierment: only log if market is going up or down 
    if direction:
        log_data={
            'timestamp': timestamp,
            'symbol': symbol,
            'price': price,
            'direction': direction
        }
        #1. this part writes to csv
        #print to console and write to csv
        print("!!!!!!!! Price changed, logging to csv!!!!!!!!")
        print(json.dumps(log_data, indent=2))
        writer.writerow(log_data)
        csv_file.flush() #ensure it's written immediately
        #2. this is where is write to kafka
        print("!!!!!!!!PRODUCING TO KAFKA !!!!!!!")
        producer.send(KAFKA_TOPIC, value=log_data)
    else:
        print(f"----(price for {symbol} is unchanged. not logging.)")

#---main connection---
def run_connection():
    print("connecting to Alpaca WebSocket. . .")

#---Line for debugging----
    print(f"---DEBUG: Script is using API key starting with: {API_KEY[:8]}")
#-------------------------   
    #this is the new V@/V# connection method
    #it uses your keys to know if ur on paper or live
    wss_client = CryptoDataStream(
        API_KEY,
        SECRET_KEY,
        url_override="wss://stream.data.alpaca.markets/v1beta3/crypto/us"
    )
    #subscribe to trade for our symbols
    wss_client.subscribe_quotes(on_quote, *SYMBOLS)

    try:
        wss_client.run()
    except KeyboardInterrupt:
        print("\ndisconnecting...")
    except Exception as e:
        print(f"An error occured: {e}")
    finally:
        print("\nDisconnecting")
        print("closing csv file and flushing kafka producer...")
        csv_file.close()
        producer.flush()
        producer.close()
        print("connection close. log file saved.")
if __name__== "__main__":
    run_connection()


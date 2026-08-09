import config
import csv
import json
import time
from datetime import datetime
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed
from kafka import KafkaProducer

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("kafka producer connected successfully.")
except Exception as e:
    print(f"!!!!!! FAILED TO CONNECT TO KAFKA !!!!!!")
    print(f"Error: {e}")
    print("please make sure your docker containers re running (docker-compose up -d)")
    exit()

KAFKA_TOPIC = 'market_ticks'

#____configuration______
API_KEY = config.API_KEY
SECRET_KEY = config.SECRET_KEY
SYMBOLS = ['AAPL', 'MSFT', 'GOOG', 'BLK'] #tickers to watch
LOG_FILE = 'market_ticks.csv'

#this dictonary will hold the last price for each symbol
previous_prices = {}
trend_tracker = {}
#----file setup----
#open the csv file in 'append' mode and create a writer'
try:
    csv_file = open(LOG_FILE, 'a', newline='')
    fieldnames = [ 'timestamp', 'symbol' , 'price', 'direction', 'precentage']
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
async def on_trade(data):
    """
    callback  function to process each trade tick 
    """
    print(f"--TICK RECIEVED: {data.symbol} @ {data.price}")
#--------------------------
    symbol = data.symbol
    price = data.price
    timestamp = data.timestamp.isoformat()
    current_time =time.time()

    direction = None #'up' or 'down'
    precentage = 0.0

    if symbol in previous_prices:
        prev_price = previous_prices[symbol]
        precentage = ((price - prev_price)/ prev_price) *100
        if price > prev_price:
            direction = 'UP'
        elif price < prev_price:
            direction = 'DOWN'
    #-- trend tracking & notification logic ---
    if direction:
        if symbol not in trend_tracker:
            trend_tracker[symbol] = {'direction': direction, 'start_time': current_time}
        else:
            if trend_tracker[symbol]['direction'] == direction:
                elapsed_time = current_time - trend_tracker[symbol]['start_time']
                if elapsed_time > 60:
                    if direction == 'UP':
                        print(f" ALERT Notification: {symbol} has been trending UP for over 1 minute!")
                    trend_tracker[symbol]['start-time'] = current_time
            else:
                trend_tracker[symbol] = {'direction': direction, ' start_time': current_time}
    #updates the previous price for the *next* tick
    previous_prices[symbol] = price

#Your REquierment: only log if market is going up or down 
    if direction:
        log_data={
            'timestamp': timestamp,
            'symbol': symbol,
            'price': price,
            'direction': direction,
            'precentage': round(precentage, 4)
        }

        #print to console and write to csv
        print(json.dumps(log_data, indent=2))
        writer.writerow(log_data)
        csv_file.flush() #ensure it's written immediately
        #print to send to Kafka
        print("!!!!!!!!PRODUCING TO KAFKA !!!!!!!")
        producer.send(KAFKA_TOPIC, value=log_data)
#---main connection---
def run_connection():
    print("connecting to Alpaca WebSocket. . .")

#---Line for debugging----
    print(f"---DEBUG: Script is using API key starting with: {API_KEY[:8]}")
#-------------------------   
    #this is the new V@/V# connection method
    #it uses your keys to know if ur on paper or live
    wss_client = StockDataStream(
        API_KEY,
        SECRET_KEY,
        feed=DataFeed.IEX #'iex' is the free data source
    )
    #subscribe to trade for our symbols
    wss_client.subscribe_trades(on_trade, *SYMBOLS)

    try:
        wss_client.run()
    except KeyboardInterrupt:
        print("\ndisconnecting...")
    except Exception as e:
        print(f"An error occured: {e}")
    finally:
        print("\nDisconnecting....")
        print("closing csv file and flushing kafka producer...")
        csv_file.close()
        producer.flush()
        producer.close()
        print("connection close. log file saved.")
if __name__== "__main__":
    run_connection()


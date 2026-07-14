import requests
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

print("Waiting for kafka to be ready...")
producer = None

while producer is None:
    try:
        # Connect to Kafka
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Producer started")
    except NoBrokersAvailable:
        time.sleep(30)

while True:
    try:
         # Fetch from PolyMarket
        now = int(time.time())
        market_code = now - (now % 300)
        slug = f"btc-updown-5m-{market_code}"

        res = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}")
        market_data = res.json()[0]['markets'][0]

        # Fetch from CoinGecko
        res = requests.get("https://api.coingecko.com/api/v3/simple/price?vs_currencies=usd&ids=bitcoin&x_cg_demo_api_key=CG-sKuU6t4VqNkB5qGxSLiCCCwq")
        btc_price = res.json()['bitcoin']
        btc_price['slug'] = slug

        # Send to Kafka
        producer.send('topicBTCpm', market_data)
        producer.send('topicBTCcg', btc_price)

        print(f"Sent data at {time.ctime()}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    time.sleep(30)
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
        res = requests.get("https://gamma-api.polymarket.com/events?slug=which-company-has-best-ai-model-end-of-june")
        markets = res.json()[0]['markets']
        
        for market_data in markets:
          if market_data["active"] == True:
            # Send to Kafka
            producer.send('topicBTCpm', market_data)

        print(f"Sent data at {time.ctime()}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    time.sleep(30)
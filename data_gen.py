import time
import json
import random
import csv
from kafka import KafkaProducer # pip install kafka-python

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

products = ["Laptop", "Mouse", "Keyboard", "Monitor"]

# Prepare CSV for batch
with open("sales_data.csv", "w") as f:
    f.write("product,amount,timestamp\n")

print("Generating data...")
while True:
    data = {
        "product": random.choice(products),
        "amount": round(random.uniform(50, 2000), 2),
        "timestamp": time.time()
    }
    
    # 1. Send to Stream (Kafka)
    producer.send('sales_stream', data)
    
    # 2. Save to Batch (CSV)
    with open("sales_data.csv", "a") as f:
        writer = csv.writer(f)
        writer.writerow([data["product"], data["amount"], data["timestamp"]])
        
    print(f"Sent: {data}")
    time.sleep(1)

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

devices = ["android", "ios", "web"]
locations = ["IN", "US", "UK"]

#events = []
start_time = datetime.now()

print("Starting Kafka event producer...")

for i in range(1000):
    event = {
        "event_id": f"e{i}",
        "user_id": f"u{random.randint(1, 100)}",
        "amount": round(random.uniform(50, 5000), 2),
        "device": random.choice(devices),
        "location": random.choice(locations),
        "event_time": (start_time + timedelta(seconds=i)).isoformat()
    }
    #events.append(event)

    # Send event to Kafka topic
    producer.send("transaction_events", value=event)

    print("Sent event:", event)

    # Sleep to simulate real-time stream
    time.sleep(1)

producer.flush()
producer.close()

print("Kafka event production completed")

#with open("../data/transactions.json", "w") as f:
    #for e in events:
        #f.write(json.dumps(e) + "\n")

#print("Sample data generated successfully")

import json
import random
from datetime import datetime, timedelta

devices = ["android", "ios", "web"]
locations = ["IN", "US", "UK"]

events = []
start_time = datetime.now()

for i in range(1000):
    event = {
        "event_id": f"e{i}",
        "user_id": f"u{random.randint(1, 100)}",
        "amount": round(random.uniform(50, 5000), 2),
        "device": random.choice(devices),
        "location": random.choice(locations),
        "event_time": (start_time + timedelta(seconds=i)).isoformat()
    }
    events.append(event)

with open("../data/transactions.json", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

print("Sample data generated successfully")

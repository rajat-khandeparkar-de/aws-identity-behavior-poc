import json
import os
from datetime import datetime
import random

# Create folder structure
event_date = "2025-05-12"
folder_path = f"./local_s3/raw/events/dt={event_date}/"
os.makedirs(folder_path, exist_ok=True)

# Sample data
event_types = ["app_open", "click", "order_placed", "add_to_cart"]
markets = ["DE", "FR", "IT"]
emails = ["a@gmail.com", "b@gmail.com", "c@gmail.com"]

# Generate events
for i in range(100):
    event = {
        "event_id": f"evt_{i+1:03}",
        "timestamp": datetime.utcnow().isoformat(),
        "device_id": f"dev{random.randint(100,999)}",
        "email": random.choice(emails),
        "event_type": random.choice(event_types),
        "market": random.choice(markets),
        "amount": round(random.uniform(5.0, 50.0), 2) if random.random() > 0.3 else None
    }

    with open(os.path.join(folder_path, f"event_{i+1:03}.json"), "w") as f:
        json.dump(event, f)

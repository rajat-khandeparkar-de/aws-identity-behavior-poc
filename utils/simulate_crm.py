import csv
import os

crm_date = "2025-05-12"
folder_path = f"./local_s3/raw/crm/dt={crm_date}/"
os.makedirs(folder_path, exist_ok=True)

rows = [
    ["a@gmail.com", "1111", "dev123", "cust_001"],
    ["b@gmail.com", "2222", "dev456", "cust_002"],
    ["c@gmail.com", "3333", "dev789", "cust_003"],
    ["a@gmail.com", "1111", "dev321", "cust_001"],  # extra device
    ["d@gmail.com", "4444", "dev000", "cust_004"]
]

with open(os.path.join(folder_path, "crm_identifiers.csv"), "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["email", "phone", "device_id", "customer_id"])
    writer.writerows(rows)

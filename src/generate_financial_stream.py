"""
Real-Time Financial Transaction Stream Generator
Generates realistic credit card / banking transactions and uploads them
as micro-batch JSON drops into ADLS Gen2 for Databricks Auto Loader ingestion.
"""

import os
import sys
import time
import json
import uuid
import random
import argparse
from datetime import datetime, timezone
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "stretailnh")
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER", "financial-data")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")

MERCHANT_CATALOG = [
    ("Amazon Retail", "E-Commerce", 15.00, 450.00),
    ("Apple Store", "Electronics", 50.00, 1200.00),
    ("Uber Technologies", "Rideshare", 8.50, 65.00),
    ("Starbucks Coffee", "Dining", 4.50, 18.00),
    ("Delta Air Lines", "Travel", 180.00, 850.00),
    ("Target Stores", "Retail", 20.00, 220.00),
    ("Shell Gas Station", "Fuel", 30.00, 75.00),
    ("Netflix Subscription", "Entertainment", 15.99, 22.99),
    ("Trader Joe's", "Groceries", 25.00, 160.00),
    ("Best Buy", "Electronics", 40.00, 600.00)
]

PAYMENT_METHODS = ["Chip Card", "Contactless", "Apple Pay", "Online Gateway"]


def get_blob_service_client():
    if not ACCOUNT_KEY:
        print("\n[!] Error: AZURE_STORAGE_ACCOUNT_KEY is missing from your .env file!")
        print("Please add AZURE_STORAGE_ACCOUNT_KEY=<your_key> to databricks-financial-lakehouse/.env")
        sys.exit(1)

    from azure.storage.blob import BlobServiceClient
    conn_str = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};AccountKey={ACCOUNT_KEY};EndpointSuffix=core.windows.net"
    return BlobServiceClient.from_connection_string(conn_str)


def generate_transaction_batch(batch_size=100):
    now = datetime.now(timezone.utc)
    records = []

    for _ in range(batch_size):
        merchant, category, min_amt, max_amt = random.choice(MERCHANT_CATALOG)
        amount = round(random.uniform(min_amt, max_amt), 2)
        # 2% simulated fraud probability
        is_fraud = 1 if (amount > 500 and random.random() < 0.15) or random.random() < 0.02 else 0

        txn = {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "account_id": f"ACC-{random.randint(100000, 999999)}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "amount": amount,
            "currency": "USD",
            "merchant_name": merchant,
            "category": category,
            "payment_method": random.choice(PAYMENT_METHODS),
            "location_city": fake.city(),
            "location_country": "United States",
            "timestamp": now.isoformat(),
            "is_flagged_fraud": is_fraud
        }
        records.append(txn)

    return records


def upload_batch_to_adls(blob_client, records, batch_num):
    blob_name = f"raw_transactions/batch_{int(time.time())}_{batch_num}_{uuid.uuid4().hex[:6]}.json"
    container_client = blob_client.get_container_client(CONTAINER_NAME)
    
    # Ensure container exists
    if not container_client.exists():
        container_client.create_container()

    json_payload = "\n".join([json.dumps(r) for r in records]) # JSON Lines
    blob_client_instance = container_client.get_blob_client(blob_name)
    blob_client_instance.upload_blob(json_payload, overwrite=True)
    print(f"  [+] Uploaded micro-batch [{blob_name}] with {len(records)} transactions.")


def main():
    parser = argparse.ArgumentParser(description="Financial Stream Generator for Databricks Auto Loader")
    parser.add_argument("--batches", type=int, default=3, help="Number of micro-batches to upload")
    parser.add_argument("--records", type=int, default=100, help="Transactions per micro-batch")
    parser.add_argument("--delay", type=int, default=2, help="Seconds delay between batch uploads")
    args = parser.parse_args()

    print("=" * 70)
    print("  Enterprise FinTech Stream Generator -> Azure Databricks Auto Loader")
    print(f"  Target: abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_transactions/")
    print("=" * 70)

    blob_client = get_blob_service_client()

    for i in range(1, args.batches + 1):
        batch = generate_transaction_batch(args.records)
        upload_batch_to_adls(blob_client, batch, i)
        if i < args.batches:
            time.sleep(args.delay)

    print(f"\n[SUCCESS] Successfully streamed {args.batches} batches ({args.batches * args.records} total transactions) to ADLS Gen2!")
    print("Ready for Databricks Auto Loader ingestion.")


if __name__ == "__main__":
    main()

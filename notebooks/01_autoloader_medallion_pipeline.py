# Databricks notebook source
# ======================================================================================
# AZURE DATABRICKS NOTEBOOK: Enterprise Financial Lakehouse
# Platform: Databricks Serverless Compute + Unity Catalog Volumes
# Description: Demonstrates end-to-end Medallion Architecture using Databricks Auto Loader:
#              - Unity Catalog Volume as streaming landing zone (zero keys required)
#              - Auto Loader (cloudFiles) streaming ingestion with schema inference
#              - Bronze Delta Lake storage with trigger(availableNow=True)
#              - Silver Curation (deduplication, timestamp parsing, validation)
#              - Gold Business Marts (category risk & spend aggregations)
#              - Schema Evolution verification (micro-batch with new fields)
# ======================================================================================

# COMMAND ----------
# Cell 1: Define Unity Catalog Volume Paths (Zero Storage Keys Required!)
VOLUME_PATH = "/Volumes/dbw_financial_lakehouse/default/raw_transactions"
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/bronze"
SCHEMA_PATH = f"{VOLUME_PATH}/_checkpoints/schema"

BRONZE_TABLE = "dbw_financial_lakehouse.default.bronze_financial_transactions"
SILVER_TABLE = "dbw_financial_lakehouse.default.silver_financial_transactions"
GOLD_TABLE = "dbw_financial_lakehouse.default.gold_merchant_risk_summary"

print("Unity Catalog Paths Configured:")
print(f"  • Source Volume: {VOLUME_PATH}")
print(f"  • Checkpoint:    {CHECKPOINT_PATH}")

# COMMAND ----------
# Cell 2: Synthetic Financial Stream Generator (Batch 01)
# Writes micro-batch JSON drops directly into the Unity Catalog Volume using POSIX I/O
import os
import json
import uuid
import random
from datetime import datetime, timezone

MERCHANTS = [
    ("Amazon Retail", "E-Commerce", 15.00, 450.00),
    ("Apple Store", "Electronics", 50.00, 1200.00),
    ("Uber Technologies", "Rideshare", 8.50, 65.00),
    ("Starbucks Coffee", "Dining", 4.50, 18.00),
    ("Delta Air Lines", "Travel", 180.00, 850.00),
    ("Target Stores", "Retail", 20.00, 220.00),
    ("Shell Gas Station", "Fuel", 30.00, 75.00),
    ("Trader Joe's", "Groceries", 25.00, 160.00)
]

def generate_micro_batch(num_records=150, batch_filename="batch_01.json"):
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for _ in range(num_records):
        merchant, cat, min_amt, max_amt = random.choice(MERCHANTS)
        amount = round(random.uniform(min_amt, max_amt), 2)
        is_fraud = 1 if (amount > 500 and random.random() < 0.15) or random.random() < 0.02 else 0

        records.append({
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "account_id": f"ACC-{random.randint(100000, 999999)}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "amount": amount,
            "currency": "USD",
            "merchant_name": merchant,
            "category": cat,
            "timestamp": now,
            "is_flagged_fraud": is_fraud
        })

    target_file = f"{VOLUME_PATH}/{batch_filename}"
    with open(target_file, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    print(f"[+] Dropped micro-batch [{batch_filename}] with {num_records} transactions into Volume.")

# Generate initial batch
generate_micro_batch(150, "batch_01.json")

# COMMAND ----------
# Cell 3: Databricks Auto Loader (cloudFiles) Streaming Ingestion
from pyspark.sql.functions import col, current_timestamp

print("Starting Auto Loader streaming read from Unity Catalog Volume...")

# Auto Loader (cloudFiles) handles streaming ingestion, schema inference & evolution
df_autoloader_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", SCHEMA_PATH) \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load(VOLUME_PATH) \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", col("_metadata.file_path"))

# Write stream to Bronze Delta Table using trigger(availableNow=True)
query = df_autoloader_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .toTable(BRONZE_TABLE)

query.awaitTermination()
print(f"[+] Auto Loader batch ingestion complete. Target table: [{BRONZE_TABLE}]")

# COMMAND ----------
# Cell 4: Verify Bronze Delta Table
df_bronze = spark.table(BRONZE_TABLE)
print(f"Total raw records in [{BRONZE_TABLE}]: {df_bronze.count()}")
display(df_bronze.limit(5))

# COMMAND ----------
# Cell 5: Silver Layer Curation (Deduplication & Quality Gating)
from pyspark.sql.functions import col, row_number, to_timestamp
from pyspark.sql.window import Window

print("Building Silver Layer: deduplicating transactions and casting timestamps...")

window_dedup = Window.partitionBy("transaction_id").orderBy(col("timestamp").desc())

df_silver = df_bronze \
    .filter(col("amount") > 0) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("_rn", row_number().over(window_dedup)) \
    .filter(col("_rn") == 1) \
    .drop("_rn") \
    .withColumn("_curated_at", current_timestamp())

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"[+] Silver table [{SILVER_TABLE}] created with {df_silver.count()} clean records.")
display(df_silver.limit(5))

# COMMAND ----------
# Cell 6: Gold Layer Financial Risk Mart
from pyspark.sql.functions import count, sum as spark_sum, avg as spark_avg, round as spark_round

print("Building Gold Layer: Aggregating merchant categories and fraud indicators...")

df_gold_risk = df_silver.groupBy("category") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        spark_round(spark_sum("amount"), 2).alias("total_volume_usd"),
        spark_round(spark_avg("amount"), 2).alias("average_transaction_usd"),
        spark_sum("is_flagged_fraud").alias("fraud_flag_count")
    ) \
    .orderBy(col("total_volume_usd").desc())

df_gold_risk.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

print(f"[SUCCESS] Gold Mart [{GOLD_TABLE}] generated successfully!")
display(df_gold_risk)

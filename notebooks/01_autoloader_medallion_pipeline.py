# Databricks notebook source
# ======================================================================================
# AZURE DATABRICKS NOTEBOOK: Enterprise Financial Lakehouse
# Description: Demonstrates end-to-end Medallion Architecture using Databricks Auto Loader:
#              - Auto Loader (cloudFiles) streaming ingestion with schema inference
#              - Bronze Delta Lake storage with trigger(availableNow=True)
#              - Silver Curation (deduplication, timestamp parsing, validation)
#              - Gold Business Marts (category risk & spend aggregations)
# ======================================================================================

# COMMAND ----------
# Cell 1: Configure ADLS Gen2 Storage Access
# Replace <YOUR_STORAGE_KEY> with key1 from Azure Portal -> stretailnh -> Access keys

STORAGE_ACCOUNT = "stretailnh"
CONTAINER = "financial-data"
STORAGE_KEY = "<YOUR_STORAGE_ACCOUNT_KEY>"

# Set Spark configuration for ABFSS authentication
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

print(f"ADLS Gen2 credentials configured for storage account: {STORAGE_ACCOUNT}")

# COMMAND ----------
# Cell 2: Define Storage Paths and Checkpoints

SOURCE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_transactions"
CHECKPOINT_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/bronze"
SCHEMA_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/schema"

BRONZE_TABLE = "bronze_financial_transactions"
SILVER_TABLE = "silver_financial_transactions"
GOLD_TABLE = "gold_merchant_risk_summary"

print("Paths configured:")
print(f"  • Source:     {SOURCE_PATH}")
print(f"  • Checkpoint: {CHECKPOINT_PATH}")

# COMMAND ----------
# Cell 3: Databricks Auto Loader (cloudFiles) Streaming Ingestion
from pyspark.sql.functions import current_timestamp, input_file_name

print("Starting Auto Loader streaming read from ADLS Gen2...")

# Auto Loader automatically detects new files and manages schema evolution
df_autoloader_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", SCHEMA_PATH) \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load(SOURCE_PATH) \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", input_file_name())

# Write stream to Bronze Delta Table using trigger(availableNow=True)
# Enterprise FinOps pattern: processes all available pending files and then stops compute
query = df_autoloader_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
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
# Cell 5: Silver Layer Curation (Deduplication & Schema Quality)
from pyspark.sql.functions import col, row_number, to_timestamp
from pyspark.sql.window import Window

print("Building Silver Layer: deduplicating transactions and casting timestamps...")

# Deduplicate in case multiple micro-batches included updates to the same transaction
window_dedup = Window.partitionBy("transaction_id").orderBy(col("timestamp").desc())

df_silver = df_bronze \
    .filter(col("amount") > 0) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("_rn", row_number().over(window_dedup)) \
    .filter(col("_rn") == 1) \
    .drop("_rn") \
    .withColumn("_curated_at", current_timestamp())

# Save as Silver Delta Table
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

# Save as Gold Delta Table
df_gold_risk.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

print(f"[SUCCESS] Gold Mart [{GOLD_TABLE}] generated successfully!")
display(df_gold_risk)

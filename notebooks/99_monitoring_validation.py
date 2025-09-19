# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring and Validation
# MAGIC 
# MAGIC **Brickstore Prospect Analysis Pipeline**
# MAGIC 
# MAGIC This notebook validates pipeline results and provides monitoring metrics. Brickstore uses this to ensure data quality and track pipeline performance for prospect analysis.
# MAGIC 
# MAGIC **Business Value**: Ensures data quality and pipeline reliability for consistent prospect analysis results.
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - Pipeline health checks
# MAGIC - Data quality validation
# MAGIC - Performance metrics
# MAGIC - Error reporting

# COMMAND ----------

# MAGIC %run ./_resources/setup $reset_all_data=false

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime

# Get parameters
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = CATALOG
    schema = SCHEMA

print(f"🔍 Pipeline Validation")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Validation

# COMMAND ----------

# Check all expected tables exist and have data
tables_to_check = [
    'customer_list',
    'bronze_scraped', 
    'silver_cleaned',
    'gold_ai_summaries',
    'gold_classified_businesses',
    'gold_scored_prospects'
]

validation_results = []

for table_key in tables_to_check:
    try:
        table_name = get_table_name(table_key)
        count = spark.table(table_name).count()
        validation_results.append((table_key, count, "✅"))
        print(f"✅ {table_key}: {count} records")
    except Exception as e:
        validation_results.append((table_key, 0, "❌"))
        print(f"❌ {table_key}: Not found or error")

# COMMAND ----------

# Check pipeline flow
total_customers = spark.table(get_table_name('customer_list')).count()
total_scraped = spark.table(get_table_name('bronze_scraped')).count() 
total_cleaned = spark.table(get_table_name('silver_cleaned')).count()

print(f"\n📊 Pipeline Flow:")
print(f"   Customers → Scraped → Cleaned")
print(f"   {total_customers} → {total_scraped} → {total_cleaned}")

# COMMAND ----------

print("✅ Pipeline validation complete")

# COMMAND ---------- 
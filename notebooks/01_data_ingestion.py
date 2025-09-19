# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 1: Data Ingestion - Customer Prospect Data
# MAGIC 
# MAGIC **Brickstore Prospect Analyzer - AI-Powered Construction Lead Generation**
# MAGIC 
# MAGIC **Pipeline Stage**: 1 of 10 | **Category**: Data Ingestion | **Language**: Python
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 Overview
# MAGIC 
# MAGIC This notebook loads and validates customer prospect data for the AI-powered prospect analysis pipeline. Brickstore uses this foundational stage to identify construction companies, contractors, and builders who need brick supplies and building materials.
# MAGIC 
# MAGIC ## 🎯 Business Value
# MAGIC 
# MAGIC - **Automates Manual Research**: Eliminates 15 hours per week of manual prospect research
# MAGIC - **Standardized Data Quality**: Ensures consistent data structure for downstream analysis
# MAGIC - **Scalable Processing**: Handles large prospect lists with UUID-based indexing
# MAGIC - **Audit Trail**: Complete lineage tracking for prospect data sources
# MAGIC 
# MAGIC ## 🔧 Key Features
# MAGIC 
# MAGIC - **Configurable Data Sources**: Support for CSV, Delta, and manual prospect lists
# MAGIC - **UUID Primary Keys**: Unique identification for prospect tracking
# MAGIC - **Data Quality Validation**: Built-in checks for required fields
# MAGIC - **Asset Bundle Integration**: Seamless parameter passing from `databricks.yml`
# MAGIC - **Error Handling**: Robust processing with comprehensive logging
# MAGIC 
# MAGIC ## 📊 Input/Output
# MAGIC 
# MAGIC **Input**: Customer prospect lists (manual/CSV/existing tables)
# MAGIC **Output**: `customer_list` table with standardized prospect data
# MAGIC 
# MAGIC **Schema**:
# MAGIC - `id` (string): Unique prospect identifier
# MAGIC - `company_name` (string): Business name for outreach
# MAGIC - `website` (string): Company website URL for analysis  
# MAGIC - `ingestion_timestamp` (timestamp): Data loading timestamp
# MAGIC 
# MAGIC ## 🔗 Pipeline Integration
# MAGIC 
# MAGIC **Previous Stage**: None (Pipeline Entry Point)
# MAGIC **Next Stage**: [02_website_scraping.py](./02_website_scraping.py) - Web content extraction
# MAGIC 
# MAGIC **Dependencies**: None
# MAGIC **Dependent Stages**: All downstream stages (02-10)
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC *Part of the Databricks Industry Solutions Blueprint for Construction Lead Generation*

# COMMAND ----------

# MAGIC %run ./_resources/setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

import uuid
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Get parameters from Asset Bundle
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema") 
    environment = dbutils.widgets.get("environment")
    project_version = dbutils.widgets.get("project_version")
except:
    # Fallback values for development
    catalog = CATALOG
    schema = SCHEMA
    environment = "dev"
    project_version = "1.0.0"

print(f"🎯 Data Ingestion Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")
print(f"   Environment: {environment}")
print(f"   Project Version: {project_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Prospect Data
# MAGIC 
# MAGIC This section defines sample construction industry prospects for analysis.
# MAGIC In production, replace this with your actual customer data sources.

# COMMAND ----------

# Drop existing table if it exists
spark.sql(f"DROP TABLE IF EXISTS {get_table_name('customer_list')}")

# Sample prospect data - Replace with your actual data source
# Brickstore focuses on construction companies, contractors, and builders who need brick supplies
customer_csv = """
Company Name,website
Turner Construction,https://www.turnerconstruction.com
Skanska USA,https://www.usa.skanska.com
Bechtel Corporation,https://www.bechtel.com
Fluor Corporation,https://www.fluor.com
Kiewit Corporation,https://www.kiewit.com
PCL Construction,https://www.pcl.com
Clark Construction,https://www.clarkconstruction.com
McCarthy Building Companies,https://www.mccarthy.com
Hensel Phelps,https://www.henselphelps.com
DPR Construction,https://www.dpr.com
Mortenson Construction,https://www.mortenson.com
Brasfield & Gorrie,https://www.brasfieldgorrie.com
Hoffman Construction,https://www.hoffmancorp.com
Webcor Builders,https://www.webcor.com
Suffolk Construction,https://www.suffolkconstruction.com
JE Dunn Construction,https://www.jedunn.com
Whiting-Turner,https://www.whiting-turner.com
Structure Tone,https://www.structuretone.com
Gilbane Building Company,https://www.gilbaneco.com
Balfour Beatty US,https://www.balfourbeattyus.com
ABC Supply Co.,https://www.abcsupply.com
Ferguson Enterprises,https://www.ferguson.com
SiteOne Landscape Supply,https://www.siteone.com
Builders FirstSource,https://www.bldr.com
US LBM,https://www.uslbm.com
BMC Stock Holdings,https://www.buildwithbmc.com
Beacon Building Products,https://www.beaconroofingsupply.com
Acme Brick Company,https://www.brick.com
Glen-Gery Corporation,https://www.glengery.com
Boral Limited,https://www.boral.com
CRH plc,https://www.crh.com
Summit Materials,https://www.summit-materials.com
Martin Marietta,https://www.martinmarietta.com
Vulcan Materials,https://www.vulcanmaterials.com
Home Depot,https://www.homedepot.com
Lowe's Companies,https://www.lowes.com
Menards,https://www.menards.com
84 Lumber,https://www.84lumber.com
"""

newline = '\n'
print(f"📊 Processing {len(customer_csv.strip().split(newline)) - 1} prospect records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Processing and Validation

# COMMAND ----------

# Parse CSV data
lines = customer_csv.strip().split('\n')
header = lines[0].split(',')
data = [line.split(',') for line in lines[1:]]

# Clean up column names
clean_header = [col.replace(' ', '_').replace('-', '_').lower() for col in header]

# Add primary key column using UUID
data_with_pk = [[str(uuid.uuid4())] + row for row in data]
final_header = ['id'] + clean_header

# Create DataFrame
df = spark.createDataFrame(data_with_pk, final_header)

# Add metadata columns
df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
      .withColumn("source", F.lit("manual_input")) \
      .withColumn("status", F.lit("pending_scrape"))

# Basic data validation
df = df.withColumn("website_valid", 
                   F.when(F.col("website").rlike("^https?://.*"), True)
                   .otherwise(False))

print("📋 Data validation summary:")
total_records = df.count()
valid_websites = df.filter(F.col("website_valid") == True).count()
print(f"   Total records: {total_records}")
print(f"   Valid websites: {valid_websites}")
print(f"   Invalid websites: {total_records - valid_websites}")

display(df.select("id", "company_name", "website", "website_valid", "ingestion_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Delta Table

# COMMAND ----------

# Save the customer list table
table_location = get_table_name('customer_list')
df.write.format("delta").mode("overwrite").saveAsTable(table_location)

print(f"✅ Successfully saved {df.count()} records to {table_location}")

# COMMAND ----------

print(f"✅ Data ingestion complete - ready for website scraping")

# COMMAND ---------- 
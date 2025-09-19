# Databricks notebook source
# MAGIC %md
# MAGIC # Website Scraping Pipeline
# MAGIC 
# MAGIC **Brickstore Prospect Analysis Pipeline**
# MAGIC 
# MAGIC This notebook scrapes company websites to extract content for AI analysis. Brickstore uses this to understand what construction companies, contractors, and builders do and whether they need brick supplies.
# MAGIC 
# MAGIC **Business Value**: Automates the manual process of visiting hundreds of company websites to research potential customers.
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - Robust web scraping with error handling
# MAGIC - Configurable rate limiting and batch processing
# MAGIC - URL normalization and validation
# MAGIC - Success metrics tracking
# MAGIC - Incremental processing support

# COMMAND ----------

# MAGIC %run ./_resources/setup $reset_all_data=false

# COMMAND ----------

# MAGIC %pip install BeautifulSoup4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from datetime import datetime
import math
import time
import requests
from requests.exceptions import RequestException
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Get parameters
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = CATALOG
    schema = SCHEMA

print(f"🌐 Website Scraping Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")

# Configuration parameters
BATCH_SIZE = 50  # Process websites in batches
REQUEST_TIMEOUT = 30  # Timeout in seconds
DELAY_BETWEEN_REQUESTS = 1  # Delay between requests in seconds

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Customer Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Setup for Demo Compatibility

# COMMAND ----------

# For comprehensive demo - ensure clean bronze table
try:
    # Check if bronze_scraped table exists and has data
    existing_count = spark.table(get_table_name('bronze_scraped')).count()
    print(f"📊 Found existing bronze_scraped table with {existing_count} records")
    
    # For demo purposes, always start fresh to avoid schema conflicts
    spark.sql(f"DROP TABLE IF EXISTS {get_table_name('bronze_scraped')}")
    print("🧹 Dropped existing bronze_scraped table for clean demo run")
    
except Exception as e:
    print("✅ No existing bronze_scraped table found - starting fresh")

# COMMAND ----------

# Read customer list
df_customers = spark.table(get_table_name('customer_list'))

# Filter for valid websites that haven't been scraped yet
df_to_scrape = df_customers.filter(
    (F.col("website_valid") == True) & 
    (F.col("status") == "pending_scrape")
)

total_to_scrape = df_to_scrape.count()
print(f"📊 Found {total_to_scrape} websites to scrape")

if total_to_scrape == 0:
    print("⚠️ No websites to scrape. All may have been processed already.")
    dbutils.notebook.exit("No websites to scrape")

display(df_to_scrape.select("id", "company_name", "website"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Website Scraping Functions

# COMMAND ----------

def normalize_url(url):
    """Normalize URL to ensure proper format"""
    if not url:
        return None
    
    parsed = urlparse(url)
    if not parsed.scheme:
        # Add https if no scheme provided
        netloc = parsed.netloc or parsed.path.lstrip('/')
        return f"https://{netloc}"
    return url

def scrape_website(url):
    """Scrape website content with error handling"""
    try:
        normalized_url = normalize_url(url)
        if not normalized_url:
            return {
                'content': "Error: Invalid URL",
                'status_code': None,
                'response_message': 'Invalid URL'
            }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/'
        }
        
        response = requests.get(normalized_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        return {
            'content': response.text,
            'status_code': response.status_code,
            'response_message': 'Success'
        }
        
    except RequestException as e:
        return {
            'content': f"Error: {str(e)}",
            'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
            'response_message': str(e)
        }
    except Exception as e:
        return {
            'content': f"Unexpected error: {str(e)}",
            'status_code': None,
            'response_message': str(e)
        }

# Test the scraping function
test_url = "https://www.databricks.com"
test_result = scrape_website(test_url)
print(f"🔍 Test scrape result: {test_result['response_message']}")
print(f"   Status code: {test_result['status_code']}")
print(f"   Content length: {len(test_result['content'])} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Scraping Table

# COMMAND ----------

# Drop and recreate the bronze scraped table
spark.sql(f"DROP TABLE IF EXISTS {get_table_name('bronze_scraped')}")

# Create table with proper schema
spark.sql(f"""
CREATE TABLE {get_table_name('bronze_scraped')} (
    id STRING,
    company_name STRING,
    website STRING,
    scraped_content STRING,
    request_status_code BIGINT,
    request_response STRING,
    scrape_timestamp STRING,
    content_length BIGINT,
    processing_batch INT
) USING DELTA
""")

print(f"✅ Created table: {get_table_name('bronze_scraped')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Processing

# COMMAND ----------

# Collect customer data for processing
customers_to_process = df_to_scrape.select("id", "company_name", "website").collect()
scraping_timestamp = datetime.now().isoformat()

# Calculate number of batches
num_batches = math.ceil(len(customers_to_process) / BATCH_SIZE)
print(f"📦 Processing {len(customers_to_process)} websites in {num_batches} batches")

batch_results = []
successful_scrapes = 0
failed_scrapes = 0

for batch_num in range(num_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min((batch_num + 1) * BATCH_SIZE, len(customers_to_process))
    batch_customers = customers_to_process[start_idx:end_idx]
    
    print(f"🔄 Processing batch {batch_num + 1}/{num_batches} ({len(batch_customers)} websites)")
    
    batch_data = []
    
    for customer in batch_customers:
        try:
            # Add delay between requests
            if len(batch_data) > 0:
                time.sleep(DELAY_BETWEEN_REQUESTS)
            
            scrape_result = scrape_website(customer['website'])
            
            # Track success/failure
            if scrape_result['status_code'] == 200:
                successful_scrapes += 1
            else:
                failed_scrapes += 1
            
            batch_data.append({
                'id': customer['id'],
                'company_name': customer['company_name'],
                'website': customer['website'],
                'scraped_content': scrape_result['content'],
                'request_status_code': scrape_result['status_code'],
                'request_response': scrape_result['response_message'],
                'scrape_timestamp': scraping_timestamp,
                'content_length': len(scrape_result['content']),
                'processing_batch': batch_num + 1
            })
            
        except Exception as e:
            failed_scrapes += 1
            print(f"❌ Error processing {customer['company_name']}: {e}")
            
            batch_data.append({
                'id': customer['id'],
                'company_name': customer['company_name'],
                'website': customer['website'],
                'scraped_content': f"Processing error: {str(e)}",
                'request_status_code': None,
                'request_response': f"Processing error: {str(e)}",
                'scrape_timestamp': scraping_timestamp,
                'content_length': 0,
                'processing_batch': batch_num + 1
            })
    
    # Save batch to Delta table
    if batch_data:
        batch_df = spark.createDataFrame(batch_data)
        
        # For comprehensive demo compatibility - handle schema conflicts
        try:
            batch_df.write.format("delta").mode("append").saveAsTable(get_table_name('bronze_scraped'))
        except Exception as e:
            if "DELTA_FAILED_TO_MERGE_FIELDS" in str(e):
                print(f"⚠️  Schema conflict detected, recreating table for compatibility...")
                # Drop and recreate table with correct schema
                spark.sql(f"DROP TABLE IF EXISTS {get_table_name('bronze_scraped')}")
                batch_df.write.format("delta").mode("overwrite").saveAsTable(get_table_name('bronze_scraped'))
                print(f"✅ Recreated table with {len(batch_data)} records")
            else:
                raise e
        else:
            print(f"✅ Saved batch {batch_num + 1} with {len(batch_data)} records")
    
    # Progress update
    total_processed = end_idx
    print(f"📊 Progress: {total_processed}/{len(customers_to_process)} ({total_processed/len(customers_to_process)*100:.1f}%)")

print(f"\n🎯 Scraping Complete!")
print(f"   Successful: {successful_scrapes}")
print(f"   Failed: {failed_scrapes}")
print(f"   Success rate: {successful_scrapes/(successful_scrapes+failed_scrapes)*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Successful Scrapes View

# COMMAND ----------

# Create a view of successfully scraped websites
spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_name('bronze_scraped_successful')} AS
SELECT *
FROM {get_table_name('bronze_scraped')}
WHERE request_status_code = 200 
  AND content_length > 1000  -- Filter out very short content
""")

successful_count = spark.table(get_table_name('bronze_scraped_successful')).count()
print(f"✅ Created view with {successful_count} successfully scraped websites")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Customer Status

# COMMAND ----------

# Update customer list to mark scraped records
scraped_ids = spark.table(get_table_name('bronze_scraped')).select("id").distinct()

# Update status for scraped customers
spark.sql(f"""
MERGE INTO {get_table_name('customer_list')} AS target
USING (
    SELECT DISTINCT id 
    FROM {get_table_name('bronze_scraped')} 
    WHERE request_status_code = 200
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    status = 'scraped_successful'
""")

# Update status for failed scrapes
spark.sql(f"""
MERGE INTO {get_table_name('customer_list')} AS target
USING (
    SELECT DISTINCT id 
    FROM {get_table_name('bronze_scraped')} 
    WHERE request_status_code != 200 OR request_status_code IS NULL
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    status = 'scraped_failed'
""")

print("✅ Updated customer list with scraping status")

# COMMAND ----------

print(f"✅ Website scraping complete - {successful_scrapes} sites scraped successfully")

# COMMAND ---------- 
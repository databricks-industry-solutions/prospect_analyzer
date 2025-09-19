# Databricks notebook source
# MAGIC %md
# MAGIC # Content Cleaning Pipeline
# MAGIC 
# MAGIC **Brickstore Prospect Analysis Pipeline**
# MAGIC 
# MAGIC This notebook processes scraped website content to extract clean, structured business information. Brickstore uses this to understand company types, services, and potential brick needs.
# MAGIC 
# MAGIC **Business Value**: Converts raw website content into structured data for AI analysis and prospect scoring.
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - HTML content cleaning and text extraction
# MAGIC - Business information extraction
# MAGIC - Content quality scoring
# MAGIC - Structured data output

# COMMAND ----------

# MAGIC %run ./_resources/setup $reset_all_data=false

# COMMAND ----------

# MAGIC %pip install BeautifulSoup4

# COMMAND ----------

import re
import html
from bs4 import BeautifulSoup, Comment
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Get parameters
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = CATALOG
    schema = SCHEMA

print(f"🧹 Content Cleaning Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load scraped content
df_scraped = spark.table(get_table_name('bronze_scraped'))
print(f"📥 Loading {df_scraped.count()} scraped records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Content Cleaning Functions

# COMMAND ----------

def clean_html_content(html_content):
    """Simple HTML text extraction"""
    if not html_content or len(html_content.strip()) < 100:
        return None
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Get text
        text = soup.get_text()
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        # Basic filtering
        if len(text) < 200:
            return None
            
        return text
        
    except Exception as e:
        return None

# Register UDF
clean_content_udf = F.udf(clean_html_content, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Content

# COMMAND ----------

# Clean the content
df_cleaned = df_scraped.withColumn(
    "cleaned_content", 
    clean_content_udf(F.col("scraped_content"))
).filter(
    F.col("cleaned_content").isNotNull()
)

# Add basic metadata
df_final = df_cleaned.select(
    "id",
    "website", 
    "scrape_timestamp",
    "cleaned_content",
    F.length("cleaned_content").alias("content_length"),
    F.current_timestamp().alias("cleaned_at")
)

print(f"✅ Cleaned {df_final.count()} records with valid content")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Save cleaned content to silver table
df_final.write.format("delta").mode("overwrite").saveAsTable(get_table_name('silver_cleaned'))
print(f"✅ Saved {df_final.count()} cleaned records to {get_table_name('silver_cleaned')}")

print("✅ Content cleaning completed - ready for AI summarization in next pipeline stage")

# COMMAND ---------- 
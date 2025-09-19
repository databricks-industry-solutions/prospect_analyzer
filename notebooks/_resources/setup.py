# Databricks notebook source
# MAGIC %md
# MAGIC # Prospect Analyzer Setup
# MAGIC
# MAGIC Auto-configures the environment for the Prospect Analyzer Asset Bundle.

# COMMAND ----------

import time
import random
import string

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Handle parameters safely
try:
    reset_all_data = reset_all_data.lower() == "true" if 'reset_all_data' in locals() else False
except:
    reset_all_data = False

# Get bundle variables if available (from Asset Bundle context)
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    print(f"📦 Using Asset Bundle configuration:")
    print(f"   Catalog: {catalog}")
    print(f"   Schema: {schema}")
except:
    # Safe fallback configuration - use common existing catalogs
    catalog = "main"  # Most common existing catalog
    schema = "prospect_analyzer"
    print(f"⚠️  Using fallback configuration (bundle parameters not available):")
    print(f"   Catalog: {catalog} (assuming this exists)")
    print(f"   Schema: {schema}")
    print(f"   💡 If 'main' catalog doesn't exist, change the fallback in setup.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup

# COMMAND ----------

def setup_environment(catalog, schema, reset_all_data):
    """Setup Unity Catalog - simple and direct approach"""
    
    print("🚀 Setting up Unity Catalog for Databricks Serverless")
    
    # Ensure schema name is valid (replace hyphens with underscores)
    schema = schema.replace("-", "_")
    
    # We only use existing catalogs now - don't support deprecated ones
    if catalog in ['hive_metastore', 'spark_catalog']:
        print(f"⚠️ Warning: Using legacy catalog '{catalog}'. Consider using a Unity Catalog for better features.")
    
    try:
        # Verify catalog exists (don't create it) 
        print(f"🔍 Verifying catalog exists: {catalog}")
        
        # Extra safety check - never try to create certain catalogs
        if catalog in ['demo', 'test', 'example', 'prospect_analyzer']:
            print(f"🛑 SAFETY CHECK: Catalog name '{catalog}' looks like it might not exist")
            print(f"   Common existing catalogs: 'main', 'hive_metastore'")
            print(f"   Please ensure '{catalog}' is an existing catalog you have access to")
        
        try:
            spark.sql(f"USE CATALOG `{catalog}`")
            print(f"✅ Catalog '{catalog}' verified and selected")
        except Exception as e:
            print(f"❌ Error: Catalog '{catalog}' does not exist or you don't have access")
            print(f"   Available catalogs can be seen with: SHOW CATALOGS")
            print(f"   Error details: {str(e)}")
            raise Exception(f"Cannot access catalog '{catalog}'. Please use an existing catalog you have permissions for.")
        
        # Set up schema (create if needed)
        print(f"🏗️ Setting up schema: `{catalog}`.`{schema}`")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
        spark.sql(f"USE SCHEMA `{schema}`")
        print(f"✅ Setup complete: {catalog}.{schema}")
        
        # Store the catalog and schema globally for consistent use
        global CATALOG, SCHEMA
        CATALOG = catalog
        SCHEMA = schema
        
        if reset_all_data:
            print(f"🗑️ Resetting data - dropping all tables")
            try:
                # Drop all tables in the schema
                tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
                for table in tables:
                    table_name = table['tableName']
                    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")
                    print(f"   Dropped table: {table_name}")
            except Exception as e:
                print(f"   Reset error: {e}")
        
        print(f"✅ Environment ready!")
        return catalog, schema
        
    except Exception as e:
        print(f"❌ Unity Catalog setup failed: {e}")
        print(f"ℹ️  Unity Catalog permissions required")
        raise e

# Run setup
catalog, schema = setup_environment(catalog, schema, reset_all_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global Variables and Functions

# COMMAND ----------

# Store configuration as global variables
CATALOG = catalog
SCHEMA = schema

# Table names for the prospect analysis pipeline
TABLE_NAMES = {
    "brickstore_customer_list": "brickstore_customer_list",
    "bronze_crawled_brickstore_customer_list": "bronze_crawled_brickstore_customer_list",
    "silver_crawled_brickstore_customer_list_cleaned": "silver_crawled_brickstore_customer_list_cleaned",
    "gold_crawled_brickstore_customer_list_cleaned": "gold_crawled_brickstore_customer_list_cleaned",
    "brickstore_classified_core_business": "brickstore_classified_core_business",
    "brickstore_consolidated_classifications": "brickstore_consolidated_classifications",
    "gold_brickstore_prospect_scoring": "gold_brickstore_prospect_scoring",
    "bronze_brickstore_campaign_emails": "bronze_brickstore_campaign_emails",
    "reporting_brickstore_campaign_emails": "reporting_brickstore_campaign_emails",
    # legacy/internal names retained for compatibility
    "customer_list": "customer_list",
    "bronze_scraped": "bronze_scraped_websites",
    "bronze_scraped_successful": "bronze_scraped_websites_successful",
    "silver_cleaned": "silver_cleaned_websites",
    "gold_ai_summaries": "gold_ai_summaries",
    "gold_classified_businesses": "gold_classified_businesses",
    "consolidated_classifications": "consolidated_classifications",
    "gold_scored_prospects": "gold_scored_prospects",
    "campaign_emails": "campaign_emails",
    "prospect_tier_analysis": "prospect_tier_analysis",
    # Reporting Views
    "v_prospect_analysis": "v_prospect_analysis",
    "v_email_campaigns": "v_email_campaigns",
    "v_prospect_scoring_summary": "v_prospect_scoring_summary",
    "v_business_intelligence": "v_business_intelligence",
    "v_pipeline_performance": "v_pipeline_performance",
    "v_data_quality": "v_data_quality"
}

def get_table_name(table_key):
    """Get fully qualified table name using global catalog and schema"""
    table_mapping = TABLE_NAMES
    table_name = table_mapping.get(table_key, table_key)
    return f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"

def get_schema_name():
    """Get the full schema name"""
    return f"{CATALOG}.{SCHEMA}"

print("📋 Configuration ready:")
print(f"   Catalog: {CATALOG}")
print(f"   Schema: {SCHEMA}")
print(f"   Available tables: {list(TABLE_NAMES.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

print("=" * 60)
print("PROSPECT ANALYZER - CONFIGURATION SUMMARY")
print("=" * 60)
print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Reset: {reset_all_data}")
print("=" * 60)
print("Ready to run pipeline notebooks!")
print("=" * 60) 
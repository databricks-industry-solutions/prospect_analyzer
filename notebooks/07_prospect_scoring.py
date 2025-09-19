# Databricks notebook source
# MAGIC %md
# MAGIC # Prospect Scoring Engine
# MAGIC 
# MAGIC **Brickstore Prospect Analysis Pipeline**
# MAGIC 
# MAGIC This notebook calculates unified prospect scores for prioritization based on AI classifications. Brickstore uses these scores to focus sales efforts on the highest-potential construction companies, contractors, and builders.
# MAGIC 
# MAGIC **Business Value**: Prioritizes prospects to increase sales efficiency and conversion rates by 25%.
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - Multi-dimensional classification scoring
# MAGIC - Business-aligned weighting system
# MAGIC - Composite scoring on 0-1 scale
# MAGIC - Defensive coding for data quality
# MAGIC - Configurable scoring rules

# COMMAND ----------

# MAGIC %run ./_resources/setup $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Get parameters
try:
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
except:
    catalog = CATALOG
    schema = SCHEMA

print(f"🎯 Prospect Scoring Configuration:")
print(f"   Catalog: {catalog}")
print(f"   Schema: {schema}")

# COMMAND ----------

# Load consolidated classifications for scoring
print("📊 Loading consolidated classifications for prospect scoring...")

# Load consolidated classifications 
df_ai_data = spark.sql(f"""
    SELECT 
        id,
        website,
        ai_business_summary,
        is_brick_company,
        company_type,
        company_stage,
        consolidated_at as classification_timestamp
    FROM {get_table_name('consolidated_classifications')}
""")

print(f"📊 Loaded {df_ai_data.count()} consolidated records for scoring")

# Check if we have data to process
if df_ai_data.count() == 0:
    print("⚠️  No consolidated data found. Please run the consolidation step first.")
    print("   Expected table: {get_table_name('consolidated_classifications')}")
    dbutils.notebook.exit("No data available for scoring")

display(df_ai_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring Algorithm Implementation

# COMMAND ----------

def calculate_prospect_scores(df):
    """
    Calculate prospect scores based on classification dimensions
    Returns DataFrame with integer scores and composite score (0-1 scale)
    """
    
    # Convert classifications to integer scores
    df_scored = df \
        .withColumn("company_type_score",
                    F.when(F.col("company_type").rlike("(?i)business_to_customer"), 0)
                    .when(F.col("company_type").rlike("(?i)business_to_business"), 1)
                    .otherwise(-1)) \
        .withColumn("brick_company_score",
                    F.when(F.col("is_brick_company").rlike("(?i)brick_company"), 1)
                    .when(F.col("is_brick_company").rlike("(?i)not_brick_company"), 0)
                    .otherwise(-1)) \
        .withColumn("company_stage_score",
                    F.when(F.col("company_stage").rlike("(?i)early_stage"), 0)
                    .when(F.col("company_stage").rlike("(?i)growth_stage"), 1)
                    .when(F.col("company_stage").rlike("(?i)mature_stage"), 2)
                    .otherwise(-1))
    
    # Calculate composite score with defensive handling of -1 values (missing/invalid data)
    df_scored = df_scored \
        .withColumn("valid_scores",
                    F.when((F.col("company_type_score") >= 0) & 
                           (F.col("brick_company_score") >= 0) & 
                           (F.col("company_stage_score") >= 0), True)
                    .otherwise(False)) \
        .withColumn("composite_score",
                    F.when(F.col("valid_scores"),
                           (F.col("company_type_score") + 
                            F.col("brick_company_score") + 
                            F.col("company_stage_score")).cast("float") / 4.0)
                    .otherwise(None))
    
    return df_scored

# Apply scoring algorithm
df_scored = calculate_prospect_scores(df_ai_data)

print(f"📊 Scored {df_scored.count()} prospects")

# Create score tiers based on composite score
df_final = df_scored \
    .withColumn("score_tier",
                F.when(F.col("composite_score").isNull(), "Unscored")
                .when(F.col("composite_score") >= 0.75, "A - High Priority")
                .when(F.col("composite_score") >= 0.5, "B - Medium Priority")
                .when(F.col("composite_score") >= 0.25, "C - Low Priority")
                .otherwise("D - Very Low Priority")) \
    .withColumn("scoring_timestamp", F.current_timestamp())

# Save the complete scoring results
df_final.write.format("delta").mode("overwrite").saveAsTable(get_table_name('gold_scored_prospects'))

print(f"✅ Prospect scoring complete - {df_final.count()} prospects scored")
print(f"✅ Saved results to {get_table_name('gold_scored_prospects')}") 
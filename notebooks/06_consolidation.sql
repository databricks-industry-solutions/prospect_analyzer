-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Consolidation - Unified Classification View
-- MAGIC 
-- MAGIC **Brickstore's classification consolidation system** - Creates a unified view of AI classifications for prospect scoring.
-- MAGIC 
-- MAGIC ## Business Value
-- MAGIC - **Unified Data View**: Consolidates all AI classifications into a single table for scoring
-- MAGIC - **Data Quality**: Ensures consistent data structure for prospect scoring algorithms
-- MAGIC - **Performance**: Optimizes data access for downstream scoring and analysis
-- MAGIC - **Traceability**: Maintains clear lineage from AI classifications to final scores
-- MAGIC 
-- MAGIC ## Pipeline Stage
-- MAGIC This notebook runs after AI classification and before prospect scoring to create a consolidated view of all classification outputs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Consolidated Classifications Table
-- MAGIC 
-- MAGIC Consolidates AI summaries with latest AI classifications into a unified view for prospect scoring.

-- COMMAND ----------

-- Create consolidated classifications table
CREATE OR REPLACE TABLE ${catalog}.${schema}.consolidated_classifications AS
WITH latest_classification AS (
  SELECT 
    id,
    is_brick_company,
    company_type,
    company_stage,
    processed_at,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY processed_at DESC) AS rn
  FROM ${catalog}.${schema}.gold_classified_businesses
)
SELECT 
  s.id,
  s.website,
  s.ai_business_summary,
  lc.is_brick_company,
  lc.company_type,
  lc.company_stage,
  lc.processed_at AS last_updated,
  current_timestamp() as consolidated_at
FROM ${catalog}.${schema}.gold_ai_summaries s
JOIN latest_classification lc
  ON s.id = lc.id AND lc.rn = 1
WHERE s.ai_business_summary IS NOT NULL
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consolidation Results
-- MAGIC 
-- MAGIC The consolidated classifications table provides a unified view for prospect scoring:

-- COMMAND ----------

-- Display consolidated classifications
SELECT 
  id,
  website,
  is_brick_company,
  company_type,
  company_stage,
  SUBSTRING(ai_business_summary, 1, 100) as summary_preview,
  consolidated_at
FROM ${catalog}.${schema}.consolidated_classifications
ORDER BY consolidated_at DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consolidation Summary
-- MAGIC 
-- MAGIC ✅ **Successfully consolidated AI classifications**
-- MAGIC 
-- MAGIC **Consolidated Data:**
-- MAGIC - **Prospect ID**: Unique identifier for each prospect
-- MAGIC - **Website**: Company website URL
-- MAGIC - **AI Business Summary**: Concise business description from AI analysis
-- MAGIC - **Brick Company Classification**: Whether the company manufactures/sells bricks
-- MAGIC - **Company Type**: B2B or B2C classification
-- MAGIC - **Company Stage**: Early stage, growth stage, or mature stage
-- MAGIC 
-- MAGIC **Key Features:**
-- MAGIC - **Unified Structure**: All classifications in one table for easy scoring
-- MAGIC - **Data Quality**: Filters out records without AI summaries
-- MAGIC - **Performance Optimized**: Processes all available records
-- MAGIC - **Timestamp Tracking**: Records when consolidation occurred
-- MAGIC 
-- MAGIC **Next Steps:**
-- MAGIC - Prospect scoring will use this consolidated view
-- MAGIC - Scoring algorithms can access all classification data efficiently
-- MAGIC - Email generation will reference the consolidated classifications
-- MAGIC 
-- MAGIC The consolidation stage is complete and ready for prospect scoring! 
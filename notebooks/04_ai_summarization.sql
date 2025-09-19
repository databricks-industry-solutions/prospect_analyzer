-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AI Summarization Pipeline
-- MAGIC 
-- MAGIC **Brickstore Prospect Analysis Pipeline**
-- MAGIC 
-- MAGIC This notebook uses Mosaic AI to generate business summaries from website content. Brickstore uses these summaries to understand what construction companies do and identify potential brick supply opportunities.
-- MAGIC 
-- MAGIC **Business Value**: Automatically generates business intelligence from website content, saving hours of manual research per prospect.
-- MAGIC 
-- MAGIC **Key Features:**
-- MAGIC - AI-powered business summary generation
-- MAGIC - Construction industry focus
-- MAGIC - Batch processing for efficiency
-- MAGIC - Structured output for scoring

-- COMMAND ----------

-- Variables are passed from the job parameters
-- Catalog and schema are provided as base_parameters in the job definition

-- COMMAND ----------

DROP TABLE IF EXISTS ${catalog}.${schema}.gold_ai_summaries;

-- COMMAND ----------

-- Create gold table if not exists
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.gold_ai_summaries (
    id STRING,
    website STRING,
    cleaned_content STRING,
    ai_business_summary STRING,
    processed_at TIMESTAMP
);

-- COMMAND ----------

-- Create temp view for unprocessed records
CREATE OR REPLACE TEMPORARY VIEW unprocessed_records AS
SELECT silver.*
FROM ${catalog}.${schema}.silver_cleaned_websites silver
LEFT JOIN ${catalog}.${schema}.gold_ai_summaries gold
ON silver.id = gold.id
WHERE gold.id IS NULL 
  AND length(silver.cleaned_content) > 500
ORDER BY length(silver.cleaned_content) DESC
;

-- COMMAND ----------

INSERT INTO ${catalog}.${schema}.gold_ai_summaries
SELECT 
    CAST(id AS STRING) as id,
    CAST(website AS STRING) as website,
    CAST(cleaned_content AS STRING) as cleaned_content,
    get_json_object(summary_result, '$.business_summary') as ai_business_summary,
    current_timestamp() as processed_at
FROM (
    SELECT 
        id,
        website,
        cleaned_content,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            concat(
                'Analyze this website content and provide a business summary. ',
                'Content to analyze: ',
                SUBSTRING(LOWER(regexp_replace(cleaned_content, '[^a-zA-Z\\s]', '')), 1, 10000)
            ),
            responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                    "name": "business_summary",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "business_summary": {
                                "type": "string",
                                "maxLength": 2000,
                                "description": "Comprehensive 2000-character summary of business, services, market focus, target customers, and key offerings"
                            }
                        },
                        "required": ["business_summary"]
                    },
                    "strict": true
                }
            }'
        ) as summary_result
    FROM unprocessed_records
) summarized;

-- COMMAND ----------

OPTIMIZE ${catalog}.${schema}.gold_ai_summaries;

-- COMMAND ----------

SELECT COUNT(*) as total_summaries
FROM ${catalog}.${schema}.gold_ai_summaries;

-- COMMAND ----------

-- Sample results
SELECT id, website, 
       SUBSTRING(ai_business_summary, 1, 200) as summary_preview
FROM ${catalog}.${schema}.gold_ai_summaries
ORDER BY processed_at DESC
; 
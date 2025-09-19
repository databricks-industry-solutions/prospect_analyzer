-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AI Classification Pipeline
-- MAGIC 
-- MAGIC **Brickstore Prospect Analysis Pipeline**
-- MAGIC 
-- MAGIC This notebook uses Mosaic AI to classify companies based on their business summaries. Brickstore uses these classifications to identify construction companies, contractors, and builders who need brick supplies.
-- MAGIC 
-- MAGIC **Business Value**: Automatically categorizes prospects to focus sales efforts on the most relevant companies.
-- MAGIC 
-- MAGIC **Key Features:**
-- MAGIC - AI-powered company classification
-- MAGIC - Construction industry focus
-- MAGIC - Multi-dimensional scoring
-- MAGIC - Structured output for prioritization

-- COMMAND ----------

-- Variables are passed from the job parameters
-- Catalog and schema are provided as base_parameters in the job definition

-- COMMAND ----------

DROP TABLE IF EXISTS ${catalog}.${schema}.gold_classified_businesses;

-- COMMAND ----------

-- Create output table if not exists
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.gold_classified_businesses (
    id STRING,
    is_brick_company STRING,
    company_type STRING,
    company_stage STRING,
    processed_at TIMESTAMP
);

-- COMMAND ----------

-- Create temp view for unprocessed records
CREATE OR REPLACE TEMPORARY VIEW unprocessed_records AS
SELECT t1.id, LOWER(regexp_replace(t1.ai_business_summary, '[^a-zA-Z\\s]', '')) as ai_summary
FROM ${catalog}.${schema}.gold_ai_summaries t1
LEFT JOIN ${catalog}.${schema}.gold_classified_businesses g
ON t1.id = g.id
WHERE g.id IS NULL 
  AND t1.id IS NOT NULL
  AND t1.ai_business_summary IS NOT NULL
  AND length(t1.ai_business_summary) > 100
ORDER BY t1.id
;

-- COMMAND ----------

-- Process and insert records with structured JSON output
INSERT INTO ${catalog}.${schema}.gold_classified_businesses
SELECT 
    id,
    get_json_object(classification_result, '$.is_brick_company') as is_brick_company,
    get_json_object(classification_result, '$.company_type') as company_type,
    get_json_object(classification_result, '$.company_stage') as company_stage,
    current_timestamp() as processed_at
FROM (
    SELECT 
        id,
        ai_query(
            'databricks-meta-llama-3-3-70b-instruct',
            concat(
                'Analyze this business and classify it across three dimensions. ',
                'Text to analyze: ', ai_summary
            ),
            responseFormat => '{
                "type": "json_schema",
                "json_schema": {
                    "name": "business_classification",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "is_brick_company": {
                                "type": "string",
                                "enum": ["brick_company", "not_brick_company"],
                                "description": "Whether company manufactures/sells brick products"
                            },
                            "company_type": {
                                "type": "string", 
                                "enum": ["business_to_business", "business_to_customer"],
                                "description": "B2B if sells to businesses/contractors, B2C if sells to consumers"
                            },
                            "company_stage": {
                                "type": "string",
                                "enum": ["early_stage_company", "growth_stage_company", "mature_stage_company"],
                                "description": "Company maturity: early (pre-revenue), growth (scaling), mature (established)"
                            }
                        },
                        "required": ["is_brick_company", "company_type", "company_stage"]
                    },
                    "strict": true
                }
            }'
        ) as classification_result
    FROM unprocessed_records
) classified;

-- COMMAND ----------

OPTIMIZE ${catalog}.${schema}.gold_classified_businesses;

-- COMMAND ----------

SELECT COUNT(*) as total_classified
FROM ${catalog}.${schema}.gold_classified_businesses;

-- COMMAND ----------

-- Sample classification results
SELECT id, is_brick_company, company_type, company_stage
FROM ${catalog}.${schema}.gold_classified_businesses
ORDER BY processed_at DESC
;

-- COMMAND ----------

-- Classification summary stats
SELECT 
    is_brick_company,
    company_type,
    company_stage,
    COUNT(*) as count
FROM ${catalog}.${schema}.gold_classified_businesses
GROUP BY is_brick_company, company_type, company_stage
ORDER BY count DESC; 
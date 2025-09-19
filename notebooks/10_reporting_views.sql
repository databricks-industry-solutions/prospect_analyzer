-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Reporting Views for Prospect Analysis
-- MAGIC 
-- MAGIC **Brickstore's clean reporting layer** - Creates well-structured views for easy development and analysis on top of the gold tables.
-- MAGIC 
-- MAGIC ## Business Value
-- MAGIC - **Clean Data Access**: Provides consistent, well-documented views for all reporting needs
-- MAGIC - **Development Ready**: Optimized views for building dashboards, reports, and analytics
-- MAGIC - **Performance Optimized**: Efficient queries with proper indexing and partitioning
-- MAGIC - **Business Focused**: Views organized around key business questions and use cases
-- MAGIC 
-- MAGIC ## Pipeline Stage
-- MAGIC This notebook creates the final reporting layer after all data processing is complete.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Core Prospect Analysis View
-- MAGIC 
-- MAGIC Comprehensive view combining all prospect data with scoring and classifications

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_prospect_analysis AS
SELECT 
    -- Core prospect information
    ps.id,
    ps.website,
    ps.ai_business_summary,
    
    -- AI Classifications
    ps.is_brick_company,
    ps.company_type,
    ps.company_stage,
    
    -- Scoring and Prioritization
    ps.composite_score,
    ps.score_tier,
    ps.company_type_score,
    ps.brick_company_score,
    ps.company_stage_score,
    
    -- Timestamps
    ps.classification_timestamp,
    ps.scoring_timestamp,
    
    -- Business Logic Fields
    CASE 
        WHEN ps.is_brick_company = 'brick_company' THEN 'Brick Company'
        WHEN ps.is_brick_company = 'not_brick_company' THEN 'Non-Brick Company'
        ELSE 'Unknown'
    END as brick_company_category,
    
    CASE 
        WHEN ps.company_type = 'business_to_business' THEN 'B2B'
        WHEN ps.company_type = 'business_to_customer' THEN 'B2C'
        ELSE 'Unknown'
    END as business_model,
    
    CASE 
        WHEN ps.company_stage = 'early_stage_company' THEN 'Early Stage'
        WHEN ps.company_stage = 'growth_stage_company' THEN 'Growth Stage'
        WHEN ps.company_stage = 'mature_stage_company' THEN 'Mature Stage'
        ELSE 'Unknown'
    END as company_maturity,
    
    -- Priority Classification
    CASE 
        WHEN ps.score_tier = 'A - High Priority' THEN 'High'
        WHEN ps.score_tier = 'B - Medium Priority' THEN 'Medium'
        WHEN ps.score_tier = 'C - Low Priority' THEN 'Low'
        WHEN ps.score_tier = 'D - Very Low Priority' THEN 'Very Low'
        ELSE 'Unscored'
    END as priority_level,
    
    -- Ideal Prospect Flag
    CASE 
        WHEN ps.is_brick_company = 'brick_company' 
         AND ps.company_type = 'business_to_business'
         AND ps.company_stage = 'mature_stage_company'
         AND ps.composite_score >= 0.75
        THEN 'Ideal Prospect'
        WHEN ps.composite_score >= 0.5
        THEN 'Good Prospect'
        WHEN ps.composite_score >= 0.25
        THEN 'Fair Prospect'
        ELSE 'Poor Prospect'
    END as prospect_quality

FROM ${catalog}.${schema}.gold_scored_prospects ps
WHERE ps.ai_business_summary IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Email Campaign View
-- MAGIC 
-- MAGIC View combining prospect data with generated email content and HTML-rendered email links for dashboard display

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_email_campaigns AS
SELECT 
    -- Prospect Information
    ce.id,
    ce.website,
    ce.ai_business_summary,
    ce.is_brick_company,
    ce.company_type,
    ce.company_stage,
    ce.composite_score,
    ce.score_tier,
    
    -- Generated Email Content
    ce.email_1_initial_outreach,
    ce.email_2_use_case_highlight,
    ce.email_3_light_touch,
    
    -- Processing Information
    ce.processed_at as email_generated_at,
    
    -- Business Logic
    CASE 
        WHEN ce.score_tier = 'A - High Priority' THEN 'High Priority Campaign'
        WHEN ce.score_tier = 'B - Medium Priority' THEN 'Medium Priority Campaign'
        WHEN ce.score_tier = 'C - Low Priority' THEN 'Low Priority Campaign'
        ELSE 'General Campaign'
    END as campaign_type,
    
    -- Email Status (for tracking)
    'Generated' as email_status,
    current_timestamp() as last_updated,
    
    -- HTML Email Links for Dashboard Rendering
    -- Email 1: Initial Outreach
    CONCAT(
        '<div style="font-family: DM Sans, sans-serif; margin-bottom: 10px;">',
        '<a href="https://mail.google.com/mail/?view=cm&fs=1&to=&su=',
        -- Get subject line more reliably - try multiple patterns
        URL_ENCODE(
            COALESCE(
                NULLIF(REGEXP_EXTRACT(ce.email_1_initial_outreach, 'Subject Line: ([^\r\n]+)', 1), ''),
                NULLIF(REGEXP_EXTRACT(ce.email_1_initial_outreach, 'Subject: ([^\r\n]+)', 1), ''),
                NULLIF(
                    REGEXP_EXTRACT(ce.email_1_initial_outreach, '\\*\\*Subject Line:\\*\\* ([^\r\n]+)', 1), ''
                ),
                CONCAT(
                    'Company-Specific Opportunity to Enhance Operations with Premium Brick Solutions'
                )
            )
        ),
        '&body=',
        -- Clean up the email body by removing instructions and notes
        URL_ENCODE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    -- Extract from "Hi [First Name]" to before "**Instructions:**"
                    SUBSTRING(
                        ce.email_1_initial_outreach,
                        GREATEST(
                            COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_1_initial_outreach), 0), 1),
                            COALESCE(NULLIF(POSITION('Hi [First Name]' IN ce.email_1_initial_outreach), 0), 1)
                        ),
                        CASE
                            WHEN
                                POSITION('**Instructions:**' IN ce.email_1_initial_outreach) > 0
                            THEN
                                POSITION('**Instructions:**' IN ce.email_1_initial_outreach)
                                - GREATEST(
                                    COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_1_initial_outreach), 0), 1),
                                    COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_1_initial_outreach), 0), 1)
                                )
                            ELSE LENGTH(ce.email_1_initial_outreach)
                        END
                    ),
                    '\\*\\*Instructions:\\*\\*[\\s\\S]*',
                    '' -- Remove any remaining instructions
                ),
                'Note: I chose[\\s\\S]*',
                '' -- Remove note about chosen benefits
            )
        ),
        '" target="_blank" style="color: #0066cc; text-decoration: none;">',
        '<button style="background-color: #CD853F; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-weight: bold;">',
        'Email 1: Initial Outreach →',
        '</button>',
        '</a></div>'
    ) AS email_1_link,
    
    -- Email 2: Use Case Highlight (similar pattern)
    CONCAT(
        '<div style="font-family: DM Sans, sans-serif; margin-bottom: 10px;">',
        '<a href="https://mail.google.com/mail/?view=cm&fs=1&to=&su=',
        URL_ENCODE(
            COALESCE(
                NULLIF(REGEXP_EXTRACT(ce.email_2_use_case_highlight, 'Subject Line: ([^\r\n]+)', 1), ''),
                NULLIF(REGEXP_EXTRACT(ce.email_2_use_case_highlight, 'Subject: ([^\r\n]+)', 1), ''),
                NULLIF(
                    REGEXP_EXTRACT(ce.email_2_use_case_highlight, '\\*\\*Subject Line:\\*\\* ([^\r\n]+)', 1), ''
                ),
                CONCAT(
                    'Unlock New Construction Possibilities with Premium Brick Solutions'
                )
            )
        ),
        '&body=',
        URL_ENCODE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    SUBSTRING(
                        ce.email_2_use_case_highlight,
                        GREATEST(
                            COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_2_use_case_highlight), 0), 1),
                            COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_2_use_case_highlight), 0), 1)
                        ),
                        CASE
                            WHEN
                                POSITION('**Instructions:**' IN ce.email_2_use_case_highlight) > 0
                            THEN
                                POSITION('**Instructions:**' IN ce.email_2_use_case_highlight)
                                - GREATEST(
                                    COALESCE(
                                        NULLIF(POSITION('Hi [First Name],' IN ce.email_2_use_case_highlight), 0), 1
                                    ),
                                    COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_2_use_case_highlight), 0), 1)
                                )
                            ELSE LENGTH(ce.email_2_use_case_highlight)
                        END
                    ),
                    '\\*\\*Instructions:\\*\\*[\\s\\S]*',
                    ''
                ),
                'Note: I chose[\\s\\S]*',
                ''
            )
        ),
        '" target="_blank" style="color: #0066cc; text-decoration: none;">',
        '<button style="background-color: #A0522D; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-weight: bold;">',
        'Email 2: Use Case Highlight with Strong CTA →',
        '</button>',
        '</a></div>'
    ) AS email_2_link,
    
    -- Email 3: Light Touch (similar pattern)
    CONCAT(
        '<div style="font-family: DM Sans, sans-serif; margin-bottom: 10px;">',
        '<a href="https://mail.google.com/mail/?view=cm&fs=1&to=&su=',
        URL_ENCODE(
            COALESCE(
                NULLIF(REGEXP_EXTRACT(ce.email_3_light_touch, 'Subject Line: ([^\r\n]+)', 1), ''),
                NULLIF(REGEXP_EXTRACT(ce.email_3_light_touch, 'Subject: ([^\r\n]+)', 1), ''),
                NULLIF(REGEXP_EXTRACT(ce.email_3_light_touch, '\\*\\*Subject Line:\\*\\* ([^\r\n]+)', 1), ''),
                CONCAT(
                    'A Quick Idea for Scaling Your Construction Success with Premium Bricks'
                )
            )
        ),
        '&body=',
        URL_ENCODE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    SUBSTRING(
                        ce.email_3_light_touch,
                        GREATEST(
                            COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_3_light_touch), 0), 1),
                            COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_3_light_touch), 0), 1)
                        ),
                        CASE
                            WHEN
                                POSITION('**Instructions:**' IN ce.email_3_light_touch) > 0
                            THEN
                                POSITION('**Instructions:**' IN ce.email_3_light_touch)
                                - GREATEST(
                                    COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_3_light_touch), 0), 1),
                                    COALESCE(NULLIF(POSITION('Hi [First Name],' IN ce.email_3_light_touch), 0), 1)
                                )
                            ELSE LENGTH(ce.email_3_light_touch)
                        END
                    ),
                    '\\*\\*Instructions:\\*\\*[\\s\\S]*',
                    ''
                ),
                'Note: I chose[\\s\\S]*',
                ''
            )
        ),
        '" target="_blank" style="color: #0066cc; text-decoration: none;">',
        '<button style="background-color: #8B4513; color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-weight: bold;">',
        'Email 3: Light Touch with Value Reminder →',
        '</button>',
        '</a></div>'
    ) AS email_3_link,
    
    -- Combined HTML for all email links
    CONCAT(
        email_1_link, 
        email_2_link, 
        email_3_link
    ) as all_email_links

FROM ${catalog}.${schema}.campaign_emails ce;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Pipeline Performance View
-- MAGIC 
-- MAGIC View for monitoring pipeline performance and data quality

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_pipeline_performance AS
WITH pipeline_stats AS (
    SELECT 
        'Data Ingestion' as pipeline_stage,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN website IS NOT NULL THEN 1 END) as successful_records,
        current_timestamp() as last_updated
    FROM ${catalog}.${schema}.customer_list
    
    UNION ALL
    
    SELECT 
        'Website Scraping' as pipeline_stage,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN scraped_content IS NOT NULL THEN 1 END) as successful_records,
        current_timestamp() as last_updated
    FROM ${catalog}.${schema}.bronze_scraped_websites
    
    UNION ALL
    
    SELECT 
        'Content Cleaning' as pipeline_stage,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN cleaned_content IS NOT NULL THEN 1 END) as successful_records,
        current_timestamp() as last_updated
    FROM ${catalog}.${schema}.silver_cleaned_websites
    
    UNION ALL
    
    SELECT 
        'AI Summarization' as pipeline_stage,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN ai_business_summary IS NOT NULL THEN 1 END) as successful_records,
        current_timestamp() as last_updated
    FROM ${catalog}.${schema}.gold_ai_summaries
    
    UNION ALL
    
    SELECT 
        'AI Classification' as pipeline_stage,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN is_brick_company IS NOT NULL THEN 1 END) as successful_records,
        current_timestamp() as last_updated
    FROM ${catalog}.${schema}.gold_classified_businesses
    
    UNION ALL
    
    SELECT 
        'Prospect Scoring' as pipeline_stage,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN composite_score IS NOT NULL THEN 1 END) as successful_records,
        current_timestamp() as last_updated
    FROM ${catalog}.${schema}.gold_scored_prospects
    
    UNION ALL
    
    SELECT 
        'Email Generation' as pipeline_stage,
        COUNT(*) as records_processed,
        COUNT(CASE WHEN email_1_initial_outreach IS NOT NULL THEN 1 END) as successful_records,
        current_timestamp() as last_updated
    FROM ${catalog}.${schema}.campaign_emails
)
SELECT 
    pipeline_stage,
    records_processed,
    successful_records,
    ROUND((successful_records / records_processed) * 100, 2) as success_rate_percent,
    last_updated
FROM pipeline_stats
ORDER BY 
    CASE pipeline_stage
        WHEN 'Data Ingestion' THEN 1
        WHEN 'Website Scraping' THEN 2
        WHEN 'Content Cleaning' THEN 3
        WHEN 'AI Summarization' THEN 4
        WHEN 'AI Classification' THEN 5
        WHEN 'Prospect Scoring' THEN 6
        WHEN 'Email Generation' THEN 7
    END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Prospect Scoring Summary View
-- MAGIC 
-- MAGIC Aggregated view for prospect scoring analysis and reporting

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_prospect_scoring_summary AS
SELECT 
    -- Score Tier Analysis
    score_tier,
    COUNT(*) as prospect_count,
    ROUND(AVG(composite_score), 3) as avg_score,
    ROUND(MIN(composite_score), 3) as min_score,
    ROUND(MAX(composite_score), 3) as max_score,
    
    -- Business Classification Breakdown
    COUNT(CASE WHEN is_brick_company = 'brick_company' THEN 1 END) as brick_companies,
    COUNT(CASE WHEN company_type = 'business_to_business' THEN 1 END) as b2b_companies,
    COUNT(CASE WHEN company_stage = 'mature_stage_company' THEN 1 END) as mature_companies,
    
    -- Ideal Prospect Count
    COUNT(CASE 
        WHEN is_brick_company = 'brick_company' 
         AND company_type = 'business_to_business'
         AND company_stage = 'mature_stage_company'
        THEN 1 
    END) as ideal_prospects,
    
    -- Timestamp
    MAX(scoring_timestamp) as last_scored_at

FROM ${catalog}.${schema}.gold_scored_prospects
WHERE composite_score IS NOT NULL
GROUP BY score_tier
ORDER BY 
    CASE score_tier
        WHEN 'A - High Priority' THEN 1
        WHEN 'B - Medium Priority' THEN 2
        WHEN 'C - Low Priority' THEN 3
        WHEN 'D - Very Low Priority' THEN 4
        ELSE 5
    END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Business Intelligence View
-- MAGIC 
-- MAGIC High-level business metrics and KPIs for executive reporting

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_business_intelligence AS
WITH kpis AS (
    SELECT 
        -- Total Prospects
        COUNT(*) as total_prospects,
        
        -- Scored Prospects
        COUNT(CASE WHEN composite_score IS NOT NULL THEN 1 END) as scored_prospects,
        
        -- High Priority Prospects
        COUNT(CASE WHEN score_tier = 'A - High Priority' THEN 1 END) as high_priority_prospects,
        
        -- Brick Companies
        COUNT(CASE WHEN is_brick_company = 'brick_company' THEN 1 END) as brick_companies,
        
        -- B2B Companies
        COUNT(CASE WHEN company_type = 'business_to_business' THEN 1 END) as b2b_companies,
        
        -- Mature Companies
        COUNT(CASE WHEN company_stage = 'mature_stage_company' THEN 1 END) as mature_companies,
        
        -- Ideal Prospects (Brick + B2B + Mature)
        COUNT(CASE 
            WHEN is_brick_company = 'brick_company' 
             AND company_type = 'business_to_business'
             AND company_stage = 'mature_stage_company'
            THEN 1 
        END) as ideal_prospects,
        
        -- Average Score
        ROUND(AVG(composite_score), 3) as avg_prospect_score,
        
        -- Email Campaigns Generated
        (SELECT COUNT(*) FROM ${catalog}.${schema}.campaign_emails) as emails_generated
        
    FROM ${catalog}.${schema}.gold_scored_prospects
)
SELECT 
    total_prospects,
    scored_prospects,
    high_priority_prospects,
    brick_companies,
    b2b_companies,
    mature_companies,
    ideal_prospects,
    avg_prospect_score,
    emails_generated,
    
    -- Calculated Metrics
    ROUND((scored_prospects / total_prospects) * 100, 2) as scoring_success_rate,
    ROUND((high_priority_prospects / scored_prospects) * 100, 2) as high_priority_rate,
    ROUND((brick_companies / total_prospects) * 100, 2) as brick_company_rate,
    ROUND((b2b_companies / total_prospects) * 100, 2) as b2b_rate,
    ROUND((mature_companies / total_prospects) * 100, 2) as mature_company_rate,
    ROUND((ideal_prospects / total_prospects) * 100, 2) as ideal_prospect_rate,
    
    current_timestamp() as last_updated
FROM kpis;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Data Quality View
-- MAGIC 
-- MAGIC View for monitoring data quality and completeness

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${schema}.v_data_quality AS
SELECT 
    'gold_scored_prospects' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN id IS NOT NULL THEN 1 END) as records_with_id,
    COUNT(CASE WHEN website IS NOT NULL THEN 1 END) as records_with_website,
    COUNT(CASE WHEN ai_business_summary IS NOT NULL THEN 1 END) as records_with_summary,
    COUNT(CASE WHEN is_brick_company IS NOT NULL THEN 1 END) as records_with_brick_classification,
    COUNT(CASE WHEN company_type IS NOT NULL THEN 1 END) as records_with_type_classification,
    COUNT(CASE WHEN company_stage IS NOT NULL THEN 1 END) as records_with_stage_classification,
    COUNT(CASE WHEN composite_score IS NOT NULL THEN 1 END) as records_with_score,
    COUNT(CASE WHEN score_tier IS NOT NULL THEN 1 END) as records_with_tier,
    current_timestamp() as last_checked
    
FROM ${catalog}.${schema}.gold_scored_prospects

UNION ALL

SELECT 
    'campaign_emails' as table_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN id IS NOT NULL THEN 1 END) as records_with_id,
    COUNT(CASE WHEN website IS NOT NULL THEN 1 END) as records_with_website,
    COUNT(CASE WHEN ai_business_summary IS NOT NULL THEN 1 END) as records_with_summary,
    COUNT(CASE WHEN is_brick_company IS NOT NULL THEN 1 END) as records_with_brick_classification,
    COUNT(CASE WHEN company_type IS NOT NULL THEN 1 END) as records_with_type_classification,
    COUNT(CASE WHEN company_stage IS NOT NULL THEN 1 END) as records_with_stage_classification,
    COUNT(CASE WHEN composite_score IS NOT NULL THEN 1 END) as records_with_score,
    COUNT(CASE WHEN score_tier IS NOT NULL THEN 1 END) as records_with_tier,
    current_timestamp() as last_checked
    
FROM ${catalog}.${schema}.campaign_emails;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View Summary
-- MAGIC 
-- MAGIC ✅ **Successfully created 6 reporting views:**
-- MAGIC 
-- MAGIC **Core Views:**
-- MAGIC - **`v_prospect_analysis`**: Complete prospect data with scoring and business logic
-- MAGIC - **`v_email_campaigns`**: Prospect data combined with generated email content and HTML-rendered email links for dashboard display
-- MAGIC 
-- MAGIC **Analytics Views:**
-- MAGIC - **`v_prospect_scoring_summary`**: Aggregated scoring analysis by tier
-- MAGIC - **`v_business_intelligence`**: High-level KPIs and business metrics
-- MAGIC 
-- MAGIC **Monitoring Views:**
-- MAGIC - **`v_pipeline_performance`**: Pipeline success rates and performance metrics
-- MAGIC - **`v_data_quality`**: Data completeness and quality monitoring
-- MAGIC 
-- MAGIC **Key Features:**
-- MAGIC - **Business Logic**: Clean categorization and priority flags
-- MAGIC - **Performance Optimized**: Efficient queries with proper aggregations
-- MAGIC - **Development Ready**: Well-documented views for building reports and dashboards
-- MAGIC - **Quality Monitoring**: Built-in data quality checks and pipeline monitoring
-- MAGIC 
-- MAGIC **Next Steps:**
-- MAGIC - Use `v_prospect_analysis` for detailed prospect analysis
-- MAGIC - Use `v_business_intelligence` for executive reporting
-- MAGIC - Use `v_pipeline_performance` for monitoring pipeline health
-- MAGIC - Build custom reports and dashboards on top of these views
-- MAGIC 
-- MAGIC The reporting layer is now ready for development and analysis! 
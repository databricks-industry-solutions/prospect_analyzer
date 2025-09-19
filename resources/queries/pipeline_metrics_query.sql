-- Pipeline Metrics Query for Dashboard KPIs
-- Shows overall performance and health metrics of the prospect analysis pipeline

SELECT 
  -- Total prospect counts
  COUNT(DISTINCT cl.customer_id) as total_prospects,
  COUNT(DISTINCT ws.customer_id) as scraped_prospects,
  COUNT(DISTINCT cc.customer_id) as processed_prospects,
  COUNT(DISTINCT ps.customer_id) as scored_prospects,
  
  -- Success rates
  ROUND(
    COUNT(DISTINCT ws.customer_id) * 100.0 / COUNT(DISTINCT cl.customer_id), 
    1
  ) as scraping_success_rate,
  
  ROUND(
    COUNT(DISTINCT cc.customer_id) * 100.0 / COUNT(DISTINCT cl.customer_id), 
    1
  ) as processing_success_rate,
  
  ROUND(
    COUNT(DISTINCT ps.customer_id) * 100.0 / COUNT(DISTINCT cl.customer_id), 
    1
  ) as scoring_success_rate,
  
  -- High-value prospect counts
  COUNT(DISTINCT CASE WHEN ps.composite_score >= 0.75 THEN ps.customer_id END) as a_tier_prospects,
  COUNT(DISTINCT CASE WHEN ps.composite_score >= 0.50 THEN ps.customer_id END) as high_value_prospects,
  
  -- Data quality metrics
  ROUND(AVG(CASE WHEN ps.composite_score IS NOT NULL THEN ps.composite_score END), 3) as avg_prospect_score,
  
  -- Timing metrics  
  MAX(cl.created_at) as latest_ingestion,
  MAX(ws.scraped_at) as latest_scraping,
  MAX(ps.scored_at) as latest_scoring,
  
  -- Pipeline health indicators
  CASE 
    WHEN COUNT(DISTINCT ws.customer_id) * 100.0 / COUNT(DISTINCT cl.customer_id) >= 80 
    THEN '🟢 Healthy'
    WHEN COUNT(DISTINCT ws.customer_id) * 100.0 / COUNT(DISTINCT cl.customer_id) >= 60 
    THEN '🟡 Warning'
    ELSE '🔴 Critical'
  END as pipeline_health

FROM ${var.catalog}.${var.schema}.customer_list cl

LEFT JOIN ${var.catalog}.${var.schema}.bronze_scraped_websites ws
  ON cl.customer_id = ws.customer_id 
  AND ws.scraping_status = 'success'

LEFT JOIN ${var.catalog}.${var.schema}.silver_cleaned_websites cc
  ON cl.customer_id = cc.customer_id

LEFT JOIN ${var.catalog}.${var.schema}.prospect_scoring ps
  ON cl.customer_id = ps.customer_id

WHERE cl.company_name IS NOT NULL
  AND cl.company_name != '' 
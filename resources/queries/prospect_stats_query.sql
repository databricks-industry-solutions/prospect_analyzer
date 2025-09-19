-- Prospect Statistics Query for Dashboard Charts
-- Shows distribution of prospects across tiers and other key metrics

SELECT 
  prospect_tier,
  COUNT(*) as prospect_count,
  ROUND(AVG(prospect_score * 100), 1) as avg_score_percentage,
  
  -- Tier colors for consistent visualization
  CASE prospect_tier
    WHEN 'A-Tier' THEN '#28a745'
    WHEN 'B-Tier' THEN '#17a2b8'
    WHEN 'C-Tier' THEN '#ffc107'
    WHEN 'D-Tier' THEN '#dc3545'
  END as tier_color

FROM (
  SELECT 
    cl.customer_id,
    cl.company_name,
    COALESCE(ps.composite_score, 0) as prospect_score,
    
    -- Tier classification
    CASE 
      WHEN COALESCE(ps.composite_score, 0) >= 0.75 THEN 'A-Tier'
      WHEN COALESCE(ps.composite_score, 0) >= 0.50 THEN 'B-Tier' 
      WHEN COALESCE(ps.composite_score, 0) >= 0.25 THEN 'C-Tier'
      ELSE 'D-Tier'
    END AS prospect_tier
    
  FROM ${var.catalog}.${var.schema}.customer_list cl
  LEFT JOIN ${var.catalog}.${var.schema}.prospect_scoring ps
    ON cl.customer_id = ps.customer_id
  WHERE cl.company_name IS NOT NULL
    AND cl.company_name != ''
) tier_data

GROUP BY prospect_tier
ORDER BY 
  CASE prospect_tier
    WHEN 'A-Tier' THEN 1
    WHEN 'B-Tier' THEN 2
    WHEN 'C-Tier' THEN 3
    WHEN 'D-Tier' THEN 4
  END 
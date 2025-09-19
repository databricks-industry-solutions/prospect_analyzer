-- Prospect Analysis Campaign Data Query
-- Integrates data from the AI-powered prospect analyzer pipeline for outreach dashboard

SELECT DISTINCT
  cl.customer_id as id,
  cl.company_name as Company_Name,
  cl.website_url,
  
  -- Formatted company header with clickable website link
  CONCAT(
    '<div style="font-family: Arial, sans-serif; margin-bottom: 10px; border-left: 4px solid #0066cc; padding-left: 12px;">',
    '<a href="', 
    CASE 
      WHEN cl.website_url LIKE 'http%' THEN cl.website_url
      ELSE CONCAT('https://', cl.website_url)
    END,
    '" target="_blank" style="color: #0066cc; text-decoration: none; font-weight: bold; font-size: 1.1em;">',
    cl.company_name,
    '</a><br>',
    '<span style="color: #666; font-size: 0.9em;">',
    COALESCE(cl.website_url, 'No website'),
    '</span>',
    '</div>'
  ) AS formatted_company_header,
  
  -- AI Business summary from AI summarization
  COALESCE(
    SUBSTRING(ais.ai_business_summary, 1, 300),
    'AI analysis pending'
  ) AS business_summary,
  
  -- Prospect scoring data
  COALESCE(ps.prospect_score, 0) AS prospect_score,
  ROUND(COALESCE(ps.prospect_score, 0) * 100, 0) AS score_percentage,
  
  -- Prospect tier from scoring table
  COALESCE(ps.prospect_tier, 'Unscored') AS prospect_tier,
  
  -- Tier description
  CASE 
    WHEN ps.prospect_tier = 'A-Tier' THEN 'High Priority - Immediate Follow-up'
    WHEN ps.prospect_tier = 'B-Tier' THEN 'Medium Priority - Active Engagement'
    WHEN ps.prospect_tier = 'C-Tier' THEN 'Low Priority - Nurture Campaign'
    ELSE 'Very Low Priority - Long-term Nurturing'
  END AS tier_description,
  
  -- AI Classification details
  CASE 
    WHEN c.is_brick_company LIKE '%brick_company%' THEN 'Brick Company'
    ELSE 'Not Brick Related'
  END as industry_classification,
  
  CASE 
    WHEN c.company_type LIKE '%business-to-business%' THEN 'B2B'
    WHEN c.company_type LIKE '%business-to-customer%' THEN 'B2C'
    ELSE 'Unknown'
  END as company_type_classification,
  
  CASE 
    WHEN c.company_stage LIKE '%early_stage%' THEN 'Early Stage'
    WHEN c.company_stage LIKE '%growth_stage%' THEN 'Growth Stage'
    WHEN c.company_stage LIKE '%mature_stage%' THEN 'Mature'
    ELSE 'Unknown'
  END as company_stage_classification,
  
  -- Outreach action buttons (HTML formatted for email templates)
  CONCAT(
    '<div style="display: flex; flex-direction: column; gap: 6px;">',
    '<button style="background: linear-gradient(135deg, #ff7f50, #ff6347); color: white; border: none; padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 11px; font-weight: 500; box-shadow: 0 2px 4px rgba(0,0,0,0.1); transition: all 0.2s;" ',
    'onclick="generateEmail(''', cl.customer_id, ''', ''initial'', ''', cl.company_name, ''')">',
    '📧 Initial Outreach</button>',
    
    '<button style="background: linear-gradient(135deg, #4CAF50, #45a049); color: white; border: none; padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 11px; font-weight: 500; box-shadow: 0 2px 4px rgba(0,0,0,0.1); transition: all 0.2s;" ',
    'onclick="generateEmail(''', cl.customer_id, ''', ''use_case'', ''', cl.company_name, ''')">',
    '🎯 Use Case Highlight</button>',
    
    '<button style="background: linear-gradient(135deg, #2196F3, #1976D2); color: white; border: none; padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 11px; font-weight: 500; box-shadow: 0 2px 4px rgba(0,0,0,0.1); transition: all 0.2s;" ',
    'onclick="generateEmail(''', cl.customer_id, ''', ''light_touch'', ''', cl.company_name, ''')">',
    '💡 Value Reminder</button>',
    '</div>'
  ) AS outreach_actions,
  
  -- Scraping and processing status
  CASE WHEN sc.customer_id IS NOT NULL THEN '✅ Scraped' 
       ELSE '⏳ Pending' END AS scraping_status,
       
  -- AI processing status
  CASE WHEN ais.customer_id IS NOT NULL THEN '✅ AI Processed'
       ELSE '⏳ Pending' END AS content_status,
       
  -- Timestamps
  cl.created_at as ingestion_date,
  sc.scraped_at,
  ps.processed_at as scored_at

FROM ${var.catalog}.${var.schema}.customer_list cl

-- Join website scraping data
LEFT JOIN ${var.catalog}.${var.schema}.bronze_scraped sc
  ON cl.customer_id = sc.customer_id 

-- Join AI summaries
LEFT JOIN ${var.catalog}.${var.schema}.gold_ai_summaries ais
  ON cl.customer_id = ais.customer_id

-- Join AI classifications
LEFT JOIN ${var.catalog}.${var.schema}.gold_classified_businesses c
  ON cl.customer_id = c.customer_id

-- Join prospect scoring data
LEFT JOIN ${var.catalog}.${var.schema}.gold_scored_prospects ps
  ON cl.customer_id = ps.customer_id

WHERE cl.company_name IS NOT NULL
  AND cl.company_name != ''

ORDER BY 
  COALESCE(ps.prospect_score, 0) DESC,
  cl.company_name ASC 
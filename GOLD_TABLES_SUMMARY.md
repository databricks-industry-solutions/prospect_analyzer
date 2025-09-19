# Gold Tables and Reporting Views Summary

## Overview
This document outlines the clean, well-structured gold tables and reporting views available for development and analysis in the Brickstore Prospect Analyzer pipeline.

## Pipeline Flow
```
01_data_ingestion → 02_website_scraping → 03_content_cleaning → 04_ai_summarization → 
05_ai_classification → 06_consolidation → 07_prospect_scoring → 08_email_generation → 
09_monitoring_validation → 10_reporting_views
```

## Gold Tables (Raw Data)

### 1. `gold_scored_prospects`
**Purpose**: Complete prospect data with AI classifications and scoring
**Key Columns**:
- `id`, `website`, `ai_business_summary`
- `is_brick_company`, `company_type`, `company_stage`
- `composite_score`, `score_tier`
- `company_type_score`, `brick_company_score`, `company_stage_score`
- `classification_timestamp`, `scoring_timestamp`

### 2. `campaign_emails`
**Purpose**: Generated email content for each prospect
**Key Columns**:
- `id`, `website`, `ai_business_summary`
- `email_1_initial_outreach`, `email_2_use_case_highlight`, `email_3_light_touch`
- `composite_score`, `score_tier`
- `processed_at`

### 3. `gold_ai_summaries`
**Purpose**: AI-generated business summaries from website content
**Key Columns**:
- `id`, `website`, `ai_business_summary`
- `processed_at`

### 4. `gold_classified_businesses`
**Purpose**: AI classifications for each prospect
**Key Columns**:
- `id`, `is_brick_company`, `company_type`, `company_stage`
- `processed_at`

## Reporting Views (Development Ready)

### 1. `v_prospect_analysis` ⭐ **PRIMARY VIEW**
**Purpose**: Complete prospect analysis with business logic and clean categorization
**Key Features**:
- All prospect data with scoring
- Business logic fields: `brick_company_category`, `business_model`, `company_maturity`
- Priority classification: `priority_level`
- Prospect quality: `prospect_quality` (Ideal/Good/Fair/Poor)
- Clean, human-readable categories

**Use Cases**:
- Detailed prospect analysis
- Sales team reporting
- Custom dashboards
- Prospect prioritization

### 2. `v_email_campaigns`
**Purpose**: Prospect data combined with generated email content
**Key Features**:
- All prospect information
- Three generated email templates
- Campaign type classification
- Email status tracking

**Use Cases**:
- Email campaign management
- Sales outreach tracking
- Email performance analysis

### 3. `v_business_intelligence` ⭐ **EXECUTIVE VIEW**
**Purpose**: High-level KPIs and business metrics
**Key Metrics**:
- `total_prospects`, `scored_prospects`, `high_priority_prospects`
- `brick_companies`, `b2b_companies`, `mature_companies`, `ideal_prospects`
- `avg_prospect_score`, `emails_generated`
- Success rates and percentages

**Use Cases**:
- Executive dashboards
- Business reporting
- Performance monitoring
- ROI analysis

### 4. `v_prospect_scoring_summary`
**Purpose**: Aggregated scoring analysis by tier
**Key Features**:
- Breakdown by score tier (A/B/C/D)
- Count, average, min, max scores
- Business classification breakdowns
- Ideal prospect counts

**Use Cases**:
- Scoring analysis
- Tier distribution reporting
- Quality assessment

### 5. `v_pipeline_performance`
**Purpose**: Pipeline monitoring and performance metrics
**Key Features**:
- Success rates for each pipeline stage
- Records processed vs successful
- Pipeline health monitoring

**Use Cases**:
- Pipeline monitoring
- Performance optimization
- Data quality tracking

### 6. `v_data_quality`
**Purpose**: Data completeness and quality monitoring
**Key Features**:
- Field-level completeness checks
- Data quality metrics
- Quality monitoring over time

**Use Cases**:
- Data quality monitoring
- Pipeline validation
- Quality reporting

## Development Guidelines

### Recommended Starting Points

1. **For General Analysis**: Start with `v_prospect_analysis`
   ```sql
   SELECT * FROM v_prospect_analysis 
   WHERE prospect_quality = 'Ideal Prospect'
   ORDER BY composite_score DESC;
   ```

2. **For Executive Reporting**: Use `v_business_intelligence`
   ```sql
   SELECT * FROM v_business_intelligence;
   ```

3. **For Email Campaigns**: Use `v_email_campaigns`
   ```sql
   SELECT website, score_tier, email_1_initial_outreach 
   FROM v_email_campaigns 
   WHERE score_tier = 'A - High Priority';
   ```

### Business Logic Examples

**Ideal Prospect Definition**:
```sql
-- Companies that are brick companies, B2B, and mature stage
WHERE is_brick_company = 'brick_company' 
  AND company_type = 'business_to_business'
  AND company_stage = 'mature_stage_company'
  AND composite_score >= 0.75
```

**High Priority Prospects**:
```sql
-- Score tier A or B with good composite scores
WHERE score_tier IN ('A - High Priority', 'B - Medium Priority')
  AND composite_score >= 0.5
```

**B2B Construction Companies**:
```sql
-- Business-to-business companies in construction
WHERE company_type = 'business_to_business'
  AND ai_business_summary LIKE '%construction%'
```

### Performance Tips

1. **Use Views**: All reporting views are optimized for performance
2. **Filter Early**: Apply WHERE clauses early in your queries
3. **Limit Results**: Use LIMIT for testing and development
4. **Monitor Performance**: Use `v_pipeline_performance` to track pipeline health

## Next Steps for Development

1. **Custom Dashboards**: Build on top of `v_business_intelligence` and `v_prospect_analysis`
2. **Sales Tools**: Create applications using `v_email_campaigns`
3. **Analytics**: Develop custom reports using the scoring summary views
4. **Monitoring**: Set up alerts using the performance and quality views

## Schema Location
All tables and views are located in: `prospect_analyzer.prospect_analyzer_dev`

## Access Pattern
```sql
-- Example: Get all high-priority prospects
SELECT 
    website,
    ai_business_summary,
    prospect_quality,
    composite_score,
    priority_level
FROM v_prospect_analysis
WHERE priority_level = 'High'
ORDER BY composite_score DESC;
```

The reporting layer is now ready for development and analysis! 🚀 
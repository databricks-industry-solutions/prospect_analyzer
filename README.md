# AI-Powered Prospecting Tool on Databricks

**Build your own smart prospect research system in under 30 minutes**

<img src="https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png" width="600px">

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue)]()
[![POC](https://img.shields.io/badge/POC-5_days-green)]()

## What This Does

Transform your prospect research from manual work into an automated AI system that:

✅ **Analyzes company websites** automatically  
✅ **Scores prospects** based on your business criteria  
✅ **Writes personalized emails** for each company  
✅ **Creates interactive dashboards** to track results  

All built on Databricks with no external tools needed.

![Dashboard Overview](images/Dashboard%20showing%20analyzed%20prospects%20with%20scores.png)
*Interactive dashboard showing analyzed prospects with AI-generated scores and insights*

## Why Build Your Own?

- **Full Control**: Your data stays in your workspace
- **Unlimited Use**: No per-prospect or monthly fees  
- **Custom Analysis**: AI tailored to your specific business
- **Easy Integration**: Connects to your existing CRM
- **Learn Databricks**: Master platform skills you can use everywhere

## Example: The Brick Store

See how a brick supplier uses this to find construction companies that need building materials. The AI analyzes potential customers and creates personalized outreach campaigns.

**Works for any B2B business**: Software companies, manufacturers, consultants, or any business that needs smart prospect research.

![Personalized Email Example](images/Example%20personalized%20email%20showing%20AI%20insights.png)
*Example AI-generated personalized email showing specific business insights*

## Quick Setup (30 minutes)

### Step 1: Import the Code
1. In Databricks: **Workspace** → **Create** → **Git repository**
2. Enter: `https://github.com/john-neil_data/prospect_analyzer`  
3. Click **Create**

### Step 2: Update Your Settings  
1. Open the `databricks.yml` file
2. Find `# CHANGE THIS:` (10 spots to update)
3. Add your workspace URL, email, and database names

### Step 3: Deploy Everything
1. Click the **Deployments** tab (rocket icon)
2. Choose `dev` 
3. Click **Deploy**

Almost Done! The system creates all tables, workflows, and dashboards automatically.

![Deployment Success](images/Successful%20deployment%20message.png)
*Successful DAB deployment showing all resources created*

### Step 4: Run the Pipeline
1. Navigate to **Jobs & Pipelines** and find your newly deployed pipeline
2. Press **Run Now** and let the pipeline complete

![Lakeflow Job UI](images/Lakeflow%20Job%20UI%20where%20Job%20is%20Run.png)
*Lakeflow Job UI showing the pipeline execution interface*

### Step 4: Final Dashboard Set-Up
1. Navigate to **Dashboards** and find your **Brickstore Prospect Analyzer - AI-Powered Outreach Dashboard**
2. Please select `Edit Draft` and update the `# CHANGE THIS:` line in the SQL Code. The default tables should be `catalog.schema.` and need to be updated.
3. Once completed, you can refresh the dashboard and **Publish**!

![Updating AI/BI Dashboard](images/Updating%20Dashboard.png)
*Update the backend SQL in the Data tab of the Dashboard*


## What You Get

### Smart Company Analysis
AI reads company websites and tells you:
- What industry they're in and company size
- What technology they use  
- Who makes decisions
- How good a prospect they are (scored 0-100)

### Personalized Email Campaigns
AI writes custom emails mentioning:
- Specific details about their business
- Relevant challenges they might face
- How your product helps their situation
- Professional, engaging tone

### Business Dashboard  
Interactive charts showing:
- Top-ranked prospects ready to contact
- Industry breakdowns and trends
- Campaign performance tracking
- Export lists for your CRM

## Easy to Customize

**For Different Industries:**
- Manufacturing: Find suppliers and production partners
- Software: Identify companies needing digital solutions  
- Consulting: Discover businesses with operational challenges

**Change What Matters to You:**
- Adjust scoring to match your ideal customer
- Modify email tone and messaging
- Add your own prospect criteria

## Requirements

- Databricks workspace with Unity Catalog
- SQL Warehouse running
- 30 minutes of your time

No external APIs or additional software needed.

## Get Started

1. **Try the demo** with the included Brick Store example
2. **Add your prospect list** (we provide the template)
3. **Customize for your business** (simple configuration changes)
4. **Scale up** to thousands of prospects

## Need Help?

- Check the troubleshooting section if something doesn't work
- File issues on GitHub for technical problems  
- All code includes detailed comments and examples

**Common Issues:**
- Make sure all `# CHANGE THIS:` values are updated
- Verify you have permissions for the database you chose
- Confirm your SQL Warehouse is running

---

# 📖 Technical Reference & Advanced Details

*For developers, data engineers, and technical implementers who want the full details*

## Technical Architecture

### AI Models & Processing
- **Foundation Model**: `databricks-meta-llama-3-3-70b-instruct` via Databricks Mosaic AI
- **Structured Outputs**: Schema-enforced JSON responses for consistency
- **Batch Processing**: Configurable batch sizes (default ~75 prospects per run)
- **Content Limits**: Processes up to 10,000 characters per website
- **No External APIs**: Uses Databricks managed AI services exclusively

### Data Pipeline Details
- **Web Scraping**: Python requests/BeautifulSoup for content extraction
- **Error Handling**: Skips failed scrapes, continues processing, respects robots.txt
- **Incremental Processing**: Only analyzes new/unprocessed prospects using Delta Lake
- **Storage**: All data in Unity Catalog with full governance and lineage

### Scale & Performance
- **Processing Capacity**: Handles hundreds to thousands of prospects per run
- **Parallel Execution**: Databricks auto-scaling for concurrent analysis
- **Resource Efficiency**: Optimized for cost-effective large-scale inference
- **Delta Optimization**: Fast incremental updates and queries

## Data Architecture

| Table Name | Type | Purpose | Key Columns |
|------------|------|---------|-------------|
| `prospect_raw_data` | Table | Original scraped data | `company_name`, `website_url`, `raw_content` |
| `v_prospect_analysis` | View | AI-analyzed summaries | `company_name`, `business_summary`, `ai_insights` |
| `v_email_campaigns` | View | Email templates | `company_name`, `personalized_subject`, `email_body` |
| `prospect_scores` | Table | Scored prospects | `company_name`, `fit_score`, `recommended_action` |

## AI Implementation Examples

### Batch Inference Pattern
```sql
SELECT 
  company_name,
  ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT('Analyze this company: ', website_content, 
           'Return JSON: {"industry": "", "size": "", "score": 0-100}')
  ) as analysis
FROM prospect_raw_data;
```

### Custom Scoring Logic
```python
def calculate_prospect_score(analysis_json):
    """Industry-specific scoring algorithm"""
    data = json.loads(analysis_json)
    
    # Weighted scoring components
    industry_fit = score_industry_match(data['industry']) * 0.4
    company_size = score_size_indicators(data['size']) * 0.3  
    tech_readiness = score_technology_adoption(data['tech']) * 0.3
    
    return industry_fit + company_size + tech_readiness
```

## Detailed Configuration

### Complete databricks.yml Reference
```yaml
bundle:
  name: prospect_analyzer

variables:
  workspace_url:
    description: "Databricks workspace URL"
    default: "https://your-workspace.cloud.databricks.com"

  catalog:
    description: "Unity Catalog name"
    default: "main"

  schema:
    description: "Schema for prospect data"
    default: "prospect_analyzer"

  notification_email:
    description: "Email for job notifications"
    default: "your-email@company.com"

  warehouse_id:
    description: "SQL Warehouse ID for queries"
    default: "your-warehouse-id"

  batch_size:
    description: "Prospects per batch"
    default: 75

  content_limit:
    description: "Max characters per website"
    default: 10000
```

## Industry Customization Examples

### Manufacturing Focus
```python
# Modify for manufacturing prospects
manufacturing_prompts = {
    'industry_analysis': 'Focus on production capabilities, supply chain needs, manufacturing technology adoption',
    'scoring_criteria': 'Prioritize: factory operations, industrial equipment usage, procurement processes',
    'email_tone': 'Professional, ROI-focused, emphasize operational efficiency'
}
```

### SaaS Business Focus  
```python
# Modify for software prospects
saas_prompts = {
    'industry_analysis': 'Focus on digital transformation, technology stack, IT decision makers',
    'scoring_criteria': 'Prioritize: technology sophistication, growth indicators, digital initiatives', 
    'email_tone': 'Innovation-focused, emphasize competitive advantage and scalability'
}
```

## Advanced Workflow Orchestration

### Pipeline Steps Detail
1. **Data Ingestion** (5-10 minutes)
   - Load prospect URLs and basic info
   - Validate data quality and format
   - Set up incremental processing markers

2. **Web Scraping** (15-30 minutes for 75 prospects)
   - Parallel content extraction
   - HTML cleaning and normalization  
   - Error handling and retry logic

3. **AI Analysis** (20-45 minutes for 75 prospects)
   - Batch LLM inference calls
   - Structured JSON output parsing
   - Quality validation of AI responses

4. **Scoring & Ranking** (5 minutes)
   - Apply custom scoring algorithms
   - Generate prospect rankings
   - Create recommended actions

5. **Email Generation** (10-15 minutes)
   - Personalized campaign creation
   - Template customization by prospect
   - Output formatting for CRM import

### Error Handling Patterns
```python
def process_prospect_with_retry(prospect_data, max_retries=3):
    """Robust processing with fallback strategies"""
    for attempt in range(max_retries):
        try:
            # Attempt web scraping
            content = scrape_website(prospect_data.url)
            
            # Attempt AI analysis
            analysis = ai_query(content)
            
            return analysis
            
        except WebScrapingError:
            # Fallback to cached data or skip
            continue
        except AIServiceError:
            # Use simplified analysis or queue for retry
            continue
    
    # Log failure and continue with next prospect
    log_processing_failure(prospect_data)
```

## Performance Optimization

### Batch Size Tuning
- **Small datasets (< 100 prospects)**: Batch size 25-50
- **Medium datasets (100-1000 prospects)**: Batch size 75-100  
- **Large datasets (> 1000 prospects)**: Batch size 100-200

### Resource Management
```python
# Optimize Spark configuration for AI workloads
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

## Integration Patterns

### CRM Integration Example
```python
def export_to_crm(scored_prospects):
    """Export results to external CRM system"""
    crm_format = scored_prospects.select(
        col("company_name").alias("Account_Name"),
        col("prospect_score").alias("Lead_Score"),
        col("ai_insights").alias("Notes"),
        col("recommended_action").alias("Next_Steps")
    )
    
    # Write to CRM-compatible format
    crm_format.write.format("csv").save("crm_export.csv")
```

### Slack Notifications
```python
def send_pipeline_notification(status, prospect_count):
    """Notify team of pipeline completion"""
    message = f"🎯 Prospect Analysis Complete!\n"
    message += f"📊 Analyzed: {prospect_count} companies\n" 
    message += f"✅ Status: {status}\n"
    message += f"📈 Dashboard: [View Results](dashboard_url)"
    
    # Send to Slack webhook
    send_slack_message(message)
```

## Monitoring & Observability

### Key Metrics to Track
- **Processing Rate**: Prospects analyzed per hour
- **AI Response Quality**: JSON parsing success rate  
- **Web Scraping Success**: Content extraction percentage
- **Scoring Distribution**: Prospect score histograms
- **Pipeline Runtime**: End-to-end execution time

### Dashboard Queries
```sql
-- Pipeline performance monitoring
SELECT 
  DATE(start_time) as run_date,
  COUNT(*) as prospects_processed,
  AVG(processing_time_seconds) as avg_processing_time,
  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*) as success_rate
FROM pipeline_execution_log
GROUP BY DATE(start_time)
ORDER BY run_date DESC;

-- Prospect quality analysis  
SELECT 
  CASE 
    WHEN prospect_score >= 80 THEN 'High Priority'
    WHEN prospect_score >= 60 THEN 'Medium Priority'
    ELSE 'Low Priority'
  END as priority_tier,
  COUNT(*) as prospect_count,
  AVG(prospect_score) as avg_score
FROM prospect_scores
GROUP BY priority_tier;
```

## Security & Compliance

### Data Privacy Considerations
- **Data Residency**: All prospect data remains in your Databricks workspace
- **Access Controls**: Unity Catalog provides granular permissions
- **Audit Trails**: Complete lineage tracking for compliance
- **Data Retention**: Configurable cleanup policies for GDPR compliance

### Web Scraping Ethics
- **Robots.txt Compliance**: Respects website scraping policies
- **Rate Limiting**: Built-in delays to avoid overwhelming target sites
- **Content Usage**: Analysis only, no content republication
- **Legal Responsibility**: Users ensure compliance with local regulations

## Troubleshooting Guide

### Deployment Issues
```bash
# Common DAB deployment problems
databricks bundle validate  # Check configuration syntax
databricks bundle deploy --verbose  # Detailed deployment logs
```

### Pipeline Failures
```sql
-- Debug failed prospect processing
SELECT 
  company_name,
  error_message,
  retry_count,
  last_attempt_time
FROM processing_errors
ORDER BY last_attempt_time DESC;
```

### Performance Problems
```sql
-- Identify slow-processing prospects
SELECT 
  company_name,
  processing_time_seconds,
  content_size_chars,
  ai_response_time_seconds
FROM processing_metrics
WHERE processing_time_seconds > 300
ORDER BY processing_time_seconds DESC;
```

---

**This project teaches you how to build AI-powered business applications on Databricks. The prospecting example is fully functional and customizable for any industry.**

© 2025 Databricks, Inc. All rights reserved.
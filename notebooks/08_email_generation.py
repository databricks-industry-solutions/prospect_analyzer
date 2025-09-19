# Databricks notebook source
# MAGIC %md
# MAGIC # Email Generation for Prospect Outreach
# MAGIC 
# MAGIC **Brickstore's AI-powered email generation system** - Creates personalized outreach emails for construction industry prospects using AI analysis of their business profiles.
# MAGIC 
# MAGIC ## Business Value
# MAGIC - **Automated Personalization**: Generates tailored emails based on AI analysis of prospect business profiles
# MAGIC - **Consistent Messaging**: Ensures all outreach follows Brickstore's proven email templates
# MAGIC - **Scalable Outreach**: Processes hundreds of prospects simultaneously
# MAGIC - **Time Savings**: Reduces manual email creation from hours to minutes
# MAGIC 
# MAGIC ## Pipeline Stage
# MAGIC This notebook runs after prospect scoring and creates three types of outreach emails:
# MAGIC 1. **Initial Outreach**: First contact with value proposition
# MAGIC 2. **Use Case Highlight**: Follow-up with specific use cases and strong CTA
# MAGIC 3. **Light Touch**: Final reminder with concrete value example

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Environment

# COMMAND ----------

# MAGIC %run ./_resources/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Campaign Emails with AI
# MAGIC 
# MAGIC Uses `ai_query` to generate three personalized email templates for each prospect based on their AI-analyzed business profile.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create campaign emails table with AI-generated content
# MAGIC CREATE OR REPLACE TABLE ${catalog}.${schema}.campaign_emails AS
# MAGIC SELECT
# MAGIC   ps.id,
# MAGIC   ps.website,
# MAGIC   ps.ai_business_summary,
# MAGIC   ps.is_brick_company,
# MAGIC   ps.company_type,
# MAGIC   ps.company_stage,
# MAGIC   ps.composite_score,
# MAGIC   ps.score_tier,
# MAGIC   ps.scoring_timestamp,
# MAGIC   
# MAGIC   -- Email 1: Initial Outreach
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT(
# MAGIC       'Create an initial outreach email for the account: ', ps.website, 
# MAGIC       ' with the following company summary: ', SUBSTRING(LOWER(regexp_replace(ps.ai_business_summary, '[^a-zA-Z\\s]', '')), 1, 10000),
# MAGIC       ' The email MUST strictly follow this exact template format:',
# MAGIC       '**Email 1: Initial Outreach**',
# MAGIC       'Subject Line: ', ps.website, '-Specific Opportunity to Enhance Operations with Premium Brick Solutions',
# MAGIC       'Hi [First Name],',
# MAGIC       ps.website, ' is doing incredible work in [specific area of focus]. Many organizations in [industry/sector] are now adopting premium brick solutions to unlock operational efficiencies and deliver even greater value to their customers.',
# MAGIC       'With BrickStore''s comprehensive brick platform, you could:',
# MAGIC       '* [Benefit 1]: [Specific value aligned with their goals].',
# MAGIC       '* [Benefit 2]: [Specific value aligned with their goals].',
# MAGIC       '* [Benefit 3]: [Specific value aligned with their goals].',
# MAGIC       'Would you be open to a quick 15-minute conversation this week to explore how this could fit into your construction workflows? I can share examples of how other companies in your space are achieving [key outcomes].',
# MAGIC       'Best regards, [Your Name] [Your Title] BrickStore',
# MAGIC       ' Instructions:',
# MAGIC       ' 1. Fill in placeholders with content specific to ', ps.website, ' and their business needs based ONLY on the provided company summary.',
# MAGIC       ' 2. For [First Name], leave it as a placeholder with "[First Name]" - do not make up a name.',
# MAGIC       ' 3. For [Your Name] and [Your Title], leave them as placeholders with "[Your Name]" and "[Your Title]".',
# MAGIC       ' 4. For [specific area of focus] and [industry/sector], identify these from the company summary. If not clear, use general terms like "your industry" rather than inventing specifics.',
# MAGIC       ' 5. For [key outcomes], use general terms related to the three benefits you chose.',
# MAGIC       ' Use these brick solutions to inform the benefits:',
# MAGIC       ' 1. Premium Clay Brick Manufacturing: High-quality clay bricks for residential and commercial construction',
# MAGIC       ' 2. Custom Brick Design & Color Matching: Specialized bricks for architectural and restoration projects',
# MAGIC       ' 3. Eco-Friendly Brick Solutions: Sustainable brick options made from recycled materials',
# MAGIC       ' 4. Fire-Resistant Brick Products: Specialized bricks for industrial and safety applications',
# MAGIC       ' 5. Decorative & Architectural Bricks: Unique designs for premium construction projects',
# MAGIC       ' 6. Bulk Brick Supply & Distribution: Large-scale brick delivery for major construction projects',
# MAGIC       ' 7. Specialty Mortar & Installation Solutions: Complete brick installation systems and support',
# MAGIC       ' 8. Weatherproof Brick Coatings: Advanced protective coatings for enhanced durability',
# MAGIC       ' 9. Reclaimed & Vintage Brick Sourcing: Authentic reclaimed bricks for restoration projects',
# MAGIC       ' 10. Brick Repair & Restoration Services: Professional services for maintaining brick structures',
# MAGIC       ' Choose 3 benefits that align closest with their business model as indicated in the company summary.'
# MAGIC     )
# MAGIC   ) AS email_1_initial_outreach,
# MAGIC   
# MAGIC   -- Email 2: Use Case Highlight with Strong CTA
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT(
# MAGIC       'Create a follow-up email for the account: ', ps.website, 
# MAGIC       ' with the following company summary: ', SUBSTRING(LOWER(regexp_replace(ps.ai_business_summary, '[^a-zA-Z\\s]', '')), 1, 10000),
# MAGIC       ' The email MUST strictly follow this exact template format:',
# MAGIC       '**Email 2: Use Case Highlight with Strong CTA**',
# MAGIC       'Subject Line: Unlock New Construction Possibilities for ', ps.website, ' with Premium Brick Solutions',
# MAGIC       'Hi [First Name],',
# MAGIC       'Imagine if ', ps.website, ' could [specific improvement or outcome tied to their business goals]. With BrickStore''s comprehensive brick platform, companies like yours are already achieving:',
# MAGIC       '* [Use Case 1]: [Brief description of how it solves a problem or creates value].',
# MAGIC       '* [Use Case 2]: [Brief description of another relevant use case].',
# MAGIC       '* [Use Case 3]: [Brief description of another relevant use case].',
# MAGIC       'Here''s a quick resource that dives deeper into these capabilities: [Insert link to demo, catalog, or case study].',
# MAGIC       'Let''s schedule time this week for a tailored walkthrough of how this could work for your projects. Does Thursday at 2:00 PM work? If not, feel free to suggest a time that fits your schedule.',
# MAGIC       'Looking forward to connecting!',
# MAGIC       'Best regards, [Your Name]',
# MAGIC       ' Instructions:',
# MAGIC       ' 1. Fill in placeholders with content specific to ', ps.website, ' and their business needs based ONLY on the provided company summary.',
# MAGIC       ' 2. For [First Name], leave it as a placeholder with "[First Name]" - do not make up a name.',
# MAGIC       ' 3. For [Your Name], leave it as a placeholder with "[Your Name]".',
# MAGIC       ' 4. For [specific improvement or outcome tied to their business goals], identify a realistic goal based on the company summary.',
# MAGIC       ' 5. Keep "[Insert link to demo, catalog, or case study]" as is.',
# MAGIC       ' Use these brick solutions to inform the use cases section:',
# MAGIC       ' 1. Premium Clay Brick Manufacturing: High-quality clay bricks for residential and commercial construction',
# MAGIC       ' 2. Custom Brick Design & Color Matching: Specialized bricks for architectural and restoration projects',
# MAGIC       ' 3. Eco-Friendly Brick Solutions: Sustainable brick options made from recycled materials',
# MAGIC       ' 4. Fire-Resistant Brick Products: Specialized bricks for industrial and safety applications',
# MAGIC       ' 5. Decorative & Architectural Bricks: Unique designs for premium construction projects',
# MAGIC       ' 6. Bulk Brick Supply & Distribution: Large-scale brick delivery for major construction projects',
# MAGIC       ' 7. Specialty Mortar & Installation Solutions: Complete brick installation systems and support',
# MAGIC       ' 8. Weatherproof Brick Coatings: Advanced protective coatings for enhanced durability',
# MAGIC       ' 9. Reclaimed & Vintage Brick Sourcing: Authentic reclaimed bricks for restoration projects',
# MAGIC       ' 10. Brick Repair & Restoration Services: Professional services for maintaining brick structures',
# MAGIC       ' Choose 3 use cases that align closest with their business model as indicated in the company summary.'
# MAGIC     )
# MAGIC   ) AS email_2_use_case_highlight,
# MAGIC   
# MAGIC   -- Email 3: Light Touch with Value Reminder
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-1-8b-instruct',
# MAGIC     CONCAT(
# MAGIC       'Create a light touch follow-up email for the account: ', ps.website, 
# MAGIC       ' with the following company summary: ', SUBSTRING(LOWER(regexp_replace(ps.ai_business_summary, '[^a-zA-Z\\s]', '')), 1, 10000),
# MAGIC       ' The email MUST strictly follow this exact template format:',
# MAGIC       '**Email 3 Light Touch with Value Reminder**',
# MAGIC       'Subject Line: A Quick Idea for Scaling Your Construction Success with Premium Bricks',
# MAGIC       'Hi [First Name],',
# MAGIC       'I was thinking about how companies like yours are using premium brick solutions to solve challenges like scaling construction operations, improving project quality, and reducing material costs. Here''s one idea that might resonate with your team:',
# MAGIC       'By leveraging BrickStore''s comprehensive brick platform, you could seamlessly source premium materials while maintaining quality standards and project timelines—no additional sourcing infrastructure required. This approach has helped companies reduce material costs by up to X% while increasing project efficiency.',
# MAGIC       'If this sounds interesting, I''d be happy to share more details or set up a time for a quick discussion. Just let me know!',
# MAGIC       'Best regards, [Your Name]',
# MAGIC       ' Instructions:',
# MAGIC       ' 1. Fill in placeholders with content specific to ', ps.website, ' and their business needs based ONLY on the provided company summary.',
# MAGIC       ' 2. For [First Name], leave it as a placeholder with "[First Name]" - do not make up a name.',
# MAGIC       ' 3. For [Your Name], leave it as a placeholder with "[Your Name]".',
# MAGIC       ' 4. Between the two paragraphs, add ONE specific paragraph with an example of how premium brick solutions could help this specific company based on their company summary.',
# MAGIC       ' 5. Replace X% with a specific percentage between 15-30%.',
# MAGIC       ' Specifically choose one very concrete example from these brick solutions:',
# MAGIC       ' 1. Premium Clay Brick Manufacturing: High-quality clay bricks for residential and commercial construction',
# MAGIC       ' 2. Custom Brick Design & Color Matching: Specialized bricks for architectural and restoration projects',
# MAGIC       ' 3. Eco-Friendly Brick Solutions: Sustainable brick options made from recycled materials',
# MAGIC       ' 4. Fire-Resistant Brick Products: Specialized bricks for industrial and safety applications',
# MAGIC       ' 5. Decorative & Architectural Bricks: Unique designs for premium construction projects',
# MAGIC       ' 6. Bulk Brick Supply & Distribution: Large-scale brick delivery for major construction projects',
# MAGIC       ' 7. Specialty Mortar & Installation Solutions: Complete brick installation systems and support',
# MAGIC       ' 8. Weatherproof Brick Coatings: Advanced protective coatings for enhanced durability',
# MAGIC       ' 9. Reclaimed & Vintage Brick Sourcing: Authentic reclaimed bricks for restoration projects',
# MAGIC       ' 10. Brick Repair & Restoration Services: Professional services for maintaining brick structures',
# MAGIC       ' Choose 1 single idea that aligns closest with their business model as indicated in the company summary.'
# MAGIC     )
# MAGIC   ) AS email_3_light_touch,
# MAGIC   
# MAGIC   current_timestamp() as processed_at
# MAGIC 
# MAGIC FROM ${catalog}.${schema}.gold_scored_prospects ps
# MAGIC WHERE ps.ai_business_summary IS NOT NULL
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Email Generation Results
# MAGIC 
# MAGIC The AI has generated three personalized email templates for each prospect:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display generated emails
# MAGIC SELECT 
# MAGIC   website,
# MAGIC   score_tier,
# MAGIC   composite_score,
# MAGIC   SUBSTRING(email_1_initial_outreach, 1, 200) as email_1_preview,
# MAGIC   SUBSTRING(email_2_use_case_highlight, 1, 200) as email_2_preview,
# MAGIC   SUBSTRING(email_3_light_touch, 1, 200) as email_3_preview,
# MAGIC   processed_at
# MAGIC FROM ${catalog}.${schema}.campaign_emails
# MAGIC ORDER BY composite_score DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Email Generation Summary
# MAGIC 
# MAGIC ✅ **Successfully generated personalized emails for prospects**
# MAGIC 
# MAGIC **Generated Email Types:**
# MAGIC - **Initial Outreach**: First contact with value proposition and benefits
# MAGIC - **Use Case Highlight**: Follow-up with specific use cases and strong call-to-action
# MAGIC - **Light Touch**: Final reminder with concrete value example
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - **AI-Powered Personalization**: Each email is tailored to the prospect's business profile
# MAGIC - **Consistent Templates**: All emails follow Brickstore's proven outreach format
# MAGIC - **Brick Industry Focus**: Content specifically addresses construction and brick supply needs
# MAGIC - **Scalable Processing**: Handles multiple prospects simultaneously
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC - Review generated emails in the `campaign_emails` table
# MAGIC - Export emails for sales team use
# MAGIC - Track email performance and response rates
# MAGIC 
# MAGIC The email generation pipeline is now complete and ready for production use! 
# Harvey Analytics Take-Home Project

## Overview
Analysis of user engagement and usage patterns for Harvey's legal AI platform, used by thousands of elite lawyers for drafting, summarizing, researching, and performing other legal tasks. These models help define and measure healthy usage patterns across different legal workflows, providing scalable analytics to understand how lawyers are integrating AI into their practice.

## Project Structure
- `/analyses`: SQL queries for data quality checks and metric analysis
- `/seeds`: Source data files
- `/models/base`: Cleansed source tables (base_events, base_users, base_firms)
- `/models/marts`: Core analytical models (user_engagement, firm_usage_summary, cohort_analysis)

## Assumptions
Legal AI Usage:
- Higher feedback scores (4-5) indicate successful AI assistance with legal tasks
- Users leveraging multiple features (ASSISTANT, VAULT, WORKFLOW) are more valuable as they're integrating AI across their legal workflow
- Document volume indicates more complex queries
- Firm size impacts expected usage patterns (larger firms likely have more diverse use cases)
- Active usage is more important than just having access (common in legal tech where tools are firm-wide licensed but adoption varies)
- Features are accessible by all users and not gated

## Models

### user_engagement
This table tracks user activity and engagement metrics over time. It contains one row per user per month starting from their creation date, including months with no activity. Use this table to monitor user adoption, identify power users, and spot engagement trends.

Key metrics:
- Query counts by feature (ASSISTANT, VAULT, WORKFLOW)
- Days active per month
- Documents processed
- Feedback scores
- Engagement level

Engagement levels are defined as:
- Power User: ≥4 active days AND ≥5 queries AND ≥45 documents
- High: ≥5 queries AND ≥45 documents
- Medium: >2 queries OR >12 documents
- Low: ≥1 active day AND (≥1 query OR ≥1 document)
- Inactive: No activity
- 
Engagement thresholds were derived by analyzing actual user activity distributions across queries, documents, and active days. See analyses/power_user_analysis.sql for full logic and justification.

### firm_usage_summary
This table provides a single-row summary for each firm's overall platform usage. Use it to assess firm health, compare adoption across firms, and identify firms needing attention.

Key metrics:
- Total active users
- Last month's active users
- Query volumes by feature
- Engagement rate (active users / firm size)
- Satisfaction rate (high feedback queries / total queries)

### cohort_analysis
This table groups users by their start month to track retention and usage patterns over time. Use it to evaluate onboarding effectiveness and long-term engagement trends.

Key metrics:
- Monthly retention rates
- Queries per retained user
- Documents processed per cohort
- Feature adoption by cohort age

## Data Quality Notes (queries in data_analysis_and_quality)
- 2 firms show zero activity since provisioning
- 1 user appears in multiple firms
- Only 12.1% of Active users achieve Power User status (507 out of 4709)
- 5 days show unusual activity spikes
- 3 instances of unusual feedback patterns found 



## How to Interpret Models and Metrics

### User Activity & Trust Indicators
- **Query Patterns**: Users progressing from simple (1 doc) to complex queries (10+ docs) suggest they're leveraging the AI for more sophisticated legal research tasks.
- **Feature Mix**: Users engaging with multiple features (ASSISTANT, VAULT, WORKFLOW) are likely integrating the platform into their daily legal workflow rather than using it as a simple document search tool.

### Engagement Levels Meaning
- **Power Users**: These lawyers have fully integrated Harvey into their practice, using it frequently (4+ days/month) for substantial document analysis (45+ docs). They're your platform champions.
- **High Engagement**: Regular users who trust the platform with significant document volumes but might not use it as frequently. Focus on increasing their active days.
- **Medium/Low Engagement**: May be using Harvey for specific types of matters only. Opportunity to demonstrate value in other practice areas.
- **Inactive**: Could be seasonal users (e.g., litigation teams between cases) or need additional training on platform capabilities.

### Firm-Level Health Indicators
- **Engagement Rate**: Low rates in large firms often indicate adoption limited to specific practice groups - opportunity for cross-department expansion.
- **Satisfaction Rate**: High feedback scores on document-heavy queries validate the AI's accuracy for legal research tasks.
- **Feature Distribution**: Firms heavily using VAULT might need education on WORKFLOW features for end-to-end matter management.

### Cohort Analysis Application
- **Early Usage**: First month patterns often reflect initial training effectiveness. High early document volumes suggest successful onboarding.
- **Long-term Trends**: Growing document volumes per user over time indicate increasing trust and reliance on the platform.
- **Retention Patterns**: Practice area seasonality may explain usage fluctuations. Focus on maintaining engagement between major matters.

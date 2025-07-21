# Harvey Analytics Take-Home Project

## Overview
Analysis of user engagement and usage patterns for Harvey's legal AI platform, used by thousands of elite lawyers for drafting, summarizing, researching, and performing other legal tasks. These models help define and measure healthy usage patterns across different legal workflows, providing scalable analytics to understand how lawyers are integrating AI into their practice.

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
A monthly snapshot of user activity and engagement metrics. Creates one row per user per month since their creation date, including months with no activity.

**Structure:**
- One row per user per month (starting from user creation date)
- Inactive months are included with metrics set to 0
- Engagement levels are calculated based on monthly activity

**Key Metrics:**
- Query counts (total, by type)
- Active days per month
- Document processing metrics
- Feedback scores
- Feature usage breakdown

**Engagement Level Criteria:**
- Power User:
  - ≥ 4 active days AND
  - ≥ 5 queries AND
  - ≥ 45 documents processed
- High Engagement:
  - ≥ 5 queries AND
  - ≥ 45 documents processed
- Medium Engagement:
  - > 2 queries OR
  - > 12 documents processed
- Low Engagement:
  - ≥ 1 active day AND
  - (≥ 1 query OR ≥ 1 document processed)
- Inactive:
  - No activity in the month

### firm_usage_summary
A summary of firm-level engagement and usage metrics. One row per firm showing both all-time and recent activity.

**Key Metrics:**
- Active users (all-time and last month)
- Query volumes by type
- Document processing totals
- Feedback and satisfaction metrics
- Engagement rate (% of firm size active)

### cohort_analysis
Analyzes user retention and engagement patterns by cohort and event type.

**Key Metrics:**
- Cohort retention rates
- Activity levels over time
- Feature adoption patterns
- Document processing trends

## Assumptions & Data Quality Notes
1. Users are considered active in a month if they have at least one event
2. Feedback scores range from 1-5, with 5 being most positive
3. Complex queries are defined as those processing ≥ 10 documents
4. Engagement levels are calculated on a monthly basis
5. A month of activity is based on event_month
6. All metrics for inactive months are set to 0 rather than null
7. User metrics start from their created_date

## Usage
The models are materialized as follows:
- Base models (base_events, base_users, base_firms) → views
- Analytical models (user_engagement, firm_usage_summary, cohort_analysis) → tables

This ensures optimal query performance while maintaining data freshness.

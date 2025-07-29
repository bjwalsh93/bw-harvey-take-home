{{
    config(
        materialized='table'
    )
}}

select
    user_id,
    firm_id,
    event_month,
    count(*) as query_count,
    max(event_timestamp) as last_active_date,
    min(event_timestamp) as first_active_date,
    count(distinct event_date) as days_active,
    count(distinct event_type) as unique_query_types,
    avg(feedback_score) as avg_feedback_score,
    -- Event type breakdowns
    count(case when event_type = 'ASSISTANT' then 1 end) as assistant_queries,
    count(case when event_type = 'VAULT' then 1 end) as vault_queries,
    count(case when event_type = 'WORKFLOW' then 1 end) as workflow_queries,
    -- Feedback analysis
    count(case when feedback_score >= 4 then 1 end) as high_feedback_queries,
    count(case when feedback_score <= 2 then 1 end) as low_feedback_queries,
    -- Document analysis
    sum(num_docs) as total_docs,
    avg(num_docs) as avg_docs_per_query,
    max(num_docs) as max_docs_in_single_query,
    count(case when num_docs >= 10 then 1 end) as complex_queries,
    count(case when num_docs = 1 then 1 end) as simple_queries,
    -- Engagement level definition
    case
        when count(distinct event_date) >= 4 
         and count(*) >= 5 
         and sum(num_docs) >= 45 then 'Power User'
        when count(*) >= 5 
         and sum(num_docs) >= 45 then 'High Engagement'
        when count(*) > 2 
         or sum(num_docs) > 12 then 'Medium Engagement'
        when count(distinct event_date) >= 1 
         and (count(*) >= 1 or sum(num_docs) >= 1) then 'Low Engagement'
        else 'Inactive'
    end as engagement_level
from {{ ref('base_events') }}
group by user_id, firm_id, event_month 
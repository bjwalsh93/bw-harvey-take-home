{{
    config(
        materialized='table'
    )
}}

with user_monthly_events as (
    select
        user_id,
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
        -- New engagement level definition
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
    group by user_id, event_month
),
all_months as (
    select distinct event_month as month
    from {{ ref('base_events') }}
)
select
    u.user_id,
    u.role,
    m.month,
    coalesce(e.query_count, 0) as query_count,
    coalesce(e.days_active, 0) as days_active,
    coalesce(e.unique_query_types, 0) as unique_query_types,
    e.avg_feedback_score,
    e.first_active_date,
    e.last_active_date,
    coalesce(e.engagement_level, 'Inactive') as engagement_level,
    -- Event type metrics
    coalesce(e.assistant_queries, 0) as assistant_queries,
    coalesce(e.vault_queries, 0) as vault_queries,
    coalesce(e.workflow_queries, 0) as workflow_queries,
    -- Feedback metrics
    coalesce(e.high_feedback_queries, 0) as high_feedback_queries,
    coalesce(e.low_feedback_queries, 0) as low_feedback_queries,
    coalesce(round(e.high_feedback_queries::float / nullif(e.query_count, 0)::float * 100, 2), 0) as satisfaction_rate,
    -- Document analysis
    coalesce(e.total_docs, 0) as total_docs,
    coalesce(e.avg_docs_per_query, 0) as avg_docs_per_query,
    coalesce(e.max_docs_in_single_query, 0) as max_docs_in_single_query,
    coalesce(e.complex_queries, 0) as complex_queries,
    coalesce(e.simple_queries, 0) as simple_queries,
    coalesce(round(e.complex_queries::float / nullif(e.query_count, 0)::float * 100, 2), 0) as complexity_rate
from {{ ref('base_users') }} u
cross join all_months m
left join user_monthly_events e 
    on u.user_id = e.user_id 
    and m.month = e.event_month
where date_trunc('month', u.created_date) <= m.month 
{{
    config(
        materialized='table'
    )
}}

with all_months as (
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
left join {{ ref('int_user_monthly_engagement') }} e 
    on u.user_id = e.user_id 
    and m.month = e.event_month
where date_trunc('month', u.created_date) <= m.month 
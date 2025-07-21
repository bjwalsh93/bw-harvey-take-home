{{
    config(
        materialized='table'
    )
}}

with firm_users as (
    select
        firm_id,
        count(distinct user_id) as active_users_all_time
    from {{ ref('base_events') }}
    group by firm_id
),
user_activity as (
    select
        e.firm_id,
        e.user_id,
        max(e.event_timestamp) as last_active_date,
        count(*) as user_query_count,
        count(case when e.event_type = 'ASSISTANT' then 1 end) as assistant_queries,
        count(case when e.event_type = 'VAULT' then 1 end) as vault_queries,
        count(case when e.event_type = 'WORKFLOW' then 1 end) as workflow_queries,
        avg(e.feedback_score) as avg_feedback_score,
        count(case when e.feedback_score >= 4 then 1 end) as high_feedback_queries,
        sum(e.num_docs) as total_docs_processed
    from {{ ref('base_events') }} e
    group by e.firm_id, e.user_id
),
firm_activity as (
    select
        firm_id,
        count(distinct user_id) as active_users,
        sum(user_query_count) as total_queries,
        max(last_active_date) as last_active_date,
        sum(assistant_queries) as total_assistant_queries,
        sum(vault_queries) as total_vault_queries,
        sum(workflow_queries) as total_workflow_queries,
        avg(avg_feedback_score) as avg_firm_feedback_score,
        sum(high_feedback_queries) as total_high_feedback_queries,
        sum(total_docs_processed) as total_docs_processed
    from user_activity
    where last_active_date >= (select date_trunc('month', max(event_timestamp)) - interval '1 month' from {{ ref('base_events') }})
    group by firm_id
)
select
    f.firm_id,
    f.firm_name,
    f.created_date as join_date,
    f.arr,
    f.firm_size,
    fu.active_users_all_time,
    fa.active_users as active_users_last_month,
    fa.total_queries,
    fa.last_active_date,
    fa.total_assistant_queries,
    fa.total_vault_queries,
    fa.total_workflow_queries,
    fa.avg_firm_feedback_score,
    fa.total_high_feedback_queries,
    round(fa.total_high_feedback_queries::float / nullif(fa.total_queries, 0)::float * 100, 2) as satisfaction_rate,
    fa.total_docs_processed,
    round(fa.active_users::float / nullif(f.firm_size, 0)::float * 100, 2) as engagement_rate,
    round(fa.total_queries::float / nullif(fa.active_users, 0)::float, 2) as avg_queries_per_active_user
from {{ ref('base_firms') }} f
left join firm_users fu on f.firm_id = fu.firm_id
left join firm_activity fa on f.firm_id = fa.firm_id 
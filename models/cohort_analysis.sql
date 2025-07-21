{{
    config(
        materialized='table'
    )
}}

with user_cohorts as (
    select
        user_id,
        event_type,
        date_trunc('month', min(event_timestamp)) as cohort_month,
        date_trunc('month', event_timestamp) as activity_month,
        count(*) as monthly_queries,
        avg(feedback_score) as monthly_feedback,
        sum(num_docs) as monthly_docs
    from {{ ref('base_events') }}
    group by user_id, event_type, date_trunc('month', event_timestamp)
),
first_month_users as (
    select 
        event_type,
        cohort_month,
        count(distinct user_id) as cohort_size
    from user_cohorts uc1
    where activity_month = cohort_month
    group by event_type, cohort_month
),
cohort_retention as (
    select
        cohort_month,
        activity_month,
        event_type,
        date_diff('month', cohort_month, activity_month) as months_since_cohort,
        count(distinct user_id) as active_users,
        sum(monthly_queries) as total_queries,
        avg(monthly_feedback) as avg_feedback,
        sum(monthly_docs) as total_docs
    from user_cohorts
    group by cohort_month, activity_month, event_type
)
select
    cr.cohort_month,
    cr.activity_month,
    cr.event_type,
    cr.months_since_cohort,
    cr.active_users,
    fmu.cohort_size as original_cohort_size,
    cr.total_queries,
    round(cr.avg_feedback, 2) as avg_feedback,
    cr.total_docs,
    round(cr.active_users::float / nullif(fmu.cohort_size, 0)::float * 100, 2) as retention_rate
from cohort_retention cr
join first_month_users fmu 
    on cr.event_type = fmu.event_type 
    and cr.cohort_month = fmu.cohort_month
order by cr.cohort_month desc, cr.event_type, cr.months_since_cohort 
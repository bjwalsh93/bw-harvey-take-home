{{
    config(
        materialized='table'
    )
}}

select
    date(event_timestamp) as event_date,
    event_type,
    count(*) as event_count,
    count(distinct user_id) as unique_users,
    sum(num_docs) as docs_processed,
    round(avg(num_docs), 2) as avg_docs_per_event,
    round(avg(feedback_score), 2) as avg_feedback_score,
    count(case when feedback_score >= 4 then 1 end) as high_satisfaction_events
from {{ ref('base_events') }}
group by date(event_timestamp), event_type
order by event_date desc, event_type 
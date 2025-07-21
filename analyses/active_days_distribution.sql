-- Get active days distribution per user per month
with user_monthly_days as (
    select 
        user_id,
        date_trunc('month', event_timestamp) as month,
        count(distinct date_trunc('day', event_timestamp)) as active_days,
        count(*) as total_queries,
        sum(num_docs) as total_docs
    from base_events
    group by user_id, date_trunc('month', event_timestamp)
)
select 
    'Active Days' as metric,
    min(active_days) as min_val,
    percentile_cont(0.25) within group (order by active_days) as p25,
    percentile_cont(0.5) within group (order by active_days) as median,
    percentile_cont(0.75) within group (order by active_days) as p75,
    max(active_days) as max_val,
    round(avg(active_days), 2) as avg_val
from user_monthly_days

union all

-- Distribution of queries per active day
select 
    'Queries per Active Day' as metric,
    min(total_queries::float / active_days) as min_val,
    percentile_cont(0.25) within group (order by total_queries::float / active_days) as p25,
    percentile_cont(0.5) within group (order by total_queries::float / active_days) as median,
    percentile_cont(0.75) within group (order by total_queries::float / active_days) as p75,
    max(total_queries::float / active_days) as max_val,
    round(avg(total_queries::float / active_days), 2) as avg_val
from user_monthly_days; 
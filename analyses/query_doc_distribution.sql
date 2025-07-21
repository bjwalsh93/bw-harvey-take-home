-- Get monthly query and doc stats
with user_monthly_stats as (
    select 
        user_id,
        date_trunc('month', event_timestamp) as month,
        count(*) as total_queries,
        sum(num_docs) as total_docs,
        round(sum(num_docs)::float / count(*), 2) as docs_per_query
    from base_events
    group by user_id, date_trunc('month', event_timestamp)
)
select 
    'Queries' as metric,
    min(total_queries) as min_val,
    percentile_cont(0.25) within group (order by total_queries) as p25,
    percentile_cont(0.5) within group (order by total_queries) as median,
    percentile_cont(0.75) within group (order by total_queries) as p75,
    max(total_queries) as max_val,
    round(avg(total_queries), 2) as avg_val
from user_monthly_stats

union all

select 
    'Documents' as metric,
    min(total_docs) as min_val,
    percentile_cont(0.25) within group (order by total_docs) as p25,
    percentile_cont(0.5) within group (order by total_docs) as median,
    percentile_cont(0.75) within group (order by total_docs) as p75,
    max(total_docs) as max_val,
    round(avg(total_docs), 2) as avg_val
from user_monthly_stats; 
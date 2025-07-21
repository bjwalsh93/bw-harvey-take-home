-- Step 1: Analyze distribution of active days per month
with monthly_active_days as (
    select 
        user_id,
        date_trunc('month', event_timestamp) as month,
        count(distinct event_date) as days_active
    from base_events
    group by user_id, date_trunc('month', event_timestamp)
)
select 
    percentile_cont(0.25) within group (order by days_active) as p25_days,
    percentile_cont(0.50) within group (order by days_active) as median_days,
    percentile_cont(0.75) within group (order by days_active) as p75_days,
    percentile_cont(0.90) within group (order by days_active) as p90_days,
    round(avg(days_active), 1) as avg_days,
    max(days_active) as max_days
from monthly_active_days
where days_active > 0;  -- Excluding inactive months
-- This analysis helped us choose ≥4 active days as one power user criterion

-- Step 2: Analyze distribution of queries per month
with monthly_queries as (
    select 
        user_id,
        date_trunc('month', event_timestamp) as month,
        count(*) as query_count
    from base_events
    group by user_id, date_trunc('month', event_timestamp)
)
select 
    percentile_cont(0.25) within group (order by query_count) as p25_queries,
    percentile_cont(0.50) within group (order by query_count) as median_queries,
    percentile_cont(0.75) within group (order by query_count) as p75_queries,
    percentile_cont(0.90) within group (order by query_count) as p90_queries,
    round(avg(query_count), 1) as avg_queries,
    max(query_count) as max_queries
from monthly_queries
where query_count > 0;  -- Excluding inactive months
-- This analysis helped us choose ≥5 queries as one power user criterion

-- Step 3: Analyze distribution of documents processed per month
with monthly_docs as (
    select 
        user_id,
        date_trunc('month', event_timestamp) as month,
        sum(num_docs) as total_docs
    from base_events
    group by user_id, date_trunc('month', event_timestamp)
)
select 
    percentile_cont(0.25) within group (order by total_docs) as p25_docs,
    percentile_cont(0.50) within group (order by total_docs) as median_docs,
    percentile_cont(0.75) within group (order by total_docs) as p75_docs,
    percentile_cont(0.90) within group (order by total_docs) as p90_docs,
    round(avg(total_docs), 1) as avg_docs,
    max(total_docs) as max_docs
from monthly_docs
where total_docs > 0;  -- Excluding inactive months
-- This analysis helped us choose ≥45 documents as one power user criterion

-- Step 4: Final power user distribution based on all criteria
with user_monthly_metrics as (
    select 
        user_id,
        date_trunc('month', event_timestamp) as month,
        count(distinct event_date) as days_active,
        count(*) as query_count,
        sum(num_docs) as total_docs
    from base_events
    group by user_id, date_trunc('month', event_timestamp)
)
select 
    case
        when days_active >= 4 and query_count >= 5 and total_docs >= 45 then 'Power User'
        when query_count >= 5 and total_docs >= 45 then 'High Engagement'
        when query_count > 2 or total_docs > 12 then 'Medium Engagement'
        when days_active >= 1 and (query_count >= 1 or total_docs >= 1) then 'Low Engagement'
        else 'Inactive'
    end as engagement_level,
    count(*) as user_months,
    round(count(*)::float / sum(count(*)) over () * 100, 2) as percentage,
    round(avg(days_active), 1) as avg_days_active,
    round(avg(query_count), 1) as avg_queries,
    round(avg(total_docs), 1) as avg_docs
from user_monthly_metrics
group by case
    when days_active >= 4 and query_count >= 5 and total_docs >= 45 then 'Power User'
    when query_count >= 5 and total_docs >= 45 then 'High Engagement'
    when query_count > 2 or total_docs > 12 then 'Medium Engagement'
    when days_active >= 1 and (query_count >= 1 or total_docs >= 1) then 'Low Engagement'
    else 'Inactive'
end
order by case engagement_level
    when 'Power User' then 1
    when 'High Engagement' then 2
    when 'Medium Engagement' then 3
    when 'Low Engagement' then 4
    else 5
end; 
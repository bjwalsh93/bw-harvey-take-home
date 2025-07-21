-- Basic table statistics
select * 
from events
;

select * 
from firms
;

select * 
from users
;

select 
    'base_events' as table_name,
    count(*) as total_rows,
    count(distinct USER_ID) as unique_users,
    count(distinct FIRM_ID) as unique_firms,
    min(event_timestamp) as earliest_date,
    max(event_timestamp) as latest_date
from base_events
union all
select 
    'base_users' as table_name,
    count(*) as total_rows,
    count(distinct user_id) as unique_users,
    null as unique_firms,
    min(created_timestamp) as earliest_date,
    max(created_timestamp) as latest_date
from base_users
union all
select 
    'base_firms' as table_name,
    count(*) as total_rows,
    null as unique_users,
    count(distinct firm_id) as unique_firms,
    min(created_date) as earliest_date,
    max(created_date) as latest_date
from base_firms;

-- 2 firms with 0 activity

-- Event type distribution
select 
    event_type,
    count(*) as event_count,
    count(distinct user_id) as unique_users,
    count(distinct firm_id) as unique_firms,
    round(avg(num_docs), 2) as avg_docs,
    round(avg(feedback_score), 2) as avg_feedback
from base_events
group by event_type;

-- User role distribution
select 
    role,
    count(*) as user_count,
    min(created_timestamp) as earliest_created,
    max(created_timestamp) as latest_created
from base_users
group by role;

-- Firm size distribution
select 
    case 
        when firm_size <= 100 then 'Small (<100)'
        when firm_size <= 250 then 'Medium (100-250)'
        else 'Large (250+)'
    end as size_bucket,
    count(*) as firm_count,
    round(avg(arr), 2) as avg_arr,
    min(arr) as min_arr,
    max(arr) as max_arr
from base_firms
group by case 
    when firm_size <= 100 then 'Small (<100)'
    when firm_size <= 250 then 'Medium (100-250)'
    else 'Large (250+)'
end;

-- Data quality checks
select 
    'Null event_type' as issue,
    count(*) as count
from base_events 
where event_type is null
union all
select 
    'Future dates' as issue,
    count(*) as count
from base_events 
where event_timestamp > current_timestamp
union all
select 
    'Invalid feedback scores' as issue,
    count(*) as count
from base_events 
where feedback_score not between 1 and 5
union all
select 
    'Negative docs' as issue,
    count(*) as count
from base_events 
where num_docs < 0;

-- Multiple firms check
select 
    user_id,
    count(distinct firm_id) as num_firms,
    string_agg(distinct firm_id::text, ', ') as firms
from base_events
group by user_id
having count(distinct firm_id) > 1;

-- 1 user with multiple firms

-- User-firm relationship checks
select 
    'Users without any firm activity' as issue,
    count(distinct u.user_id) as count
from base_users u
left join base_events e on u.user_id = e.USER_ID
where e.FIRM_ID is null
union all
select 
    'Users with events in non-existent firms' as issue,
    count(distinct e.USER_ID) as count
from base_events e
left join base_firms f on e.FIRM_ID = f.firm_id
where f.firm_id is null;

-- Activity patterns with rolling averages
with daily_stats as (
    select 
        event_date,
        count(*) as event_count,
        count(distinct user_id) as unique_users
    from base_events
    group by event_date
),
rolling_stats as (
    select 
        *,
        avg(event_count) over (
            order by event_date
            rows between 6 preceding and current row
        ) as rolling_7day_avg,
        stddev(event_count) over (
            order by event_date
            rows between 6 preceding and current row
        ) as rolling_7day_stddev
    from daily_stats
)
select 
    event_date,
    event_count,
    unique_users,
    round(rolling_7day_avg, 2) as rolling_7day_avg,
    case 
        when event_count > (rolling_7day_avg + 2 * rolling_7day_stddev) then 'High Anomaly'
        when event_count < (rolling_7day_avg - 2 * rolling_7day_stddev) then 'Low Anomaly'
        else 'Normal'
    end as anomaly_flag
from rolling_stats
order by event_date;

-- User activity by role
select 
    u.role,
    count(distinct u.user_id) as total_users,
    count(distinct case when e.user_id is not null then u.user_id end) as active_users,
    round(avg(e.num_docs), 2) as avg_docs_per_event,
    round(avg(e.feedback_score), 2) as avg_feedback
from base_users u
left join base_events e on u.user_id = e.user_id
group by u.role;

-- Top firms by ARR and activity
select 
    f.firm_id,
    f.arr,
    count(distinct e.user_id) as active_users,
    count(*) as total_events,
    round(count(*)::float / nullif(count(distinct e.user_id), 0), 2) as events_per_user
from base_firms f
left join base_events e on f.firm_id = e.firm_id
group by f.firm_id, f.arr
order by f.arr desc
limit 10;

-- Monthly document volume distribution
with user_activity as (
    select 
        user_id,
        event_month,
        sum(num_docs) as total_docs
    from base_events
    group by all
)
select 
    case 
        when total_docs = 0 then '0 docs'
        when total_docs <= 10 then '1-10 docs'
        when total_docs <= 25 then '11-25 docs'
        when total_docs <= 50 then '26-50 docs'
        when total_docs <= 100 then '51-100 docs'
        else '100+ docs'
    end as activity_bucket,
    count(distinct user_id) as user_count,
    round((count(*)::float / sum(count(*)) over ()) * 100, 2) as percentage
from user_activity
group by activity_bucket
order by 
    case activity_bucket
        when '0 docs' then 1
        when '1-10 docs' then 2
        when '11-25 docs' then 3
        when '26-50 docs' then 4
        when '51-100 docs' then 5
        else 6
    end;

-- Engagement level analysis
with monthly_user_stats as (
    select 
        user_id,
        date_trunc('month', event_timestamp) as month,
        sum(num_docs) as total_docs,
        count(distinct event_type) as num_features_used,
        string_agg(distinct event_type, ', ' order by event_type) as features_used,
        case
            when count(distinct event_type) = 3 and sum(num_docs) >= 50 then 'High Engagement'
            when (count(distinct event_type) >= 2 and sum(num_docs) >= 25) or 
                 (count(distinct event_type) = 3 and sum(num_docs) >= 10) then 'Medium Engagement'
            else 'Low Engagement'
        end as engagement_level
    from base_events
    group by user_id, date_trunc('month', event_timestamp)
)
select 
    engagement_level,
    num_features_used,
    count(*) as user_months,
    round(count(*)::float / sum(count(*)) over () * 100, 2) as percentage,
    string_agg(distinct features_used, ' | ') as feature_combinations
from monthly_user_stats
group by engagement_level, num_features_used
order by 
    case engagement_level
        when 'High Engagement' then 1
        when 'Medium Engagement' then 2
        else 3
    end,
    num_features_used desc;

--13 % of users in high engagement bucket

-- Anomaly detection by event type
with daily_counts as (
    select 
        event_date,
        event_type,
        count(*) as event_count,
        count(distinct user_id) as unique_users,
        avg(num_docs) as avg_docs
    from base_events
    group by event_date, event_type
),
rolling_stats as (
    select 
        *,
        avg(event_count) over (
            partition by event_type
            order by event_date
            rows between 30 preceding and 1 preceding
        ) as prev_30day_avg,
        stddev(event_count) over (
            partition by event_type
            order by event_date
            rows between 30 preceding and 1 preceding
        ) as prev_30day_stddev
    from daily_counts
)
select 
    event_date,
    event_type,
    event_count,
    unique_users,
    round(avg_docs, 2) as avg_docs,
    round(prev_30day_avg, 2) as prev_30day_avg,
    case 
        when event_count > (prev_30day_avg + 2 * prev_30day_stddev) then 'ANOMALY: High Activity'
        when event_count < (prev_30day_avg - 2 * prev_30day_stddev) then 'ANOMALY: Low Activity'
        else 'Normal'
    end as anomaly_flag
from rolling_stats
where event_count > (prev_30day_avg + 2 * prev_30day_stddev)
   or event_count < (prev_30day_avg - 2 * prev_30day_stddev)
order by event_date desc; 
---5 results for anomalous activity




with raw_daily_stats as (
    select 
        date_trunc('day', event_timestamp) as day,
        avg(FEEDBACK_SCORE) as daily_avg_score,
        count(*) as daily_feedback_count
    from base_events
    where FEEDBACK_SCORE is not null
    group by date_trunc('day', event_timestamp)
),
daily_stats_with_rolling as (
    select 
        day,
        daily_avg_score,
        daily_feedback_count,
        avg(daily_avg_score) over (
            order by day
            rows between 6 preceding and current row
        ) as rolling_7day_avg,
        stddev(daily_avg_score) over (
            order by day
            rows between 6 preceding and current row
        ) as rolling_7day_stddev
    from raw_daily_stats
)
select
    day,
    daily_avg_score,
    daily_feedback_count,
    rolling_7day_avg,
    rolling_7day_stddev,
    case
        when rolling_7day_stddev = 0 then 'No Variation'
        when daily_avg_score < (rolling_7day_avg - 2 * rolling_7day_stddev) then 'Unusually Low'
        when daily_avg_score > (rolling_7day_avg + 2 * rolling_7day_stddev) then 'Unusually High'
        else 'Normal'
    end as anomaly_status
from daily_stats_with_rolling
where anomaly_status != 'Normal'
order by day 

-- 3 rows with anomalous feedback scores
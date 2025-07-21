-- Finding 1: Firms with zero activity
select f.firm_id, f.firm_name, f.created_date
from base_firms f
left join base_events e on f.firm_id = e.firm_id
group by f.firm_id, f.firm_name, f.created_date
having count(e.event_id) = 0;
-- Result: 2 firms with 0 activity

-- Finding 2: Users with multiple firms
select 
    user_id,
    count(distinct firm_id) as num_firms,
    string_agg(distinct firm_id::text, ', ') as firms
from base_events
group by user_id
having count(distinct firm_id) > 1;
-- Result: 1 user with multiple firms

-- Finding 3: Activity anomalies
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
-- Result: 5 days with anomalous activity

-- Finding 4: Feedback score anomalies
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
order by day;
-- Result: 3 instances of unusual feedback patterns

-- Finding 5: Power User Analysis
select 
    engagement_level,
    count(*) as user_count,
    round(count(*)::float / sum(count(*)) over () * 100, 2) as percentage
from user_engagement
where month = (select max(month) from user_engagement)
group by engagement_level
order by 
    case engagement_level
        when 'Power User' then 1
        when 'High Engagement' then 2
        when 'Medium Engagement' then 3
        when 'Low Engagement' then 4
        else 5
    end;
-- Result: Only 12.1% of Active users achieve Power User status (507 out of 4709) 
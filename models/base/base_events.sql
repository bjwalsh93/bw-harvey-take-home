select
    USER_ID,
    FIRM_ID,
    EVENT_TYPE,
    NUM_DOCS,
    FEEDBACK_SCORE,
    strptime(CREATED, '%m/%d/%Y') as event_timestamp,
    date(strptime(CREATED, '%m/%d/%Y')) as event_date,
    date_trunc('month', strptime(CREATED, '%m/%d/%Y')) as event_month
from {{ ref('events') }} 
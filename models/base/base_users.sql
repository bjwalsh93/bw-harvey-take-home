select
    ID as user_id,
    TITLE as role,
    strptime(CREATED, '%m/%d/%Y') as created_timestamp,
    date(strptime(CREATED, '%m/%d/%Y')) as created_date
from {{ ref('users') }} 
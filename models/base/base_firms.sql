select
    ID as firm_id,
    ID as firm_name,
    CAST(CREATED AS DATE) as created_date,
    FIRM_SIZE as firm_size,
    ARR_IN_THOUSANDS * 1000 as arr
from {{ ref('firms') }} 
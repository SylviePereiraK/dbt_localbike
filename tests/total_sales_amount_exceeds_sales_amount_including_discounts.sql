select 
    *
from 
    {{ref('mrt_localbike__max_amount_sales')}}
where 
    max_total_amount < max_discount_amount
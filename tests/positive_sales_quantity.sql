select 
    *
from 
    {{ref('mrt_localbike__max_amount_sales')}}
where 
    max_quantity_sales < 0
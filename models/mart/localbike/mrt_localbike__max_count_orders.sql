select 'store' as dimension, 
       s.store_id as id, 
       store_name as name,
       '' as last_name,
       count_orders_by_store as max_count
from {{ref('mrt_localbike__count_orders')}} co
left join {{ref('stg_localbike__stores')}} s
on co.store_id = s.store_id
qualify row_number() over (order by count_orders_by_store desc) = 1

union all

select 'staff' as dimension, 
       st.staff_id as id, 
       staff_first_name as name,
       staff_last_name as last_name,
       count_orders_by_staff as max_count
from {{ref('mrt_localbike__count_orders')}} co
left join {{ref('stg_localbike__staffs')}}st
on co.staff_id = st.staff_id
qualify row_number() over (order by count_orders_by_staff desc) = 1

union all

select 'customer' as dimension, 
       cu.customer_id as id, 
       customer_first_name as name,
       customer_last_name as last_name,
       count_orders_by_customer as max_count
from {{ref('mrt_localbike__count_orders')}} co
left join {{ref('stg_localbike__customers')}} cu
on co.customer_id = cu.customer_id
qualify row_number() over (order by count_orders_by_customer desc) = 1

union all

select 'status' as dimension, 
       status_id as id, 
       '' as name,
       '' as last_name,
       count_orders_by_status as max_count
from {{ref('mrt_localbike__count_orders')}} co
qualify row_number() over (order by count_orders_by_status desc) = 1
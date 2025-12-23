select 
  distinct store_id,sum(count_orders) over (partition by store_id) as count_orders_by_store,
  staff_id, sum(count_orders) over (partition by staff_id) as count_orders_by_staff,
  customer_id, sum(count_orders) over (partition by customer_id) as count_orders_by_customer,
  status_id, sum(count_orders) over (partition by status_id) as count_orders_by_status
from {{ref('int_localbike__orders')}}
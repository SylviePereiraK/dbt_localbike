select
    case when store_id is null then -1 else store_id end as store_id,
    case when staff_id is null then -1 else staff_id end as staff_id,
    case when customer_id is null then -1 else customer_id end as customer_id,
    case when order_status is null then -1 else order_status end as status_id,
    count(distinct order_id) as count_orders
from {{ ref("stg_localbike__orders") }} o
group by
    store_id,
    staff_id,
    customer_id,
    status_id
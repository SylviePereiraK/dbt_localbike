select
    case when p.product_id is null then -1 else p.product_id end as product_id,
    case when b.brand_id is null then -1 else b.brand_id end as brand_id,
    case when c.category_id is null then -1 else c.category_id end as category_id,
    case when store.store_id is null then -1 else store.store_id end as store_id,
    case when staff.staff_id is null then -1 else staff.staff_id end as staff_id,
    case when customer.customer_id is null then -1 else customer.customer_id end as customer_id,
    sum(item_quantity) as total_quantity,
    sum((item_quantity*oi.list_price)) as total_amount,
    sum((item_quantity*oi.list_price)*(discount)) as discount_amount,
from {{ ref("stg_localbike__orders") }} orders 
    left outer join {{ ref("stg_localbike__order_items") }} oi on orders.order_id = oi.order_id
    left outer join {{ ref("stg_localbike__products") }} p on oi.product_id = p.product_id
    left outer join {{ ref("stg_localbike__brands") }} b on p.brand_id = b.brand_id
    left outer join {{ ref("stg_localbike__categories") }} c on p.category_id = c.category_id
    left outer join {{ ref("stg_localbike__stores") }} store on orders.store_id = store.store_id
    left outer join {{ ref("stg_localbike__staffs") }} staff on orders.staff_id = staff.staff_id
    left outer join {{ ref("stg_localbike__customers") }} customer on orders.customer_id = customer.customer_id
group by
    p.product_id,
    b.brand_id,
    c.category_id,
    store.store_id,
    staff.staff_id,
    customer.customer_id
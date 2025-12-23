select 'store' as dimension, 
       s.store_id as id, 
       store_name as name,
       '' as last_name,
       total_quantity_by_store as max_quantity_sales,
       total_amount_by_store as max_total_amount,
       discount_amount_by_store as max_discount_amount,
from {{ref('mrt_localbike__amount_sales')}} ams
left join {{ref('stg_localbike__stores')}} s
on ams.store_id = s.store_id
qualify row_number() over (order by max_discount_amount desc) = 1

union all

select 'staff' as dimension, 
       st.staff_id as id, 
       staff_first_name as name,
       staff_last_name as last_name,
       total_quantity_by_staff as max_quantity_sales,
       total_amount_by_staff as max_total_amount,
       discount_amount_by_staff as max_discount_amount,
from {{ref('mrt_localbike__amount_sales')}} ams
left join {{ref('stg_localbike__staffs')}}st
on ams.staff_id = st.staff_id
qualify row_number() over (order by max_discount_amount desc) = 1

union all

select 'customer' as dimension, 
       cu.customer_id as id, 
       customer_first_name as name,
       customer_last_name as last_name,
       total_quantity_by_customer as max_quantity_sales,
       total_amount_by_customer as max_total_amount,
       discount_amount_by_customer as max_discount_amount,
from {{ref('mrt_localbike__amount_sales')}} ams
left join {{ref('stg_localbike__customers')}} cu
on ams.customer_id = cu.customer_id
qualify row_number() over (order by max_discount_amount desc) = 1

union all

select 'category' as dimension, 
       ca.category_id as id, 
       category_name as name,
       '' as last_name,
       total_quantity_by_category as max_quantity_sales,
       total_amount_by_category as max_total_amount,
       discount_amount_by_category as max_discount_amount,
from {{ref('mrt_localbike__amount_sales')}} ams
left join {{ref('stg_localbike__categories')}} ca
on ams.category_id = ca.category_id
qualify row_number() over (order by max_discount_amount desc) = 1

union all

select 'product' as dimension, 
       p.product_id as id, 
       product_name as name,
       '' as last_name,
       total_quantity_by_product as max_quantity_sales,
       total_amount_by_product as max_total_amount,
       discount_amount_by_product as max_discount_amount,
from {{ref('mrt_localbike__amount_sales')}} ams
left join {{ref('stg_localbike__products')}} p
on ams.product_id = p.product_id
qualify row_number() over (order by max_discount_amount desc) = 1

union all

select 'brand' as dimension, 
       b.brand_id as id, 
       brand_name as name,
       '' as last_name,
       total_quantity_by_brand as max_quantity_sales,
       total_amount_by_brand as max_total_amount,
       discount_amount_by_brand as max_discount_amount,
from {{ref('mrt_localbike__amount_sales')}} ams
left join {{ref('stg_localbike__brands')}} b
on ams.brand_id = b.brand_id
qualify row_number() over (order by max_discount_amount desc) = 1
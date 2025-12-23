select 
    distinct store_id,sum(total_quantity) over (partition by store_id) as total_quantity_by_store,
    sum(total_amount) over (partition by store_id) as total_amount_by_store,
    sum(discount_amount) over (partition by store_id) as discount_amount_by_store,

    staff_id, sum(total_quantity) over (partition by staff_id) as total_quantity_by_staff,
    sum(total_amount) over (partition by staff_id) as total_amount_by_staff,
    sum(discount_amount) over (partition by staff_id) as discount_amount_by_staff,

    customer_id, sum(total_quantity) over (partition by customer_id) as total_quantity_by_customer,
    sum(total_amount) over (partition by customer_id) as total_amount_by_customer,
    sum(discount_amount) over (partition by customer_id) as discount_amount_by_customer,

    category_id, sum(total_quantity) over (partition by category_id) as total_quantity_by_category,
    sum(total_amount) over (partition by category_id) as total_amount_by_category,
    sum(discount_amount) over (partition by category_id) as discount_amount_by_category,

  brand_id, sum(total_quantity) over (partition by category_id) as total_quantity_by_brand,
  sum(total_amount) over (partition by category_id) as total_amount_by_brand,
  sum(discount_amount) over (partition by category_id) as discount_amount_by_brand,

  product_id, sum(total_quantity) over (partition by category_id) as total_quantity_by_product,
  sum(total_amount) over (partition by category_id) as total_amount_by_product,
  sum(discount_amount) over (partition by category_id) as discount_amount_by_product,
from {{ref('int_localbike__sales')}}
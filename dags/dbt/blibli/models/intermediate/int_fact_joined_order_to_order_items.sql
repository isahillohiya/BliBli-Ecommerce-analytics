with joined_table as (
    select o.*,oi.order_item_id, oi.product_id,oi.seller_id, oi.pickup_limit_date, oi.price, oi.shipping_cost
    from {{ref("stg_orders")}} o
    left join 
    {{ref("stg_order_item")}}  oi
    on 
    o.order_id = oi.order_id
     
),
final as (
select * from joined_table
)


select * from final
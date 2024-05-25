{{ config(materialized='view') }}

with joined_table as (
    select *
        
    from {{ref('int_fact_joined_order_to_order_items')}} orders
        left join 
    {{ref("int_payment_groupby_order")}} pay
        on
    orders.order_id = pay.pay_order_id
     
),
 final as (

    select * from joined_table
 )

select * from final



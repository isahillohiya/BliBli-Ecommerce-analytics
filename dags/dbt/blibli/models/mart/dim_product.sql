with final as(
    select 
        ROW_NUMBER() OVER (ORDER BY _FIVETRAN_START) AS product_key,

        product_id,
        product_category ,      
        product_name_length,            
        product_description_length,      
        product_photos_qty,                       
        product_weight_g ,              
        product_length_cm ,              
        product_height_cm ,
        product_width_cm ,
         TO_DATE(_FIVETRAN_START::timestamp) as "start_date",
         TO_DATE(_FIVETRAN_END::timestamp)  as "end_date",
        _FIVETRAN_ACTIVE as "IS_CURRENT_VERSION"
    from {{ref("stg_product")}}
)

select * from final
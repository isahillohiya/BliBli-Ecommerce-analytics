with final as(
    select 
        ROW_NUMBER() OVER (ORDER BY _FIVETRAN_START) AS seller_key,

        seller_id,
        SELLER_ZIP_CODE,
        SELLER_CITY,
        SELLER_STATE,
         TO_DATE(_FIVETRAN_START::timestamp) as "start_date",
         TO_DATE(_FIVETRAN_END::timestamp)  as "end_date",
        _FIVETRAN_ACTIVE as "IS_CURRENT_VERSION"
    from {{ref("stg_seller")}}
)

select * from final
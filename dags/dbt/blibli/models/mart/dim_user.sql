with final as(
    select 
        ROW_NUMBER() OVER (ORDER BY _FIVETRAN_START) AS user_key,

        user_name,
        CUSTOMER_ZIP_CODE,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
         TO_DATE(_FIVETRAN_START::timestamp) as "start_date",
         TO_DATE(_FIVETRAN_END::timestamp)  as "end_date",
        IS_ACTIVE as "IS_CURRENT_VERSION"
    from {{ref("int_user_active_inactive_records")}}
)

select * from final
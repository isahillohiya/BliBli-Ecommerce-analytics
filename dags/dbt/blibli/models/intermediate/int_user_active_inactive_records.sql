with record_rank as (
    select *,
        row_number() over (partition by user_name order by _FIVETRAN_START desc)  as rnk 
    from  {{ref('stg_user')}} 
    where _FIVETRAN_ACTIVE = True
),

active_record as (
    select *, True as "IS_ACTIVE"
    from record_rank 
    where rnk = 1
) ,
inactive_record as(
    select * , False as "IS_ACTIVE"
    from record_rank 
    where rnk != 1
    union all
    select *,9999 as rnk,
    False as "IS_ACTIVE"  
    from  {{ref('stg_user')}} 
    where _FIVETRAN_ACTIVE = False
),


final as (
    select * from active_record
    union all
    select * from inactive_record
)

select * from final 
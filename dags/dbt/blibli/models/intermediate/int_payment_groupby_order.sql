with grouped_orders as (
select order_id as pay_order_id,

        count(PAYMENT_SEQUENTIAL) as num_payment , 
        sum(PAYMENT_VALUE) as total_payment_value , 
        sum(payment_installments) as total_payment_installments,
        
        sum(case when payment_type= 'credit_card' then 1 else 0 END) as num_credit_card ,
        sum(case when payment_type= 'credit_card' then payment_value else 0 END) as total_payment_credit_card , 
        
        sum(case when payment_type= 'blipay' then 1 else 0 END) as num_blipay ,
        sum(case when payment_type= 'blipay' then payment_value else 0 END) as total_payment_blipay ,
        
        sum(case when payment_type= 'voucher' then 1 else 0 END) as num_voucher ,
        sum(case when payment_type= 'voucher' then payment_value else 0 END) as total_payment_voucher ,
        
        sum(case when payment_type= 'debit_card' then 1 else 0 END) as num_debit_card ,
        sum(case when payment_type= 'debit_card' then payment_value else 0 END) as total_payment_debit_card ,

        sum(case when payment_type not in  ('credit_card','blipay','voucher','debit_card') then 1 else 0 END) as num_other_paymet_method ,
        sum(case when payment_type not in  ('credit_card','blipay','voucher','debit_card')  then payment_value else 0 END) as total_payment_other_payment_method 

        
from  {{ref("stg_payment")}}  
group by order_id 
),

final as (
    select * from grouped_orders
)

select * from final
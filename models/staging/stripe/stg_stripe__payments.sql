select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    -- Le montant est en centimes, on le convertit en dollars/euros
    amount / 100 as amount,
    created as created_at

from `dbt-tutorial.stripe.payment`
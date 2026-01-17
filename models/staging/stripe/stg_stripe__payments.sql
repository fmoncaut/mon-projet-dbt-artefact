with source as (

    select * from {{ source('stripe', 'payment') }}

),

rename as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        
        -- Conversion des cents en dollars
        amount / 100 as amount,
        
        -- ATTENTION : J'ai retiré la virgule ici car c'est la dernière colonne
        created as created_at
        
    from source

)

select * from rename
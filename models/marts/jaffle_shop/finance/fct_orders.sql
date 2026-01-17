with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }} 
    -- ⚠️ ATTENTION : Vérifiez le nom de ce fichier dans votre dossier models !
    -- Parfois c'est 'stg_payments' ou 'stg_jaffle_shop__payments' selon ce que vous avez créé.

),

order_payments as (

    -- ÉTAPE CLÉ : On calcule le total par commande
    select
        order_id,
        sum(amount) as total_amount

    from payments
    group by 1

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        -- AJOUTEZ CETTE LIGNE ICI :
        orders.order_date,
        -- On remplace les NULL par 0 pour les commandes gratuites
        coalesce(order_payments.total_amount, 0) as amount

    from orders
    -- On joint la liste des commandes avec le montant calculé
    left join order_payments using (order_id)

)

select * from final
with source as (

    -- On ne renomme pas ici, on prend tout brut
    select * from {{ source('jaffle_shop', 'customers') }}

),

renamed as (

    select
        -- 👇 On renomme UNIQUEMENT ici
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
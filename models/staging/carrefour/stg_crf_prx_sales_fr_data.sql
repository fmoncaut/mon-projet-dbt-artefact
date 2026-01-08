SELECT
    -- Ajoutez cette ligne :
    pdt_SUB_CATEGORY AS sub_category,

    -- 1. Barcode (via votre macro)
    {{ clean_barcode('barcode_ean13') }} AS barcode,
    
    -- 2. Magasin : site_key devient store_id
    CAST(site_key AS STRING) AS store_id,
    
    -- 3. Produit : Pas de product_id, on utilise le barcode comme ID
    {{ clean_barcode('barcode_ean13') }} AS product_id,
    
    -- 4. Quantité : N'existe pas, on met NULL (casté en entier)
    CAST(NULL AS INT64) AS sales_qty,
    
    -- 5. Revenu : CA devient sales_revenue
    CA AS sales_revenue,
    
    -- 6. Date
    DATE AS transaction_date,
    
    'PRX' AS store_type

FROM {{ source('dbt_intro', 'crf_prx_sales_fr_data') }}
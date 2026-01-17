SELECT
    -- Pas de barcode dans cette table
    CAST(NULL AS STRING) AS barcode,
    
    CAST(NULL AS STRING) AS store_id,
    
    -- On garde le nom du produit
    product_name,
    
    -- Ici on a bien les quantités (probablement en STRING vu le schéma précédent)
    SAFE_CAST(quantity AS INT64) AS sales_qty,
    
    -- Ici on a le turnover
    SAFE_CAST(turnover AS FLOAT64) AS sales_revenue,
    
    SAFE_CAST(week AS INT64) AS week_number,
    
    'FERRERO' AS store_type

FROM {{ source('dbt_intro', 'ferrero_sales_per_week_dirty') }}
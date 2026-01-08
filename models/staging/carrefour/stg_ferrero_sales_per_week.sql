SELECT
    -- Pas de code-barres dans la source, on met NULL pour respecter le format
    CAST(NULL AS STRING) AS barcode,
    
    -- On garde le nom du produit car c'est notre seule info produit
    product_name,
    
    -- Pas de magasin spécifié, on met NULL
    CAST(NULL AS STRING) AS store_id,
    
    -- Nettoyage : On convertit le texte en nombre (INTEGER)
    SAFE_CAST(quantity AS INT64) AS sales_qty,
    
    -- Nettoyage : On convertit le turnover (texte) en nombre (FLOAT)
    SAFE_CAST(turnover AS FLOAT64) AS sales_revenue,
    
    -- On garde le numéro de semaine
    week AS week_number,
    
    -- Source
    'FERRERO' AS store_type

FROM {{ source('dbt_intro', 'ferrero_sales_per_week_dirty') }}
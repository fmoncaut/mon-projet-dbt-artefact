WITH unioned_sales AS (
    -- 1. Magasins de Proximité
    SELECT * FROM {{ ref('stg_crf_prx_sales_fr_data') }}
    
    UNION ALL
    
    -- 2. Supermarchés
    SELECT * FROM {{ ref('stg_crf_sup_sales_fr_data') }}
    
    UNION ALL
    
    -- 3. Hypermarchés
    SELECT * FROM {{ ref('stg_crf_hyp_sales_fr_data') }}
)

SELECT 
    barcode,
    store_id,
    product_id,
    sales_qty,
    sales_revenue,
    transaction_date,
    store_type,
    sub_category,
FROM unioned_sales
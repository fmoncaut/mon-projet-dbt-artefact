SELECT
    {{ clean_barcode('barcode_ean13') }} AS barcode, -- Appel de la macro ici
    site_key,
    product_id,
    sales_qty,
    sales_revenue,
    transaction_date,
    'SUP' as store_type
FROM {{ source('dbt_intro', 'crf_sup_sales_fr_data') }}
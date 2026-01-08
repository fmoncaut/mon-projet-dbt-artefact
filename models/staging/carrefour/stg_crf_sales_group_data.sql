SELECT
    {{ clean_barcode('barcode_ean13') }} AS barcode, -- Appel de la macro ici    store_id,
    product_id,
    sales_qty,
    sales_revenue,
    transaction_date
FROM {{ source('dbt_intro', 'crf_sales_group_data') }}
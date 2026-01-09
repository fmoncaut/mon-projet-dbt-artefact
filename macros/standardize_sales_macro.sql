{% macro standardize_sales_macro(source_table_name, store_type_code) %}

SELECT
    -- On réutilise notre petite macro à l'intérieur de la grande !
    {{ clean_barcode('barcode_ean13') }} AS barcode,
    
    CAST(site_key AS STRING) AS store_id,
    
    {{ clean_barcode('barcode_ean13') }} AS product_id,
    
    -- On gère la colonne manquante directement ici
    CAST(NULL AS INT64) AS sales_qty,
    
    CA AS sales_revenue,
    
    DATE AS transaction_date,
    
    -- On injecte le code du magasin passé en paramètre
    '{{ store_type_code }}' AS store_type,
    
    pdt_SUB_CATEGORY AS sub_category

FROM {{ source('dbt_intro', source_table_name) }}

{% endmacro %}
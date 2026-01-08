SELECT
    {{ clean_barcode('barcode_ean13') }} AS barcode, -- Appel de la macro ici
    -- Sélectionnez les autres colonnes. 
    -- Si vous voulez tout garder tel quel, vous pouvez lister les colonnes explicitement (recommandé)
    -- ou utiliser * après avoir traité les cas particuliers, mais attention aux doublons de colonnes.
    site_key,
    product_id,
    sales_qty,
    sales_revenue,
    transaction_date,
    'PRX' as store_type -- Utile d'ajouter la source si on doit tout combiner plus tard
FROM {{ source('dbt_intro', 'crf_prx_sales_fr_data') }}
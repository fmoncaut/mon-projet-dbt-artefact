WITH fr_sales AS (
    -- On aggrège les ventes France par produit
    SELECT 
        barcode,
        SUM(sales_revenue) as fr_revenue,
        SUM(sales_qty) as fr_qty
    FROM {{ ref('int_sales_fr_consolidated') }}
    GROUP BY barcode
),

group_sales AS (
    -- On aggrège les ventes Groupe par produit
    SELECT 
        barcode,
        SUM(sales_revenue) as group_revenue,
        SUM(sales_qty) as group_qty
    FROM {{ ref('stg_crf_sales_group_data') }}
    GROUP BY barcode
)

SELECT
    COALESCE(f.barcode, g.barcode) as barcode,
    f.fr_revenue,
    g.group_revenue,
    (IFNULL(f.fr_revenue, 0) - IFNULL(g.group_revenue, 0)) as revenue_diff,
    CASE 
        WHEN ABS(IFNULL(f.fr_revenue, 0) - IFNULL(g.group_revenue, 0)) > 0.01 THEN 'ALERT'
        ELSE 'OK'
    END as status
FROM fr_sales f
FULL OUTER JOIN group_sales g ON f.barcode = g.barcode
ORDER BY revenue_diff DESC
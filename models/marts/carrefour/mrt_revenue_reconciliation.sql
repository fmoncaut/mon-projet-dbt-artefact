WITH fr_sales AS (
    SELECT 
        sub_category, -- On groupe par catégorie maintenant
        SUM(sales_revenue) as fr_revenue
    FROM {{ ref('int_sales_fr_consolidated') }}
    GROUP BY sub_category -- Changement ici
),

group_sales AS (
    SELECT 
        sub_category,
        SUM(sales_revenue) as group_revenue
    FROM {{ ref('stg_crf_sales_group_data') }}
    GROUP BY sub_category -- Changement ici
)

SELECT
    COALESCE(f.sub_category, g.sub_category) as sub_category,
    f.fr_revenue,
    g.group_revenue,
    (IFNULL(f.fr_revenue, 0) - IFNULL(g.group_revenue, 0)) as revenue_diff
FROM fr_sales f
FULL OUTER JOIN group_sales g ON f.sub_category = g.sub_category
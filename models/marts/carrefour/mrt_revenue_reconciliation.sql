WITH fr_sales AS (
    -- On aggrège les ventes France par sous-catégorie
    SELECT 
        sub_category,
        SUM(sales_revenue) as fr_revenue
    FROM {{ ref('int_sales_fr_consolidated') }}
    GROUP BY sub_category
),

group_sales AS (
    -- On aggrège les ventes Groupe par sous-catégorie
    SELECT 
        sub_category,
        SUM(sales_revenue) as group_revenue
    FROM {{ ref('stg_crf_sales_group_data') }}
    GROUP BY sub_category
)

SELECT
    -- On gère les catégories vides pour éviter l'erreur du test not_null
    COALESCE(COALESCE(f.sub_category, g.sub_category), 'INCONNU') as sub_category,
    
    f.fr_revenue,
    g.group_revenue,
    
    -- Calcul de l'écart
    (IFNULL(f.fr_revenue, 0) - IFNULL(g.group_revenue, 0)) as revenue_diff,
    
    -- Calcul du statut d'alerte
    CASE 
        WHEN ABS(IFNULL(f.fr_revenue, 0) - IFNULL(g.group_revenue, 0)) > 0.01 THEN 'ALERT'
        ELSE 'OK'
    END as status

FROM fr_sales f
FULL OUTER JOIN group_sales g ON f.sub_category = g.sub_category
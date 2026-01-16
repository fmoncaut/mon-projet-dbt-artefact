{{ config(materialized='table') }}

WITH stg_neighbourhoods AS (
    SELECT * FROM {{ ref('stg_neighbourhoods') }}
)

SELECT
    -- Nettoyage : On renomme la colonne technique en nom métier
    string_field_0 AS neighbourhood_group,
    
    neighbourhood

FROM stg_neighbourhoods
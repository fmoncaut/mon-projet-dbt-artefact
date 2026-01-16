{{ config(materialized='table') }}

WITH stg_listings AS (
    SELECT * FROM {{ ref('stg_listings') }}
)

SELECT
    listing_id,
    -- CORRECTION ICI : On prend 'name' (source) et on le renomme
    name AS listing_name,
    
    price,
    neighbourhood,
    room_type,
    accommodates,
    is_superhost,

    -- GÉO (Indispensable pour la carte)
    latitude,
    longitude

FROM stg_listings
WHERE listing_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY listing_id) = 1
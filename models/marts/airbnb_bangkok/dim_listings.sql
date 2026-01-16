{{ config(materialized='table') }}

WITH stg_listings AS (
    SELECT * FROM {{ ref('stg_listings') }}
)

SELECT
    listing_id,
    -- On garde le renommage existant
    name AS listing_name,
    
    price,
    neighbourhood,
    room_type,
    accommodates,
    is_superhost,
    
    -- --- NOUVEAUX CHAMPS AJOUTÉS ---
    
    -- Informations Hôte
    host_response_time,
    host_acceptance_rate,
    
    -- Equipements (Raw)
    amenities,
    
    -- Data Engineering : Extraction de flags pour Power BI
    -- (Adapte la syntaxe LIKE selon le format de tes données, ici format standard)
    CASE WHEN amenities LIKE '%Wifi%' THEN TRUE ELSE FALSE END AS has_wifi,
    CASE WHEN amenities LIKE '%Pool%' OR amenities LIKE '%Piscine%' THEN TRUE ELSE FALSE END AS has_pool,
    CASE WHEN amenities LIKE '%Kitchen%' OR amenities LIKE '%Cuisine%' THEN TRUE ELSE FALSE END AS has_kitchen,

    -- --- FIN NOUVEAUX CHAMPS ---

    -- GÉO
    latitude,
    longitude

FROM stg_listings
WHERE listing_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY listing_id) = 1
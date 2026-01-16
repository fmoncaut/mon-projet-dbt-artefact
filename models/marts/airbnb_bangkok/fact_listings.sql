{{ config(materialized='table') }}

WITH stg_listings AS (
    SELECT * FROM {{ ref('stg_listings') }}
)

SELECT
    listing_id,
    price,
    number_of_reviews,
    review_scores_rating,
    
    -- --- NOUVEAUX CHAMPS AJOUTÉS ---
    review_scores_cleanliness,
    review_scores_location,
    review_scores_value,
    -- -------------------------------

    reviews_per_month

FROM stg_listings
-- On garde le filtre de cohérence avec la dimension
WHERE listing_id IN (SELECT listing_id FROM {{ ref('dim_listings') }})
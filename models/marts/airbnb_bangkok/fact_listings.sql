{{ config(materialized='table') }}

WITH stg_listings AS (
    SELECT * FROM {{ ref('stg_listings') }}
)

SELECT
    listing_id,
    price,
    number_of_reviews,
    review_scores_rating,
    reviews_per_month

FROM stg_listings
-- On filtre pour s'assurer qu'on ne garde que ce qui est dans la dim (si jamais dim a des filtres supplémentaires)
WHERE listing_id IN (SELECT listing_id FROM {{ ref('dim_listings') }})
WITH raw_listings AS (
    SELECT * FROM {{ source('airbnb', 'raw_listings') }}
)

SELECT
    id AS listing_id,
    name,
    description,
    
    -- GÉO : On s'assure que ça reste du FLOAT (Nombre décimal)
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) AS longitude,

    -- PRIX : Puisqu'il est déjà en nombre dans le RAW, on le convertit juste en NUMERIC
    CAST(price AS NUMERIC) AS price,

    number_of_reviews,
    review_scores_rating,
    reviews_per_month,
    host_is_superhost AS is_superhost,
    neighbourhood_cleansed AS neighbourhood,
    room_type,
    accommodates

FROM raw_listings
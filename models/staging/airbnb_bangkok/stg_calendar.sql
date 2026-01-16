WITH raw_calendar AS (
    SELECT * FROM {{ source('airbnb', 'raw_calendar') }}
)

SELECT
    listing_id,
    
    -- Conversion DATE
    CAST(date AS DATE) AS date,

    -- Booléen disponibilité
    available AS is_available,

    -- Nettoyage des PRIX
    -- AVANT (Ce qui plante) :
    -- SAFE_CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS FLOAT64) AS price,

    -- APRÈS (La correction) :
    CAST(price AS NUMERIC) AS price,
    CAST(adjusted_price AS NUMERIC) AS adjusted_price,

    minimum_nights,
    maximum_nights

FROM raw_calendar
WHERE date IS NOT NULL
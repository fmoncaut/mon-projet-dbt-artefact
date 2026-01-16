{{ config(materialized='table') }}

WITH stg_calendar AS (
    SELECT * FROM {{ ref('stg_calendar') }}
),

stg_listings AS (
    SELECT listing_id FROM {{ ref('dim_listings') }}
)

SELECT
    c.listing_id,
    c.date,
    c.is_available,
    c.price,
    c.adjusted_price,
    CASE WHEN c.is_available = TRUE THEN 0 ELSE 1 END AS is_occupied

FROM stg_calendar c
-- C'est ici que la magie opère :
INNER JOIN stg_listings l ON c.listing_id = l.listing_id
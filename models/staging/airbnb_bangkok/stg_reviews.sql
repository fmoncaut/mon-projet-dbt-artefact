WITH raw_reviews AS (
    SELECT * FROM {{ source('airbnb', 'raw_reviews') }}
)

SELECT
    listing_id,
    id AS review_id,
    
    -- Conversion de la date (String -> Date)
    CAST(date AS DATE) AS review_date,
    
    reviewer_id,
    reviewer_name,
    comments

FROM raw_reviews
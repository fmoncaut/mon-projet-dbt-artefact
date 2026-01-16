{{ config(materialized='table') }}

WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),
stg_listings AS (
    SELECT listing_id FROM {{ ref('dim_listings') }}
)

SELECT
    r.review_id,
    r.listing_id,
    r.review_date,
    r.reviewer_name,
    r.comments,
    LENGTH(r.comments) as review_length

FROM stg_reviews r
INNER JOIN stg_listings l ON r.listing_id = l.listing_id
-- ON DÉDOUBLONNE SUR L'ID DU COMMENTAIRE :
QUALIFY ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_date DESC) = 1
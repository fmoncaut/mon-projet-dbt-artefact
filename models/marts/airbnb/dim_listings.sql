{{ config(materialized='table') }}

select
  listing_id,
  host_id,

  name,
  neighbourhood_cleansed as neighbourhood,

  room_type,
  property_type,

  accommodates,
  bedrooms,
  beds,

  price_clean as price,
  has_price,
  price_is_outlier,

  number_of_reviews,
  review_scores_rating,

  latitude,
  longitude

from {{ ref('stg_airbnb_listings') }}
where listing_id is not null

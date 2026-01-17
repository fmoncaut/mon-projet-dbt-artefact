{{ config(materialized='table') }}

select
  listing_id,
  host_id,
  name,
  neighbourhood,
  neighbourhood_cleansed,
  neighbourhood_group_cleansed,
  latitude,
  longitude,
  room_type,
  property_type,
  accommodates,
  bedrooms,
  beds,
  price,
  number_of_reviews,
  review_scores_rating
from {{ ref('stg_airbnb_listings') }}
where listing_id is not null

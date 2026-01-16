with raw as (
  select *
  from {{ source('airbnb', 'raw_listings') }}
)

select
  id as listing_id,
  name,
  listing_url,
  neighbourhood,
  room_type,
  accommodates,
  host_is_superhost as is_superhost,
  minimum_nights,
  host_id,
  price,

  -- Champs souvent absents dans les datasets Airbnb -> on neutralise
  cast(NULL as timestamp) as created_at,
  cast(NULL as timestamp) as updated_at,

  host_response_time,
  host_acceptance_rate,
  amenities,

  review_scores_rating,
  review_scores_accuracy,
  review_scores_cleanliness,
  review_scores_checkin,
  review_scores_communication,
  review_scores_location,
  review_scores_value,

  number_of_reviews,
  reviews_per_month,
  availability_365,

  latitude,
  longitude

from raw
where id is not null

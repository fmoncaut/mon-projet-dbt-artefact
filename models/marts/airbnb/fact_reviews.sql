{{ config(materialized='table') }}

select
  r.review_id,
  r.listing_id,
  r.review_date,

  r.reviewer_name,
  r.comments,

  l.review_scores_rating

from {{ ref('stg_airbnb_reviews') }} r
join {{ ref('dim_listings') }} l
  on r.listing_id = l.listing_id

where
  r.review_id is not null
  and r.review_date is not null

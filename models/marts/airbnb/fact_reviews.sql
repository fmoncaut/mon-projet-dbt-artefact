{{ config(materialized='table') }}

select
  review_id,
  listing_id,
  review_date,
  reviewer_name,
  comments
from {{ ref('stg_airbnb_reviews') }}
where review_id is not null
  and listing_id is not null

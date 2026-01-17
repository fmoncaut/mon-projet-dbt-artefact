{{ config(materialized='table') }}

select
  neighbourhood_cleansed as neighbourhood,
  count(distinct listing_id) as listings_count
from {{ ref('stg_airbnb_listings') }}
where neighbourhood_cleansed is not null
group by neighbourhood_cleansed

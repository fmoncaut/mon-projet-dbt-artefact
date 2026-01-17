{{ config(materialized='table') }}

select
  listing_id,
  calendar_date,
  is_available,
  price
from {{ ref('stg_airbnb_calendar') }}
where listing_id is not null
  and calendar_date is not null

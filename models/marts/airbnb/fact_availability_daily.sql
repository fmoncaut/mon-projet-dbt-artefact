{{ config(
    materialized='table',
    partition_by={
      "field": "calendar_date",
      "data_type": "date"
    },
    cluster_by=["listing_id"]
) }}

select
  c.listing_id,
  c.calendar_date,

  c.is_available,

  l.price as price,

  -- KPI de base
  case when c.is_available = false then 1 else 0 end as is_booked

from {{ ref('stg_airbnb_calendar') }} c
join {{ ref('dim_listings') }} l
  on c.listing_id = l.listing_id

where
  l.has_price = true
  and l.price_is_outlier = false
  and c.calendar_date is not null

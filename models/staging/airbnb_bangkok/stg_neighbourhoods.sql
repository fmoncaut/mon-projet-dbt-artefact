-- models/staging/airbnb_bangkok/stg_neighbourhoods.sql

with raw as (
  select *
  from {{ source('airbnb', 'raw_neighbourhoods') }}
)

select
  neighbourhood_group,
  neighbourhood
from raw

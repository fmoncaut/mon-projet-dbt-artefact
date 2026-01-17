{{ config(materialized='table') }}

select distinct
  neighbourhood
from {{ ref('stg_airbnb_neighbourhoods') }}
where neighbourhood is not null

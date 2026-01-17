with src as (

  select *
  from {{ source('raw_airbnb', 'neighbourhoods') }}

)

select
  cast(string_field_1 as string) as neighbourhood
from src
where string_field_1 is not null

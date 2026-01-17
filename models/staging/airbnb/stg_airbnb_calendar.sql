with src as (

  select *
  from {{ source('raw_airbnb', 'calendar') }}

),

clean as (

  select
    cast(listing_id as string) as listing_id,
    safe_cast(date as date) as calendar_date,

    -- available est parfois 't'/'f' ou 'TRUE'/'FALSE'
    case
      when lower(cast(available as string)) in ('t','true','1') then true
      when lower(cast(available as string)) in ('f','false','0') then false
      else null
    end as is_available,

    safe_cast(regexp_replace(price, r'[^0-9.]', '') as float64) as price

  from src

)

select * from clean

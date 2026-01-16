{{ config(materialized='table') }}

with stg_listings as (
    select * from {{ ref('stg_listings') }}
)

select
    listing_id,
    name as listing_name,
    
    price,
    neighbourhood,
    room_type,
    accommodates,
    is_superhost,

    host_response_time,
    host_acceptance_rate,

    amenities,

    case when amenities like '%Wifi%' then true else false end as has_wifi,
    case when amenities like '%Pool%' or amenities like '%Piscine%' then true else false end as has_pool,
    case when amenities like '%Kitchen%' or amenities like '%Cuisine%' then true else false end as has_kitchen,

    latitude,
    longitude

from stg_listings
qualify row_number() over (partition by listing_id order by listing_id) = 1

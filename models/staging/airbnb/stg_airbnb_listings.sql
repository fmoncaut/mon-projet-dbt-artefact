with src as (

  select *
  from {{ source('raw_airbnb', 'listings') }}

),

clean as (

  select
    cast(id as string) as listing_id,
    cast(host_id as string) as host_id,

    name,
    neighbourhood,
    neighbourhood_cleansed,
    neighbourhood_group_cleansed,

    safe_cast(latitude as float64) as latitude,
    safe_cast(longitude as float64) as longitude,

    room_type,
    property_type,

    safe_cast(accommodates as int64) as accommodates,
    safe_cast(bedrooms as int64) as bedrooms,
    safe_cast(beds as int64) as beds,

    -- price est souvent une string avec $ et virgules
    safe_cast(regexp_replace(cast(price as string), r'[^0-9.]', '') as float64) as price,

    safe_cast(number_of_reviews as int64) as number_of_reviews,
    safe_cast(review_scores_rating as float64) as review_scores_rating

  from src

)

select * from clean
